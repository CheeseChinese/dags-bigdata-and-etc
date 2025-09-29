CREATE OR REPLACE PROCEDURE gate.monthly_monitoring()
 LANGUAGE plpgsql
AS $procedure$
 begin
insert into gate.process_log
(id,
name_pr,
status,
created_at)
((select nextval('gate."id_process"'),
'mm_rare_used_indxs_start',
'done',
(select now())));

/*1 редко используемые индексы, которые не уникальные, не для внешних ключей,  менее 50 раз*/
truncate gate.mon_rare_used_indxs;

insert into gate.mon_rare_used_indxs (table_name, index_name, index_scans, index_size,table_size , operations, distinct_index_col, index_cols, deletable)
 with
    foreign_key_indexes as (
        select i.indexrelid
        from
            pg_catalog.pg_constraint c
            inner join
            lateral -- используется так как есть функция, возвращающая множество значений unnest()
            unnest(c.conkey) -- unnest разворачивает множество номеров столбцов с ограничение по одному номеру на строку
            with ordinality u(attnum, attposition)
            on true
            inner join pg_catalog.pg_index i on i.indrelid = c.conrelid -- conrelid - таблица в которой есть это ограниченик
            and (c.conkey::int[] <@ i.indkey::int[]) -- массив номеров столбцов с ограничениями и массив номеров столбцов, которые входят в индекс
            -- проверка: все ли столбцы с ограничениями входят в индекс
        where c.contype = 'f' /*
        c = check constraint,
        f = foreign key constraint,
        n = not-null constraint (domains only),
         p = primary key constraint, u = unique constraint,
         t = constraint trigger, x = exclusion constraint
        **/
    ),
tem as (
select
    psui.relid::regclass::text as table_name,
    psui.indexrelid::regclass::text as index_name,
    psui.idx_scan as index_scans,
    pg_relation_size(i.indexrelid) as index_size,
    pg_relation_size(psui.relid) as table_size,
    (sut.n_tup_ins + sut.n_tup_upd + sut.n_tup_del) as operations,
   substring(pg_get_indexdef(i.indexrelid), position('(' in pg_get_indexdef(i.indexrelid))+ 1,
   position(')' in pg_get_indexdef(i.indexrelid)) - position('(' in pg_get_indexdef(i.indexrelid)) -1 ) as index_cols
from
    pg_catalog.pg_stat_user_indexes psui
    join pg_catalog.pg_index i on i.indexrelid = psui.indexrelid
    left join pg_catalog.pg_stat_user_tables sut on sut.relid = psui.relid
where
    psui.schemaname in ('cdm', 'cmdm', 'gate')
    and (not i.indisunique or i.indisprimary)
   and i.indexrelid not in (select * from foreign_key_indexes)  /* retain indexes on foreign keys */
   and psui.idx_scan < 50::integer
order by table_name, pg_relation_size(i.indexrelid) desc
),
count_cols as (
select count(distinct index_cols) as distinct_index_col,
count(index_cols) as index_cols, table_name
from tem
group by table_name
)
select
tem.table_name,
tem.index_name,
tem.index_scans,
tem.index_size,
tem.table_size,
tem.operations,
cc.distinct_index_col,
cc.index_cols,
case when tem.table_size = 0 or tem.operations = 0 or  cc.distinct_index_col <> cc.index_cols
 then 1 else 0 end as deletable
from tem
join count_cols cc on cc.table_name = tem.table_name
order by deletable desc;

insert into gate.process_log
(id,
name_pr,
status,
created_at)
((select nextval('gate."id_process"'),
'mm_rare_used_indxs_end',
'done',
(select now())));

insert into gate.process_log
(id,
name_pr,
status,
created_at)
((select nextval('gate."id_process"'),
'mm_nulls_in_indxs_start',
'done',
(select now())));

/* 2 индесы, которые содержат null значения, рекомендуется исключить такие строки при создании индекса на это поле*/
truncate gate.mon_nulls_in_indxs;

insert into gate.mon_nulls_in_indxs(table_name, index_name, nullable_fields, index_size)
select
    x.indrelid::regclass::text as table_name,
    x.indexrelid::regclass::text as index_name,
    string_agg(a.attname, ', ') as nullable_fields, -- In fact, there will always be only one column.
    pg_relation_size(x.indexrelid) as index_size
from
    pg_catalog.pg_index x
    inner join pg_catalog.pg_stat_all_indexes psai on psai.indexrelid = x.indexrelid
    inner join pg_catalog.pg_attribute a on a.attrelid = x.indrelid and a.attnum = any(x.indkey)
where
    not x.indisunique and --ищем неуникальные
    not a.attnotnull  ----ищем которые м б нулл
    and psai.schemaname in ('our', 'chosen', 'schemas')
   and array_position(x.indkey, a.attnum) = 0  /* только первый */
   and x.indpred is null --Null if not a partial index
group by x.indrelid, x.indexrelid, x.indpred
order by index_size desc;

insert into gate.process_log
(id,
name_pr,
status,
created_at)
((select nextval('gate."id_process"'),
'mm_nulls_in_indxs_end',
'done',
(select now())));

insert into gate.process_log
(id,
name_pr,
status,
created_at)
((select nextval('gate."id_process"'),
'mm_intersected_indxs_start',
'done',
(select now())));

/*3 пересекающиеся индексы A <@ A+B*/
truncate gate.mon_intersected_indxs;

insert into gate.mon_intersected_indxs( table_name, intersected_cols, intersected_indexes)
with
    index_info as (
        select
            i.indrelid,
            i.indexrelid,
            array(select unnest(i.indkey)) as cols,
            substring(pg_get_indexdef(i.indexrelid), position('(' in pg_get_indexdef(i.indexrelid))+ 1,
   position(')' in pg_get_indexdef(i.indexrelid)) - position('(' in pg_get_indexdef(i.indexrelid)) -1 ) as index_cols,
            'idx=' || i.indexrelid::regclass || ' , size=' || pg_relation_size(i.indexrelid) as info
        from
            pg_catalog.pg_index i
            inner join pg_catalog.pg_stat_all_indexes psai on psai.indexrelid = i.indexrelid
       where psai.schemaname in ('our', 'chosen', 'schemas')
    )
select
    a.indrelid::regclass::text as table_name,
    a.index_cols as intersected_cols,
    a.info || '; ' || b.info as intersected_indexes
from
    index_info a
    inner join
        (select * from index_info) b on a.indrelid = b.indrelid and a.indexrelid < b.indexrelid and a.cols <@ b.cols
order by table_name, intersected_indexes;

insert into gate.process_log
(id,
name_pr,
status,
created_at)
((select nextval('gate."id_process"'),
'mm_intersected_indxs_end',
'done',
(select now())));

insert into gate.process_log
(id,
name_pr,
status,
created_at)
((select nextval('gate."id_process"'),
'mm_duplicated_indxs_start',
'done',
(select now())));

/*4 полностью идентичные индексы. по групповому ключу*/
truncate gate.mon_duplicated_indxs;

insert into gate.mon_duplicated_indxs(table_name, duplicated_indexes)
select
    table_name,
    string_agg('idx=' || idx::text || ', size=' || pg_relation_size(idx), '; ') as duplicated_indexes
from (
    select
        x.indexrelid::regclass as idx,
        x.indrelid::regclass::text as table_name, -- cast to text for sorting purposes
        (
            x.indrelid::text || ' ' ||
            x.indclass::text || ' ' || --oid класса операторов применимого к данному типу индексов
            x.indkey::text || ' ' || -- какие столбцы индексируются
            x.indcollation::text -- см pg_collation
        ) as grouping_key
    from
        pg_catalog.pg_index x
        inner join pg_catalog.pg_stat_all_indexes psai on psai.indexrelid = x.indexrelid
    where psai.schemaname in ('our', 'chosen', 'schemas')
) sub
group by table_name, grouping_key
having count(*) > 1
order by table_name, sum(pg_relation_size(idx)) desc;


insert into gate.process_log
(id,
name_pr,
status,
created_at)
((select nextval('gate."id_process"'),
'mm_duplicated_indxs_end',
'done',
(select now())));
 END;
$procedure$
;
