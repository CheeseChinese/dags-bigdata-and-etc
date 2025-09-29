with step1 as (
	select split_part(c.oid::regclass::text, '.', 1)  as schemaname,
	c.relname,
	pg_size_pretty(pg_relation_size(c.oid)) as table_size,
	pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid)) as index_size,
	pg_total_relation_size(c.oid) as total_size,
	c.reltuples as live_strings,
	a.n_tup_ins + a.n_tup_upd + a.n_tup_del as operations,
	greatest (a.last_analyze, a.last_autoanalyze) last_stat_upd,
	row_number () over(partition by split_part(c.oid::regclass::text, '.', 1) , c.relname order by a.n_tup_ins + a.n_tup_upd + a.n_tup_del desc ) as rn
	from pg_catalog.pg_class c
	left join pg_catalog.pg_namespace  n on (n.oid = c.relnamespace)
	left join pg_catalog.pg_stat_all_tables a on c.relname = a.relname
	where n.nspname not in ('information_schema', 'pg_catalog')
	and c.relkind  in ( 'm','r')
	and n.nspname !~ '^pg_toast' and n.nspname !~ '^pg_'
	and split_part(c.oid::regclass::text, '.', 1)  not in ('our', 'chosen', 'schemas')
	order by pg_total_relation_size(c.oid) desc, last_stat_upd
	),
filterr as (
select schemaname, relname, table_size, index_size, total_size, live_strings, operations, last_stat_upd,
case when operations = 0 then 0 else operations/live_strings * 100 end as use_coef
from step1
where rn =1 and (date_trunc('day', last_stat_upd ) <= to_date('01-09-2025', 'DD-MM-YYYY') or  last_stat_upd is null) --дата обновления статистики
)
select schemaname, relname, table_size, index_size,  pg_size_pretty(total_size) as total_size, live_strings, operations, last_stat_upd,use_coef
from filterr
where use_coef < 1 and total_size/(1024 * 1024 * 1024) >12; --смотрим, где мало операций, но таблица большая. Размер условный.
