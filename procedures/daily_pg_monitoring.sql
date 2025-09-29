CREATE OR REPLACE PROCEDURE gate.daily_monitoring()
 LANGUAGE plpgsql
AS $procedure$
	begin

insert into gate.process_log
(id,
name_pr,
status,
created_at)
((select nextval('gate."id_process"'),
'delta_start',
'done',
(select now())))
;commit;

/*get_rows_day_delta*/
truncate gate.mon_num_rows_second_day ;

insert into gate.mon_num_rows_second_day (name_of_schema,name_of_table, num_rows_yesturday, num_rows_today)
	select  fd.name_of_schema, fd.name_of_table, fd.n_live_strings as num_rows_yesturday, qq.live_strings as num_rows_today
	from gate.mon_num_rows_first_day fd
	full outer join (
		select distinct a.schemaname,
		c.relname,
		n_live_tup as live_strings
		from pg_catalog.pg_class c
		left join pg_catalog.pg_namespace  n on (n.oid = c.relnamespace)
		join pg_catalog.pg_stat_user_tables a on c.relname = a.relname
		where n.nspname not in ('information_schema', 'pg_catalog')
		and c.relkind <> 'i'
		and a.schemaname not in ('dataset', 'public', 'stage', 'store', 'work')
		and n.nspname !~ '^pg_toast'
		and c.relname not like 'mon%'
		order by schemaname
	)qq
	on fd.name_of_table = qq.relname and fd.name_of_schema = qq.schemaname;

update gate.mon_num_rows_second_day set delta = num_rows_today - num_rows_yesturday;

truncate gate.mon_num_rows_first_day ;

insert into gate.mon_num_rows_first_day (name_of_schema,name_of_table, n_live_strings)
	select qq.schemaname, qq."relation", qq.live_strings
	from
		(select distinct a.schemaname,
		c.relname as "relation",
		n_live_tup as live_strings
		from pg_catalog.pg_class c
		left join pg_catalog.pg_namespace  n on (n.oid = c.relnamespace)
		join pg_catalog.pg_stat_user_tables a on c.relname = a.relname
		where n.nspname not in ('information_schema', 'pg_catalog')
		and c.relkind <> 'i'
		and a.schemaname not in ('dataset', 'public', 'stage', 'store', 'work')
		and n.nspname !~ '^pg_toast'
		and c.relname not like 'mon%'
		order by schemaname) qq;

insert into gate.process_log
(id,
name_pr,
status,
created_at)
((select nextval('gate."id_process"'),
'delta_end',
'done',
(select now())))
;commit;

insert into gate.process_log
(id,
name_pr,
status,
created_at)
((select nextval('gate."id_process"'),
'check_vrb_start',
'done',
(select now())))
;commit;

/*heck_vrb_tables Проверка изменений в избранных таблицах*/
truncate gate.mon_check_vrb_tables;

insert into gate.mon_check_vrb_tables (name_of_table, updated_on_khd, created_on_khd, max_business_date, "delta at 7:05 am")
 select distinct 'det_application', max(_updated_dttm),
 max(_created_dttm), max(dt_application_status), 't-' || extract(day from now() -max(dt_application_status))
 from cmdm.det_application;

insert into gate.mon_check_vrb_tables (name_of_table, updated_on_khd, created_on_khd)
 select distinct  'det_customer', max(_updated_dttm),
 max(_created_dttm)
 from cmdm.det_customer;
/*....*/


insert into gate.process_log
(id,
name_pr,
status,
created_at)
((select nextval('gate."id_process"'),
'check_vrb_end',
'done',
(select now())))
;commit;


/*set_process_log_errors*/
truncate gate.mon_proc_log_errors ;

insert into gate.mon_proc_log_errors (id, name_pr, status, created_at, errors)
	select  *
	from (
		select id,
		name_pr,
		status,
		created_at,
		case when last_value(name_pr) over w like '%start' and first_value(name_pr) over w like '%start'
		then 'процесс не завершился'
		when last_value(name_pr) over w like '%end' and first_value(name_pr) over w like '%start'
		and extract(epoch from (last_value(created_at) over w - first_value(created_at) over w))/ 3600 > 1
		then 'процесс длился больше часа'
		when  created_at = max(created_at) over () and name_pr like '%start'
		and extract(epoch from (now() - created_at))/ 3600 > 1
		then 'процесс длится больше часа'
		end as errors
		from gate.process_log
		window w as (
		partition by regexp_replace(name_pr, '(_start|_end)$', '')
		order by id
		rows between current row and 1 following
		)) t
	where errors is not null and  date_trunc('day',t.created_at ) > now() - interval '30d'
	order by created_at desc ;


insert into gate.process_log
(id,
name_pr,
status,
created_at)
((select nextval('gate."id_process"'),
'mon_top_biggest_tab_start',
'done',
(select now())))
;commit;

/*get_top_biggest_tab*/
truncate gate.mon_top_biggest_tab;
insert into gate.mon_top_biggest_tab (name_of_schema, name_of_table, table_size, index_size, total_size, live_strings)
	select a.schemaname,
	c.relname,
	pg_size_pretty(pg_relation_size(c.oid)) as table_size,
	pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid)) as index_size,
	pg_size_pretty(pg_total_relation_size(c.oid)) as total_size,
	n_live_tup as live_strings
	from pg_catalog.pg_class c
	left join pg_catalog.pg_namespace  n on (n.oid = c.relnamespace)
	join pg_catalog.pg_stat_user_tables a on c.relname = a.relname
	where n.nspname not in ('information_schema', 'pg_catalog')
	and c.relkind <> 'i'
	and n.nspname !~ '^pg_toast'
	and a.schemaname not in ('our', 'chosen', 'schemas')
	order by pg_total_relation_size(c.oid) desc;

insert into gate.process_log
(id,
name_pr,
status,
created_at)
((select nextval('gate."id_process"'),
'mon_top_biggest_tab_end',
'done',
(select now())))
;commit;

/*responses*/
truncate cmdm_temp.mon_responses;
insert into cmdm_temp.mon_responses
select distinct campaign_cd, 
date_trunc('day', ch.create_dttm)::date as created_dttm,
ch.channel,
sum(case when cr.task_id is null then 0 else 1 end) over (partition by campaign_cd, date_trunc('day', cr.create_dttm), cr.channel) as response_amount,
sum(case when ch.task_id is null then 0 else 1 end) over (partition by campaign_cd, date_trunc('day', ch.create_dttm), ch.channel) as communication_amount,
from cdm.communication_history ch
left join  cdm.communication_response cr
on cr.task_id = ch.task_id
where ch.create_dttm > now() - interval '7d'
order by campaign_cd, created_dttm;
commit;

/*убивает старые сессии с дбивера которые без движения 4 дня*/
create table gate.mon_terminate_temp as
	SELECT pg_terminate_backend(pid)
	FROM pg_catalog.pg_stat_activity sa
	where usename in ('acrm_s', 'acrm')
	and date_trunc('day',query_start) <= now() -interval '2Day'
	and lower(application_name) like 'dbeaver%'
	and  datname = 'acrm'
	and state = 'idle'
union all
 --долгие сессии
 select  pg_terminate_backend(pid)
from pg_catalog.pg_stat_activity
where upper(query) like 'INSERT INTO DATASET%'
 and (extract (hour from (now() - query_start)) >= 3
 or  (extract   (hour from query_start) between 0 and 5));

drop table gate.mon_terminate_temp;

	END;
$procedure$
;
