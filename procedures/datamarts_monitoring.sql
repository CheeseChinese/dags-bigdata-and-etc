 with cte as (
 select * from (
			select
			name_table,
			type_engine,
			type_table,
			flow_run_id,
			id,
			start_dttm,
			end_dttm,
			status,
			error_desc,
			more_info,
			ROW_NUMBER() OVER (PARTITION BY name_table ORDER by START_DTTM desc nulls last) AS rn
			from gate.loading_tables_h lth
			where start_dttm >= (CURRENT_DATE - 1)
			and (name_table in (select  source_table from cmdm_temp.datamarts_structure ds) or name_table in (select datamart_name from cmdm_temp.datamarts_structure ds))
			) gg
where rn=1
)
select * from (
	SELECT
	      distinct
	      'DET_CUSTOMER' main_tab
		, t.name_table as det_tab , start_dttm, end_dttm
		, CASE WHEN (t.status IS NULL ) THEN 'To Day Not Queued'
			    WHEN t.status = 'RUNNING' THEN t.status
			    WHEN t.status = 'SUCCESSFUL'  THEN t.status ELSE CONCAT(t.status, ' TO DAY') END  status
		,t.type_engine,
		t.type_table,
		t.flow_run_id,
		t.id
		, t.error_desc
		, t.more_info
	from cte as t
	WHERE t.name_table in (...)
union
	SELECT
	      distinct
	      'DET_CUSTOMER_CONSENT' main_tab
		, t.name_table as det_tab , start_dttm, end_dttm
		, CASE WHEN (t.status IS NULL ) THEN 'To Day Not Queued'
			    WHEN t.status = 'RUNNING' THEN t.status
			    WHEN t.status = 'SUCCESSFUL'  THEN t.status ELSE CONCAT(t.status, ' TO DAY') END  status
		,t.type_engine,
		t.type_table,
		t.flow_run_id,
		t.id
		, t.error_desc
		, t.more_info
	from cte as t
	WHERE t.name_table in (...)
union
	SELECT
	      distinct
	      'DET_CUSTOMER_CONTACT' main_tab
		, t.name_table as det_tab , start_dttm, end_dttm
		, CASE WHEN (t.status IS NULL ) THEN 'To Day Not Queued'
			    WHEN t.status = 'RUNNING' THEN t.status
			    WHEN t.status = 'SUCCESSFUL'  THEN t.status ELSE CONCAT(t.status, ' TO DAY') END  status
		,t.type_engine,
		t.type_table,
		t.flow_run_id,
		t.id
		, t.error_desc
		, t.more_info
	from cte as t
	WHERE t.name_table in (...)
union
	SELECT
	      distinct
	      'DET_CUSTOMER_WORK' main_tab
		, t.name_table as det_tab , start_dttm, end_dttm
		, CASE WHEN (t.status IS NULL ) THEN 'To Day Not Queued'
			    WHEN t.status = 'RUNNING' THEN t.status
			    WHEN t.status = 'SUCCESSFUL'  THEN t.status ELSE CONCAT(t.status, ' TO DAY') END  status
		,t.type_engine,
		t.type_table,
		t.flow_run_id,
		t.id
		, t.error_desc
		, t.more_info
	from cte as t
	WHERE t.name_table in (...)
union
	SELECT
	      distinct
	      'DET_DEPARTMENT' main_tab
		, t.name_table as det_tab , start_dttm, end_dttm
		, CASE WHEN (t.status IS NULL ) THEN 'To Day Not Queued'
			    WHEN t.status = 'RUNNING' THEN t.status
			    WHEN t.status = 'SUCCESSFUL'  THEN t.status ELSE CONCAT(t.status, ' TO DAY') END  status
		,t.type_engine,
		t.type_table,
		t.flow_run_id,
		t.id
		, t.error_desc
		, t.more_info
	from cte as t
	WHERE t.name_table in (...)
union
	SELECT
	      distinct
	      'DET_DEPOSITDEAL' main_tab
		, t.name_table as det_tab , start_dttm, end_dttm
		, CASE WHEN (t.status IS NULL ) THEN 'To Day Not Queued'
			    WHEN t.status = 'RUNNING' THEN t.status
			    WHEN t.status = 'SUCCESSFUL'  THEN t.status ELSE CONCAT(t.status, ' TO DAY') END  status
		,t.type_engine,
		t.type_table,
		t.flow_run_id,
		t.id
		, t.error_desc
		, t.more_info
	from cte as t
	WHERE t.name_table in ( ...)
union
	SELECT
	      distinct
	      'DET_PLASTIC' main_tab
		, t.name_table as det_tab , start_dttm, end_dttm
		, CASE WHEN (t.status IS NULL ) THEN 'To Day Not Queued'
			    WHEN t.status = 'RUNNING' THEN t.status
			    WHEN t.status = 'SUCCESSFUL'  THEN t.status ELSE CONCAT(t.status, ' TO DAY') END  status
		,t.type_engine,
		t.type_table,
		t.flow_run_id,
		t.id
		, t.error_desc
		, t.more_info
	from cte as t
	WHERE t.name_table in (...)
union
	SELECT
	      distinct
	      'DET_RKODEAL' main_tab
		, t.name_table as det_tab , start_dttm, end_dttm
		, CASE WHEN (t.status IS NULL ) THEN 'To Day Not Queued'
			    WHEN t.status = 'RUNNING' THEN t.status
			    WHEN t.status = 'SUCCESSFUL'  THEN t.status ELSE CONCAT(t.status, ' TO DAY') END  status
		,t.type_engine,
		t.type_table,
		t.flow_run_id,
		t.id
		, t.error_desc
		, t.more_info
	from cte as t
	WHERE t.name_table in (...)
union
	SELECT
	      distinct
	      'DET_SUBPRODUCT' main_tab
		, t.name_table as det_tab , start_dttm, end_dttm
		, CASE WHEN (t.status IS NULL ) THEN 'To Day Not Queued'
			    WHEN t.status = 'RUNNING' THEN t.status
			    WHEN t.status = 'SUCCESSFUL'  THEN t.status ELSE CONCAT(t.status, ' TO DAY') END  status
		,t.type_engine,
		t.type_table,
		t.flow_run_id,
		t.id
		, t.error_desc
		, t.more_info
	from cte as t
	WHERE t.name_table in (...)
union
	SELECT
	      distinct
	      'FCT_SALARY_PAYMENT' main_tab
		, t.name_table as det_tab , start_dttm, end_dttm
		, CASE WHEN (t.status IS NULL ) THEN 'To Day Not Queued'
			    WHEN t.status = 'RUNNING' THEN t.status
			    WHEN t.status = 'SUCCESSFUL'  THEN t.status ELSE CONCAT(t.status, ' TO DAY') END  status
		,t.type_engine,
		t.type_table,
		t.flow_run_id,
		t.id
		, t.error_desc
		, t.more_info
	from cte as t
	WHERE t.name_table in (...)
union
	SELECT
	      distinct
	      'BM_DET_DEAL' main_tab
		, t.name_table as det_tab , start_dttm, end_dttm
		, CASE WHEN (t.status IS NULL ) THEN 'To Day Not Queued'
			    WHEN t.status = 'RUNNING' THEN t.status
			    WHEN t.status = 'SUCCESSFUL'  THEN t.status ELSE CONCAT(t.status, ' TO DAY') END  status
		,t.type_engine,
		t.type_table,
		t.flow_run_id,
		t.id
		, t.error_desc
		, t.more_info
	from cte as t
	WHERE t.name_table in (...)
union
	SELECT
	      distinct
	      'DET_CREDITDEAL' main_tab
		, t.name_table as det_tab , start_dttm, end_dttm
		, CASE WHEN (t.status IS NULL ) THEN 'To Day Not Queued'
			    WHEN t.status = 'RUNNING' THEN t.status
			    WHEN t.status = 'SUCCESSFUL'  THEN t.status ELSE CONCAT(t.status, ' TO DAY') END  status
		,t.type_engine,
		t.type_table,
		t.flow_run_id,
		t.id
		, t.error_desc
		, t.more_info
	from cte as t
	WHERE t.name_table in (...)
) as a
order by a.main_tab, a.start_dttm;
