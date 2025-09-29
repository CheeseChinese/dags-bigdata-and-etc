
with cte as (
select * from (
			SELECT
			STATUS_CD ,
			DAG_RUN_ID ,
			t.TASK_ID ,
			TASK_RUN_ID ,
			START_DTTM ,
			END_DTTM ,
			QUEUED_DTTM ,
			task_name,
			CASE WHEN task_desc LIKE '%IPC%' THEN 'Загрука в систему' WHEN task_desc LIKE '%ETL%в%витрину%EDM%' THEN 'Расчет EDM витрины' ELSE task_desc END task_desc ,
			ERROR_DESC error,
			REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TASK_NAME , 'A',''),'B',''), 'C',''),'D',''),'E',''),'F','') name_tab,
			ROW_NUMBER() OVER (PARTITION BY task_name ORDER by  case when QUEUED_DTTM is not null then QUEUED_DTTM else START_DTTM end desc nulls last) AS rn
			from gate.cm_task t
			join gate.cm_log_task_run cltr
			on t.task_id=cltr.task_id
			 WHERE (t.task_name::text ~~ '%T_MART%VRB%ACRM%'::text
			 or t.task_name  ~~ '%T_MART_LOAD__BM%'
			 OR t.task_name::text ~~ '%VRB_FCT_LOYALTYPROGRAM%'::text
			 OR t.task_name::text ~~ '%VRB_DET_ACCOUNT%'::text OR t.task_name::text ~~ '%VRB_DET_CUSTOMER%'::text
			 OR t.task_name::text ~~ '%BM_FCT_CARRY%'::text )
			 AND t.task_name::text !~~ '%T_MART_IPC_VRB2ACRMZ%'::text
			 AND cltr.start_dttm >= (CURRENT_DATE - 1)
			 AND t.task_name::text <> 'CALL_INT_LOG_BATCH'::text
			) ff
where rn=1 and (ff.name_tab in (select  source_table from cmdm_temp.datamarts_structure ds)
or ff.name_tab in (select  datamart_name from cmdm_temp.datamarts_structure ds)  )
), ytd as (
select * from (
			SELECT
			STATUS_CD ,
			DAG_RUN_ID ,
			t.TASK_ID ,
			TASK_RUN_ID ,
			START_DTTM ,
			END_DTTM ,
			QUEUED_DTTM ,
			task_name,
			CASE WHEN task_desc LIKE '%IPC%' THEN 'Загрука в систему' WHEN task_desc LIKE '%ETL%в%витрину%EDM%' THEN 'Расчет EDM витрины' ELSE task_desc END task_desc ,
			ERROR_DESC error,
			REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TASK_NAME , 'A',''),'B',''), 'C',''),'D',''),'E',''),'F','') name_tab,
			ROW_NUMBER() OVER (PARTITION BY task_name ORDER by  case when QUEUED_DTTM is not null then QUEUED_DTTM else START_DTTM end desc nulls last) AS rn
			from gate.cm_task t
			join gate.cm_log_task_run cltr
			on t.task_id=cltr.task_id
			 WHERE (t.task_name::text ~~ '%dummy%text1'::text
			 or t.task_name  ~~ '%ummy%text2%'
			 OR t.task_name::text ~~ '%dummy%text1%'::text
			 OR t.task_name::text ~~ '%ummy%text1%'::text OR t.task_name::text ~~ '%ummy%text1%'::text
			 OR t.task_name::text ~~ '%ummy%text1%'::text )
			 AND t.task_name::text !~~ '%ummy%text1%'::text
			 AND cltr.start_dttm >= (CURRENT_DATE - 4)
			 AND t.task_name::text <> 'CALL_INT_LOG_BATCH'::text
			) ff
where rn=1 and (ff.name_tab in (select  source_table from cmdm_temp.datamarts_structure ds)
or ff.name_tab in (select  datamart_name from cmdm_temp.datamarts_structure ds)  )
), etl as (
		SELECT
			t.TASK_ID ,
			AVG(START_DTTM - QUEUED_DTTM) avg_hour_start,
			AVG(END_DTTM - START_DTTM) avg_hour_end
		FROM
			gate.CM_LOG_TASK_RUN cltr
		JOIN gate.CM_TASK t ON
			t.TASK_ID = CLTR.task_id
			WHERE (t.task_name::text ~~ '%ummy%text1%'::text
			 or t.task_name  ~~ '%ummy%text1%'
			 OR t.task_name::text ~~ '%ummy%text1%'::text
			 OR t.task_name::text ~~ '%ummy%text1T%'::text
			 OR t.task_name::text ~~ '%ummy%text1%'::text
			 OR t.task_name::text ~~ '%ummy%text1%'::text )
			 AND t.task_name::text !~~ '%ummy%text1%'::text
			AND cltr.start_dttm >= (CURRENT_DATE - 4)
			AND cltr.STATUS_CD = 'SUCCESSFUL'
			GROUP BY t.TASK_ID
)
select * from (
	SELECT
	      distinct
	      'DET_CUSTOMER' main_tab
		, t.name_tab as det_tab
		, t.task_name
		, t.task_id
		, t.task_run_id
		, CASE WHEN cte.error IS NULL THEN t.error ELSE cte.error END error
	 	, CASE WHEN (cte.STATUS_CD IS NULL AND cte.queued_dttm IS NULL) THEN 'To Day Not Queued'
			    WHEN cte.STATUS_CD = 'RUNNING' THEN cte.STATUS_CD
			    WHEN cte.STATUS_CD = 'SUCCESSFUL'  THEN cte.STATUS_CD ELSE CONCAT(cte.STATUS_CD, ' TO DAY') END  status
		, cte.queued_dttm + avg_hour_start AS predict_start_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.start_dttm ELSE cte.start_dttm END  start_dttm
		, cte.start_dttm + avg_hour_end AS predict_end_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.end_dttm ELSE cte.end_dttm END  end_dttm
		, CASE WHEN cte.queued_dttm IS NULL THEN t.queued_dttm ELSE cte.queued_dttm END  queued_dttm
		, CASE WHEN cte.task_desc IS NULL THEN  t.task_desc ELSE  cte.task_desc END task_desc
	FROM ytd as t
	LEFT JOIN cte ON cte.name_tab = t.name_tab AND cte.task_name = t.task_name left join etl on etl.task_id = t.task_id
	WHERE t.name_tab in (...)
union
	SELECT
	      distinct
	      'DET_CUSTOMER_CONSENT' main_tab
		, t.name_tab as det_tab
		, t.task_name
		, t.task_id
		, t.task_run_id
		, CASE WHEN cte.error IS NULL THEN t.error ELSE cte.error END error
	 	, CASE WHEN (cte.STATUS_CD IS NULL AND cte.queued_dttm IS NULL) THEN 'To Day Not Queued'
			    WHEN cte.STATUS_CD = 'RUNNING' THEN cte.STATUS_CD
			    WHEN cte.STATUS_CD = 'SUCCESSFUL'  THEN cte.STATUS_CD ELSE CONCAT(cte.STATUS_CD, ' TO DAY') END  status
		, cte.queued_dttm + avg_hour_start AS predict_start_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.start_dttm ELSE cte.start_dttm END  start_dttm
		, cte.start_dttm + avg_hour_end AS predict_end_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.end_dttm ELSE cte.end_dttm END  end_dttm
		, CASE WHEN cte.queued_dttm IS NULL THEN t.queued_dttm ELSE cte.queued_dttm END  queued_dttm
		, CASE WHEN cte.task_desc IS NULL THEN  t.task_desc ELSE  cte.task_desc END task_desc
	FROM ytd as t
	LEFT JOIN cte ON cte.name_tab = t.name_tab AND cte.task_name = t.task_name left join etl on etl.task_id = t.task_id
	WHERE t.name_tab in (...)
union
	SELECT
	      distinct
	      'DET_CUSTOMER_CONTACT' main_tab
		, t.name_tab as det_tab
		, t.task_name
		, t.task_id
		, t.task_run_id
		, CASE WHEN cte.error IS NULL THEN t.error ELSE cte.error END error
	 	, CASE WHEN (cte.STATUS_CD IS NULL AND cte.queued_dttm IS NULL) THEN 'To Day Not Queued'
			    WHEN cte.STATUS_CD = 'RUNNING' THEN cte.STATUS_CD
			    WHEN cte.STATUS_CD = 'SUCCESSFUL'  THEN cte.STATUS_CD ELSE CONCAT(cte.STATUS_CD, ' TO DAY') END  status
		, cte.queued_dttm + avg_hour_start AS predict_start_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.start_dttm ELSE cte.start_dttm END  start_dttm
		, cte.start_dttm + avg_hour_end AS predict_end_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.end_dttm ELSE cte.end_dttm END  end_dttm
		, CASE WHEN cte.queued_dttm IS NULL THEN t.queued_dttm ELSE cte.queued_dttm END  queued_dttm
		, CASE WHEN cte.task_desc IS NULL THEN  t.task_desc ELSE  cte.task_desc END task_desc
	FROM ytd as t
	LEFT JOIN cte ON cte.name_tab = t.name_tab AND cte.task_name = t.task_name left join etl on etl.task_id = t.task_id
	WHERE t.name_tab in (...)
union
	SELECT
	      distinct
	      'DET_CUSTOMER_WORK' main_tab
		, t.name_tab as det_tab
		, t.task_name
		, t.task_id
		, t.task_run_id
		, CASE WHEN cte.error IS NULL THEN t.error ELSE cte.error END error
	 	, CASE WHEN (cte.STATUS_CD IS NULL AND cte.queued_dttm IS NULL) THEN 'To Day Not Queued'
			    WHEN cte.STATUS_CD = 'RUNNING' THEN cte.STATUS_CD
			    WHEN cte.STATUS_CD = 'SUCCESSFUL'  THEN cte.STATUS_CD ELSE CONCAT(cte.STATUS_CD, ' TO DAY') END  status
		, cte.queued_dttm + avg_hour_start AS predict_start_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.start_dttm ELSE cte.start_dttm END  start_dttm
		, cte.start_dttm + avg_hour_end AS predict_end_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.end_dttm ELSE cte.end_dttm END  end_dttm
		, CASE WHEN cte.queued_dttm IS NULL THEN t.queued_dttm ELSE cte.queued_dttm END  queued_dttm
		, CASE WHEN cte.task_desc IS NULL THEN  t.task_desc ELSE  cte.task_desc END task_desc
	FROM ytd as t
	LEFT JOIN cte ON cte.name_tab = t.name_tab AND cte.task_name = t.task_name left join etl on etl.task_id = t.task_id
	WHERE t.name_tab in (...)
union
	SELECT
	      distinct
	      'DET_DEPARTMENT' main_tab
		, t.name_tab as det_tab
		, t.task_name
		, t.task_id
		, t.task_run_id
		, CASE WHEN cte.error IS NULL THEN t.error ELSE cte.error END error
	 	, CASE WHEN (cte.STATUS_CD IS NULL AND cte.queued_dttm IS NULL) THEN 'To Day Not Queued'
			    WHEN cte.STATUS_CD = 'RUNNING' THEN cte.STATUS_CD
			    WHEN cte.STATUS_CD = 'SUCCESSFUL'  THEN cte.STATUS_CD ELSE CONCAT(cte.STATUS_CD, ' TO DAY') END  status
		, cte.queued_dttm + avg_hour_start AS predict_start_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.start_dttm ELSE cte.start_dttm END  start_dttm
		, cte.start_dttm + avg_hour_end AS predict_end_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.end_dttm ELSE cte.end_dttm END  end_dttm
		, CASE WHEN cte.queued_dttm IS NULL THEN t.queued_dttm ELSE cte.queued_dttm END  queued_dttm
		, CASE WHEN cte.task_desc IS NULL THEN  t.task_desc ELSE  cte.task_desc END task_desc
	FROM ytd as t
	LEFT JOIN cte ON cte.name_tab = t.name_tab AND cte.task_name = t.task_name left join etl on etl.task_id = t.task_id
	WHERE t.name_tab in (...)
union
	SELECT
	      distinct
	      'DET_DEPOSITDEAL' main_tab
		, t.name_tab as det_tab
		, t.task_name
		, t.task_id
		, t.task_run_id
		, CASE WHEN cte.error IS NULL THEN t.error ELSE cte.error END error
	 	, CASE WHEN (cte.STATUS_CD IS NULL AND cte.queued_dttm IS NULL) THEN 'To Day Not Queued'
			    WHEN cte.STATUS_CD = 'RUNNING' THEN cte.STATUS_CD
			    WHEN cte.STATUS_CD = 'SUCCESSFUL'  THEN cte.STATUS_CD ELSE CONCAT(cte.STATUS_CD, ' TO DAY') END  status
		, cte.queued_dttm + avg_hour_start AS predict_start_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.start_dttm ELSE cte.start_dttm END  start_dttm
		, cte.start_dttm + avg_hour_end AS predict_end_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.end_dttm ELSE cte.end_dttm END  end_dttm
		, CASE WHEN cte.queued_dttm IS NULL THEN t.queued_dttm ELSE cte.queued_dttm END  queued_dttm
		, CASE WHEN cte.task_desc IS NULL THEN  t.task_desc ELSE  cte.task_desc END task_desc
	FROM ytd as t
	LEFT JOIN cte ON cte.name_tab = t.name_tab AND cte.task_name = t.task_name left join etl on etl.task_id = t.task_id
		WHERE t.name_tab in ( ...)
union
	SELECT
	      distinct
	      'DET_PLASTIC' main_tab
		, t.name_tab as det_tab
		, t.task_name
		, t.task_id
		, t.task_run_id
		, CASE WHEN cte.error IS NULL THEN t.error ELSE cte.error END error
	 	, CASE WHEN (cte.STATUS_CD IS NULL AND cte.queued_dttm IS NULL) THEN 'To Day Not Queued'
			    WHEN cte.STATUS_CD = 'RUNNING' THEN cte.STATUS_CD
			    WHEN cte.STATUS_CD = 'SUCCESSFUL'  THEN cte.STATUS_CD ELSE CONCAT(cte.STATUS_CD, ' TO DAY') END  status
		, cte.queued_dttm + avg_hour_start AS predict_start_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.start_dttm ELSE cte.start_dttm END  start_dttm
		, cte.start_dttm + avg_hour_end AS predict_end_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.end_dttm ELSE cte.end_dttm END  end_dttm
		, CASE WHEN cte.queued_dttm IS NULL THEN t.queued_dttm ELSE cte.queued_dttm END  queued_dttm
		, CASE WHEN cte.task_desc IS NULL THEN  t.task_desc ELSE  cte.task_desc END task_desc
	FROM ytd as t
	LEFT JOIN cte ON cte.name_tab = t.name_tab AND cte.task_name = t.task_name left join etl on etl.task_id = t.task_id
		WHERE t.name_tab in (...)
union
	SELECT
	      distinct
	      'DET_RKODEAL' main_tab
		, t.name_tab as det_tab
		, t.task_name
		, t.task_id
		, t.task_run_id
		, CASE WHEN cte.error IS NULL THEN t.error ELSE cte.error END error
	 	, CASE WHEN (cte.STATUS_CD IS NULL AND cte.queued_dttm IS NULL) THEN 'To Day Not Queued'
			    WHEN cte.STATUS_CD = 'RUNNING' THEN cte.STATUS_CD
			    WHEN cte.STATUS_CD = 'SUCCESSFUL'  THEN cte.STATUS_CD ELSE CONCAT(cte.STATUS_CD, ' TO DAY') END  status
		, cte.queued_dttm + avg_hour_start AS predict_start_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.start_dttm ELSE cte.start_dttm END  start_dttm
		, cte.start_dttm + avg_hour_end AS predict_end_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.end_dttm ELSE cte.end_dttm END  end_dttm
		, CASE WHEN cte.queued_dttm IS NULL THEN t.queued_dttm ELSE cte.queued_dttm END  queued_dttm
		, CASE WHEN cte.task_desc IS NULL THEN  t.task_desc ELSE  cte.task_desc END task_desc
	FROM ytd as t
	LEFT JOIN cte ON cte.name_tab = t.name_tab AND cte.task_name = t.task_name left join etl on etl.task_id = t.task_id
		WHERE t.name_tab in (...)
union
	SELECT
	      distinct
	      'DET_SUBPRODUCT' main_tab
		, t.name_tab as det_tab
		, t.task_name
		, t.task_id
		, t.task_run_id
		, CASE WHEN cte.error IS NULL THEN t.error ELSE cte.error END error
	 	, CASE WHEN (cte.STATUS_CD IS NULL AND cte.queued_dttm IS NULL) THEN 'To Day Not Queued'
			    WHEN cte.STATUS_CD = 'RUNNING' THEN cte.STATUS_CD
			    WHEN cte.STATUS_CD = 'SUCCESSFUL'  THEN cte.STATUS_CD ELSE CONCAT(cte.STATUS_CD, ' TO DAY') END  status
		, cte.queued_dttm + avg_hour_start AS predict_start_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.start_dttm ELSE cte.start_dttm END  start_dttm
		, cte.start_dttm + avg_hour_end AS predict_end_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.end_dttm ELSE cte.end_dttm END  end_dttm
		, CASE WHEN cte.queued_dttm IS NULL THEN t.queued_dttm ELSE cte.queued_dttm END  queued_dttm
		, CASE WHEN cte.task_desc IS NULL THEN  t.task_desc ELSE  cte.task_desc END task_desc
	FROM ytd as t
	LEFT JOIN cte ON cte.name_tab = t.name_tab AND cte.task_name = t.task_name left join etl on etl.task_id = t.task_id
		WHERE t.name_tab in (...')
union
	SELECT
	      distinct
	      'FCT_SALARY_PAYMENT' main_tab
		, t.name_tab as det_tab
		, t.task_name
		, t.task_id
		, t.task_run_id
		, CASE WHEN cte.error IS NULL THEN t.error ELSE cte.error END error
	 	, CASE WHEN (cte.STATUS_CD IS NULL AND cte.queued_dttm IS NULL) THEN 'To Day Not Queued'
			    WHEN cte.STATUS_CD = 'RUNNING' THEN cte.STATUS_CD
			    WHEN cte.STATUS_CD = 'SUCCESSFUL'  THEN cte.STATUS_CD ELSE CONCAT(cte.STATUS_CD, ' TO DAY') END  status
		, cte.queued_dttm + avg_hour_start AS predict_start_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.start_dttm ELSE cte.start_dttm END  start_dttm
		, cte.start_dttm + avg_hour_end AS predict_end_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.end_dttm ELSE cte.end_dttm END  end_dttm
		, CASE WHEN cte.queued_dttm IS NULL THEN t.queued_dttm ELSE cte.queued_dttm END  queued_dttm
		, CASE WHEN cte.task_desc IS NULL THEN  t.task_desc ELSE  cte.task_desc END task_desc
	FROM ytd as t
	LEFT JOIN cte ON cte.name_tab = t.name_tab AND cte.task_name = t.task_name left join etl on etl.task_id = t.task_id
		WHERE t.name_tab in (...)
union
	SELECT
	      distinct
	      'BM_DET_DEAL' main_tab
		, t.name_tab as det_tab
		, t.task_name
		, t.task_id
		, t.task_run_id
		, CASE WHEN cte.error IS NULL THEN t.error ELSE cte.error END error
	 	, CASE WHEN (cte.STATUS_CD IS NULL AND cte.queued_dttm IS NULL) THEN 'To Day Not Queued'
			    WHEN cte.STATUS_CD = 'RUNNING' THEN cte.STATUS_CD
			    WHEN cte.STATUS_CD = 'SUCCESSFUL'  THEN cte.STATUS_CD ELSE CONCAT(cte.STATUS_CD, ' TO DAY') END  status
		, cte.queued_dttm + avg_hour_start AS predict_start_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.start_dttm ELSE cte.start_dttm END  start_dttm
		, cte.start_dttm + avg_hour_end AS predict_end_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.end_dttm ELSE cte.end_dttm END  end_dttm
		, CASE WHEN cte.queued_dttm IS NULL THEN t.queued_dttm ELSE cte.queued_dttm END  queued_dttm
		, CASE WHEN cte.task_desc IS NULL THEN  t.task_desc ELSE  cte.task_desc END task_desc
	FROM ytd as t
	LEFT JOIN cte ON cte.name_tab = t.name_tab AND cte.task_name = t.task_name left join etl on etl.task_id = t.task_id
		WHERE t.name_tab in (...)
union
	SELECT
	      distinct
	      'DET_CREDITDEAL' main_tab
		, t.name_tab as det_tab
		, t.task_name
		, t.task_id
		, t.task_run_id
		, CASE WHEN cte.error IS NULL THEN t.error ELSE cte.error END error
	 	, CASE WHEN (cte.STATUS_CD IS NULL AND cte.queued_dttm IS NULL) THEN 'To Day Not Queued'
			    WHEN cte.STATUS_CD = 'RUNNING' THEN cte.STATUS_CD
			    WHEN cte.STATUS_CD = 'SUCCESSFUL'  THEN cte.STATUS_CD ELSE CONCAT(cte.STATUS_CD, ' TO DAY') END  status
		, cte.queued_dttm + avg_hour_start AS predict_start_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.start_dttm ELSE cte.start_dttm END  start_dttm
		, cte.start_dttm + avg_hour_end AS predict_end_dttm
		, CASE WHEN (cte.end_dttm IS NULL AND cte.start_dttm IS NULL) THEN t.end_dttm ELSE cte.end_dttm END  end_dttm
		, CASE WHEN cte.queued_dttm IS NULL THEN t.queued_dttm ELSE cte.queued_dttm END  queued_dttm
		, CASE WHEN cte.task_desc IS NULL THEN  t.task_desc ELSE  cte.task_desc END task_desc
	FROM ytd as t
	LEFT JOIN cte ON cte.name_tab = t.name_tab AND cte.task_name = t.task_name left join etl on etl.task_id = t.task_id
		WHERE t.name_tab in (...)
) as a
order by a.main_tab, a.start_dttm;




