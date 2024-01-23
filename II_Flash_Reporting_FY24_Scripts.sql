CREATE TABLE fin_insights.rev_summ 
(
    fiscalyearquartercode character varying(8) ENCODE lzo,
    load_type character varying(52) ENCODE lzo,
    flash_month character varying(25) ENCODE lzo,
    flash_month_version character varying(8) ENCODE lzo,
    region character varying(10) ENCODE lzo,
    programtype character varying(25) ENCODE lzo,
    gross_new_enrolls_k character varying(25) ENCODE lzo,
    cum_enrolls_k character varying(25) ENCODE lzo,
    net_rev_m character varying(25) ENCODE lzo,
    gm_m character varying(25) ENCODE lzo,
    gm_pts character varying(25) ENCODE lzo,
    r_d character varying(25) ENCODE lzo,
    marketing character varying(25) ENCODE lzo,
    admin character varying(25) ENCODE lzo,
    opex character varying(25) ENCODE lzo,
    oop_m character varying(25) ENCODE lzo,
    oop_pts character varying(25) ENCODE lzo,
    fof_rnk integer ENCODE az64,
    yoy_rnk integer ENCODE az64,
    load_date timestamp without time zone ENCODE az64,
    is_active integer ENCODE az64,
    flash_period character varying(25) ENCODE lzo,
    fsc character varying(25) ENCODE lzo,
    gne_operational character varying(25) ENCODE lzo,
    cum_operational character varying(25) ENCODE lzo
)
DISTSTYLE AUTO;

---------------------------------------------------------Master Stored Procedure to Load Flash Reporting Data-----------------------------------------------------
CREATE OR REPLACE PROCEDURE fin_insights.load_cos_rev_data_pbi()
 LANGUAGE plpgsql
AS $_$

DECLARE get_flash_mon varchar(max) := '';
        flash_mon varchar(3) := '';
        exec_del_cmd varchar(max) :=  ''; 
        get_curr_fls_mon varchar(max) := '';
        get_prev_fls_mon varchar(max) := '';
        get_curr_fls_mon_fiscal_year varchar(max) := '';
        get_prev_fls_mon_fiscal_year varchar(max) := ''; 
        get_master_load_flag varchar(max) := '';
        get_curr_fls_mon_dt_load_status varchar(max) := '';
        get_prev_fls_mon_dt_load_status varchar(max) := '';
        get_curr_bdg_dt_load_status varchar(max) := '';
        get_prev_act_fy_dt_load_status varchar(max) := '';
        curr_fls_mon varchar(3) := '';
        prev_fls_mon varchar(3) := '';
        curr_fls_mon_fiscal_year varchar(4) := '';
        prev_fls_mon_fiscal_year varchar(4) := '';
        mst_load_flag int;
        curr_fls_mon_dt_load_status int ;
        prev_fls_mon_dt_load_status int ;
        curr_bdg_dt_load_status int ;
        prev_act_fy_dt_load_status int ;

begin
	  get_master_load_flag := 'select master_load_flag from fin_insights.fls_pbi_config order by run_id desc limit 1'; 
	  EXECUTE get_master_load_flag INTO mst_load_flag ;
	 
	  if mst_load_flag = 1 then
	 
	  call fin_insights.load_curr_fls_data_fy24();
      call fin_insights.load_prev_fls_data_fy24();
  	  call fin_insights.load_budget_data_fy24();
  	  call fin_insights.load_prev_fy_act_data();
  	 
  	  end if ;
      
	  get_curr_fls_mon := 'select curr_fls_mon from fin_insights.fls_pbi_config order by run_id desc limit 1';
	  get_prev_fls_mon :=  'select prev_fls_mon from fin_insights.fls_pbi_config order by run_id desc limit 1';
	  get_curr_fls_mon_fiscal_year := 'select curr_fls_mon_fiscal_year from fin_insights.fls_pbi_config order by run_id desc limit 1'; 
	  get_prev_fls_mon_fiscal_year := 'select prev_fls_mon_fiscal_year from fin_insights.fls_pbi_config order by run_id desc limit 1'; 
      get_curr_fls_mon_dt_load_status := 'select curr_fls_mon_dt_load_status from fin_insights.fls_pbi_config order by run_id desc limit 1'; 
      get_prev_fls_mon_dt_load_status := 'select prev_fls_mon_dt_load_status from fin_insights.fls_pbi_config order by run_id desc limit 1'; 
      get_curr_bdg_dt_load_status := 'select curr_bdg_dt_load_status from fin_insights.fls_pbi_config order by run_id desc limit 1'; 
      get_prev_act_fy_dt_load_status := 'select prev_act_fy_dt_load_status from fin_insights.fls_pbi_config order by run_id desc limit 1'; 	  
 	  
 	  EXECUTE get_curr_fls_mon INTO curr_fls_mon ;
	  EXECUTE get_prev_fls_mon INTO prev_fls_mon ;
	  EXECUTE get_curr_fls_mon_fiscal_year INTO curr_fls_mon_fiscal_year ;
	  EXECUTE get_prev_fls_mon_fiscal_year INTO prev_fls_mon_fiscal_year ;
      EXECUTE get_curr_fls_mon_dt_load_status INTO curr_fls_mon_dt_load_status ;
      EXECUTE get_prev_fls_mon_dt_load_status INTO prev_fls_mon_dt_load_status ;
      EXECUTE get_curr_bdg_dt_load_status INTO curr_bdg_dt_load_status ;
      EXECUTE get_prev_act_fy_dt_load_status INTO prev_act_fy_dt_load_status ;	  
	  
	  if (curr_fls_mon_dt_load_status = 1 and prev_fls_mon_dt_load_status = 1 and curr_bdg_dt_load_status = 1 and prev_act_fy_dt_load_status = 1)  THEN
	  	
	  DELETE FROM fin_insights.rev_summ where flash_period = (select flash_period from fin_insights.fls_pbi_config order by run_id desc limit 1);
	 
	  UPDATE fin_insights.rev_summ
      SET is_active = 0;
    
      DROP TABLE IF EXISTS fin_insights.rev_from_curr_bdg ;
      CREATE TABLE fin_insights.rev_from_curr_bdg as 
      SELECT 
             fls.fiscalyearquartercode
             ,(SELECT DISTINCT fiscal_year_quarter_code FROM fin_insights.vw_dim_month WHERE calendar_year_month_day = to_date(date_trunc('month', dateadd(month,-1,current_date)),'yyyy-mm-dd')) as max_qtr_act
             ,(SELECT DISTINCT fiscal_year_quarter_code FROM fin_insights.vw_dim_month WHERE calendar_year_month_day = to_date(date_trunc('month', current_date),'yyyy-mm-dd')) as curr_qtr
             ,(SELECT mon_num_by_fyq FROM fin_insights.vw_dim_month WHERE calendar_year_month_day = to_date(date_trunc('month', dateadd(month,-1,current_date)),'yyyy-mm-dd'))::char(1) as max_mon_num_act
             ,CASE WHEN RIGHT(curr_qtr,2) in ('Q2','Q3','Q4') THEN LEFT(curr_qtr,7)||RIGHT(curr_qtr,1)-1 else 'FY'|| substring(curr_qtr,3,4)-1 || 'Q4' END AS prev_qtr
             ,CASE WHEN max_qtr_act = curr_qtr THEN curr_qtr
                   WHEN max_mon_num_act = 1 THEN curr_qtr 
              else prev_qtr END AS min_qtr_flsh
             ,RIGHT(fls.fiscalyearquartercode,2) || substring(fls.fiscalyearquartercode,5,2) || ' Budget'  AS load_type
             ,pbi.curr_fls_mon as flash_month
             ,'current' as flash_month_version
             ,region
             ,programtype
             ,grossnewenrollments_k ::varchar(25)
             ,cumenrollees_k ::varchar(25)
             ,netrevenue_m ::varchar(25)
             ,grossmargin_m ::varchar(25)
             ,gm_pts ::varchar(25) as grossmarginbyprecent
             ,rd ::varchar(25)
             ,marketing ::varchar(25)
             ,admin ::varchar(25)
             ,opex ::varchar(25)
             ,op_m ::varchar(25)
             ,op_pts as opbypercent
             ,RIGHT(fls.fiscalyearquartercode,2) || ' Budget'  AS metric_name
             ,fsc ::varchar(25)
             ,gne_operational ::varchar(25)
             ,paid_bc::varchar(25) as cum_operational
      FROM fin_insights.budget_summary_fy24 fls
      inner join (select * from fin_insights.fls_pbi_config order by run_id desc limit 1) pbi
      on fls.budget_period = pbi.curr_fls_mon_fiscal_year
      and fls.programtype in ('Instant Ink','Instant Toner','Instant Paper','Instant Services');   

      DROP TABLE IF EXISTS fin_insights.fy_total_from_curr_bdg ;
      CREATE TABLE fin_insights.fy_total_from_curr_bdg as 
      SELECT 
             'ALL' as fiscalyearquartercode ,
             '' as max_qtr_act,
             '' as curr_qtr,
             '' as max_mon_num_act,
             '' as prev_qtr,
             '' as min_qtr_flash, 
             'FY'||substring(max(fiscalyearquartercode),5,2) || ' Budget' as load_type, 
             flash_month,
             flash_month_version,
             region,
             programtype,
             sum(grossnewenrollments_k::numeric(38,2))::varchar(25) as grossnewenrollments_k,
             max(cumenrollees_k::numeric(38,2))::varchar(25) as cumenrollees_k 
            ,sum(netrevenue_m::numeric(38,2))::varchar(25) as netrevenue_m
            ,sum(grossmargin_m::numeric(38,2))::varchar(25) as grossmargin_m
            ,ROUND((sum(grossmargin_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as grossmarginbyprecent
            ,sum(rd::numeric(38,2))::varchar(25) as rd
            ,sum(marketing::numeric(38,2))::varchar(25) as marketing
            ,sum(admin::numeric(38,2))::varchar(25) as admin
            ,sum(opex::numeric(38,2))::varchar(25) as opex
            ,sum(op_m::numeric(38,2))::varchar(25) as op_m
            ,ROUND((sum(op_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as opbypercent
            ,'FY' || ' Budget'  AS metric_name      
            ,sum(fsc::numeric(38,2))::varchar(25) as fsc
            ,sum(gne_operational::numeric(38,2))::varchar(25) as gne_operational 
            ,max(cum_operational::numeric(38,2))::varchar(25) as cum_operational       
      FROM fin_insights.rev_from_curr_bdg
      GROUP BY flash_month,flash_month_version,region,programtype ;
     
      DROP TABLE IF EXISTS fin_insights.rev_from_curr_mon_fls ; 
      CREATE TABLE fin_insights.rev_from_curr_mon_fls as 
      SELECT 
             fls.fiscalyearquartercode
             ,(SELECT DISTINCT fiscal_year_quarter_code FROM fin_insights.vw_dim_month WHERE calendar_year_month_day = to_date(date_trunc('month', dateadd(month,-1,current_date)),'yyyy-mm-dd')) as max_qtr_act
             ,(SELECT DISTINCT fiscal_year_quarter_code FROM fin_insights.vw_dim_month WHERE calendar_year_month_day = to_date(date_trunc('month', current_date),'yyyy-mm-dd')) as curr_qtr
             ,(SELECT mon_num_by_fyq FROM fin_insights.vw_dim_month WHERE calendar_year_month_day = to_date(date_trunc('month', dateadd(month,-1,current_date)),'yyyy-mm-dd'))::char(1) as max_mon_num_act
             ,CASE WHEN RIGHT(curr_qtr,2) in ('Q2','Q3','Q4') THEN LEFT(curr_qtr,7)||RIGHT(curr_qtr,1)-1 else 'FY'|| substring(curr_qtr,3,4)-1 || 'Q4' END AS prev_qtr
             ,CASE WHEN max_qtr_act = curr_qtr THEN curr_qtr
                   WHEN max_mon_num_act = 1 THEN curr_qtr 
              else prev_qtr END AS min_qtr_flsh
             ,CASE WHEN fls.fiscalyearquartercode >= min_qtr_flsh THEN RIGHT(fls.fiscalyearquartercode,2) || substring(fls.fiscalyearquartercode,5,2) || ' ' || flash_month || ' Flash'  
              else RIGHT(fls.fiscalyearquartercode,2) || substring(fls.fiscalyearquartercode,5,2) ||  ' ' || flash_month || ' Act' END AS load_type
             ,flash_month as flash_month
             ,'current' as flash_month_version
             ,region
             ,programtype
             ,grossnewenrollments_k ::varchar(25)
             ,cumenrollees_k ::varchar(25)
             ,netrevenue_m ::varchar(25)
             ,grossmargin_m ::varchar(25)
    --         ,gm_pts ::varchar(25) as grossmarginbyprecent
             ,(ROUND((cast(gm_pts as float)*100),2))::varchar(25) || '%' as grossmarginbyprecent
             ,rd ::varchar(25)
             ,marketing ::varchar(25)
             ,admin ::varchar(25)
             ,opex ::varchar(25)
             ,op_m ::varchar(25)
        --     ,op_pts::varchar(25) as opbypercent
             ,(ROUND((cast(op_pts as float)*100),2))::varchar(25) || '%' as opbypercent
             ,CASE WHEN fls.fiscalyearquartercode >= min_qtr_flsh THEN RIGHT(fls.fiscalyearquartercode,2) || ' ' || flash_month || ' Flash'  
              else RIGHT(fls.fiscalyearquartercode,2) || ' ' || flash_month || ' Act' end as metric_name
             ,fsc ::varchar(25)
             ,gne_operational ::varchar(25)
             ,paid_bc::varchar(25) as cum_operational           
             ,dense_rank() over (order by right(fls.fiscalyearquartercode,1) ) as fy_curr_q_rnk
             ,case when right(fls.fiscalyearquartercode,1) = 1 then 2
                   when right(fls.fiscalyearquartercode,1) = 2 then 3
                   when right(fls.fiscalyearquartercode,1) = 3 then 4
                   when right(fls.fiscalyearquartercode,1) = 4 then 1 end as fy_prev_q_rnk
      FROM fin_insights.flash_summary_fy24 fls
      inner join (select * from fin_insights.fls_pbi_config order by run_id desc limit 1) pbi on
      fls.flash_period = pbi.flash_period
      and fls.programtype in ('Instant Ink','Instant Toner','Instant Paper','Instant Services');
      
      DROP TABLE IF EXISTS fin_insights.fy_total_from_curr_mon_fls ;
      CREATE TABLE fin_insights.fy_total_from_curr_mon_fls as 
      SELECT 
            'ALL' as fiscalyearquartercode ,
            '' as max_qtr_act,
            '' as curr_qtr,
            '' as max_mon_num_act,
            '' as prev_qtr,
            '' as min_qtr_flash, 
            'FY'||substring(max(fiscalyearquartercode),5,2) || ' ' || flash_month || ' Flash' as load_type, 
            flash_month,
            flash_month_version,
            region,
            programtype,
            sum(grossnewenrollments_k::numeric(38,2))::varchar(25) as grossnewenrollments_k,
            max(cumenrollees_k::numeric(38,2))::varchar(25) as cumenrollees_k 
           ,sum(netrevenue_m::numeric(38,2))::varchar(25) as netrevenue_m
           ,sum(grossmargin_m::numeric(38,2))::varchar(25) as grossmargin_m
           ,ROUND((sum(grossmargin_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as grossmarginbyprecent
           ,sum(rd::numeric(38,2))::varchar(25) as rd
           ,sum(marketing::numeric(38,2))::varchar(25) as marketing
           ,sum(admin::numeric(38,2))::varchar(25) as admin
           ,sum(opex::numeric(38,2))::varchar(25) as opex
           ,sum(op_m::numeric(38,2))::varchar(25) as op_m
           ,ROUND((sum(op_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as opbypercent
           ,'FY'|| ' ' || flash_month || ' Flash' as metric_name
           ,sum(fsc::numeric(38,2))::varchar(25) as fsc
           ,sum(gne_operational::numeric(38,2))::varchar(25) as gne_operational 
           ,max(cum_operational::numeric(38,2))::varchar(25) as cum_operational      
      FROM fin_insights.rev_from_curr_mon_fls 
      GROUP BY flash_month,flash_month_version,region,programtype ;
     
      DROP TABLE IF EXISTS fin_insights.inst_svc_per_reg_curr_mon_fls ;
      CREATE TABLE fin_insights.inst_svc_per_reg_curr_mon_fls as 
      SELECT 
            curr.fiscalyearquartercode ,
            '' as max_qtr_act,
            '' as curr_qtr,
            '' as max_mon_num_act,
            '' as prev_qtr,
            '' as min_qtr_flsh, 
            curr.load_type, 
            curr.flash_month,
            curr.flash_month_version,
            curr.region,
            'Instant Services' as programtype,
            sum(case when curr.programtype in ('Instant Ink', 'Instant Toner') then curr.grossnewenrollments_k end::numeric(38,2))::varchar(25) as grossnewenrollments_k,
            sum(case when curr.programtype in ('Instant Ink', 'Instant Toner') then curr.cumenrollees_k end::numeric(38,2))::varchar(25) as cumenrollees_k 
           ,sum(curr.netrevenue_m::numeric(38,2))::varchar(25) as netrevenue_m
           ,sum(curr.grossmargin_m::numeric(38,2))::varchar(25) as grossmargin_m
           ,ROUND((sum(curr.grossmargin_m::numeric(38,2))/sum(nullif(curr.netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as grossmarginbyprecent
           ,sum(rd::numeric(38,2))::varchar(25) as rd
           ,sum(marketing::numeric(38,2))::varchar(25) as marketing
           ,sum(admin::numeric(38,2))::varchar(25) as admin
           ,sum(opex::numeric(38,2))::varchar(25) as opex
           ,sum(op_m::numeric(38,2))::varchar(25) as op_m
           ,ROUND((sum(op_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as opbypercent
           ,metric_name
           ,sum(fsc::numeric(38,2))::varchar(25) as fsc
           ,sum(case when curr.programtype in ('Instant Ink', 'Instant Toner') then curr.gne_operational end::numeric(38,2))::varchar(25) as gne_operational
           ,sum(case when curr.programtype in ('Instant Ink', 'Instant Toner') then curr.cum_operational end::numeric(38,2))::varchar(25) as cum_operational      
           ,dense_rank() over (order by right(curr.fiscalyearquartercode,1) ) as fy_curr_q_rnk
           ,case when right(curr.fiscalyearquartercode,1) = 1 then 2
                 when right(curr.fiscalyearquartercode,1) = 2 then 3
                 when right(curr.fiscalyearquartercode,1) = 3 then 4
                 when right(curr.fiscalyearquartercode,1) = 4 then 1 end as fy_prev_q_rnk
      FROM fin_insights.rev_from_curr_mon_fls curr
      where region <> 'WW' 
      GROUP BY curr.fiscalyearquartercode,curr.load_type,curr.flash_month_version,curr.flash_month,curr.region,metric_name ; 
     
/*   This part is commented as data is flowing directly from currently flash cycle csv file
      
      DROP TABLE IF EXISTS fin_insights.inst_svc_ww_curr_mon_fls ;
      CREATE TABLE fin_insights.inst_svc_ww_curr_mon_fls as 
      SELECT 
      curr.fiscalyearquartercode ,
      '' as max_qtr_act,
      '' as curr_qtr,
      '' as max_mon_num_act,
      '' as prev_qtr,
      '' as min_qtr_flash, 
      curr.load_type, 
      curr.flash_month,
      'current' as flash_month_version,
  --  case when curr.region = 'WW' and programtype = 'Instant Paper' then 'NA' else curr.region end as region1,
      curr.region,
      'Instant Services' as programtype,
      sum(case when curr.programtype in ('Instant Ink', 'Instant Toner') then curr.grossnewenrollments_k end::numeric(38,2))::varchar(25) as grossnewenrollments_k,
      sum(case when curr.programtype in ('Instant Ink', 'Instant Toner') then curr.cumenrollees_k end::numeric(38,2))::varchar(25) as cumenrollees_k 
     ,sum(curr.netrevenue_m::numeric(38,2))::varchar(25) as netrevenue_m
     ,sum(curr.grossmargin_m::numeric(38,2))::varchar(25) as grossmargin_m
     ,ROUND((sum(curr.grossmargin_m::numeric(38,2))/sum(nullif(curr.netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as grossmarginbyprecent
     ,sum(rd::numeric(38,2))::varchar(25) as rd
     ,sum(marketing::numeric(38,2))::varchar(25) as marketing
     ,sum(admin::numeric(38,2))::varchar(25) as admin
     ,sum(opex::numeric(38,2))::varchar(25) as opex
     ,sum(op_m::numeric(38,2))::varchar(25) as op_m
     ,ROUND((sum(op_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as opbypercent
     ,sum(fsc::numeric(38,2))::varchar(25) as fsc
     ,sum(case when curr.programtype in ('Instant Ink', 'Instant Toner') then curr.gne_operational end::numeric(38,2))::varchar(25) as gne_operational
     ,sum(case when curr.programtype in ('Instant Ink', 'Instant Toner') then curr.cum_operational end::numeric(38,2))::varchar(25) as cum_operational       
      FROM fin_insights.rev_from_curr_mon_fls curr
      where region IN ('WW') and curr.programtype in ('Instant Ink','Instant Toner','Instant Paper')
      GROUP BY curr.fiscalyearquartercode,curr.load_type,curr.flash_month,curr.region ; 
      
 */ 
     
      DROP TABLE IF EXISTS fin_insights.inst_svc_per_reg_fy_curr_mon_fls ; 
      CREATE TABLE fin_insights.inst_svc_per_reg_fy_curr_mon_fls as 
      SELECT 
            cfy.fiscalyearquartercode ,
            '' as max_qtr_act,
            '' as curr_qtr,
            '' as max_mon_num_act,
            '' as prev_qtr,
            '' as min_qtr_flash, 
            cfy.load_type, 
            cfy.flash_month,
            cfy.flash_month_version,
            cfy.region,
            'Instant Services' as programtype,
            sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.grossnewenrollments_k end::numeric(38,2))::varchar(25) as grossnewenrollments_k,
            sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.cumenrollees_k end::numeric(38,2))::varchar(25) as cumenrollees_k 
           ,sum(cfy.netrevenue_m::numeric(38,2))::varchar(25) as netrevenue_m
           ,sum(cfy.grossmargin_m::numeric(38,2))::varchar(25) as grossmargin_m
           ,ROUND((sum(cfy.grossmargin_m::numeric(38,2))/sum(nullif(cfy.netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as grossmarginbyprecent
           ,sum(rd::numeric(38,2))::varchar(25) as rd
           ,sum(marketing::numeric(38,2))::varchar(25) as marketing
           ,sum(admin::numeric(38,2))::varchar(25) as admin
           ,sum(opex::numeric(38,2))::varchar(25) as opex
           ,sum(op_m::numeric(38,2))::varchar(25) as op_m
           ,ROUND((sum(op_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as opbypercent
           ,metric_name
           ,sum(fsc::numeric(38,2))::varchar(25) as fsc
           ,sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.gne_operational end::numeric(38,2))::varchar(25) as gne_operational
           ,sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.cum_operational end::numeric(38,2))::varchar(25) as cum_operational     
      FROM fin_insights.fy_total_from_curr_mon_fls cfy
      where region <> 'WW'
      GROUP BY cfy.fiscalyearquartercode,cfy.load_type,cfy.flash_month,cfy.flash_month_version,cfy.region,metric_name ;
 
 /*    This part is commented as data is flowing directly from current flash cycle csv file
  
      DROP TABLE IF EXISTS fin_insights.inst_svc_ww_fy_curr_mon_fls ;
      CREATE TABLE fin_insights.inst_svc_ww_fy_curr_mon_fls as 
      SELECT 
            cfy.fiscalyearquartercode ,
            '' as max_qtr_act,
            '' as curr_qtr,
            '' as max_mon_num_act,
            '' as prev_qtr,
            '' as min_qtr_flash, 
            cfy.load_type, 
            cfy.flash_month,
            cfy.flash_month_version,
   --         case when cfy.region = 'WW' and programtype = 'Instant Paper' then 'NA' else cfy.region end as region1,
            cfy.region,
            'Instant Services' as programtype,
            sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.grossnewenrollments_k end::numeric(38,2))::varchar(25) as grossnewenrollments_k,
            sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.cumenrollees_k end::numeric(38,2))::varchar(25) as cumenrollees_k 
           ,sum(cfy.netrevenue_m::numeric(38,2))::varchar(25) as netrevenue_m
           ,sum(cfy.grossmargin_m::numeric(38,2))::varchar(25) as grossmargin_m
           ,ROUND((sum(cfy.grossmargin_m::numeric(38,2))/sum(nullif(cfy.netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as grossmarginbyprecent
           ,sum(rd::numeric(38,2))::varchar(25) as rd
           ,sum(marketing::numeric(38,2))::varchar(25) as marketing
           ,sum(admin::numeric(38,2))::varchar(25) as admin
           ,sum(opex::numeric(38,2))::varchar(25) as opex
           ,sum(op_m::numeric(38,2))::varchar(25) as op_m
           ,ROUND((sum(op_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as opbypercent
           ,metric_name
           ,sum(fsc::numeric(38,2))::varchar(25) as fsc
           ,sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.gne_operational end::numeric(38,2))::varchar(25) as gne_operational
           ,sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.cum_operational end::numeric(38,2))::varchar(25) as cum_operational       
      FROM fin_insights.fy_total_from_curr_mon_fls cfy
      where cfy.region =  'WW'  and cfy.programtype in ('Instant Ink','Instant Toner','Instant Paper')
      GROUP BY cfy.fiscalyearquartercode,cfy.load_type,cfy.flash_month,cfy.flash_month_version,cfy.region,metric_name ; 
     
 */    
      
      DROP TABLE IF EXISTS fin_insights.rev_from_prev_mon_flsh ; 
      CREATE TABLE fin_insights.rev_from_prev_mon_flsh as
      SELECT 
            fls.fiscalyearquartercode
            ,(SELECT DISTINCT fiscal_year_quarter_code FROM fin_insights.vw_dim_month WHERE calendar_year_month_day = to_date(date_trunc('month', dateadd(month,-2,current_date)),'yyyy-mm-dd')) as max_qtr_act
            ,(SELECT DISTINCT fiscal_year_quarter_code FROM fin_insights.vw_dim_month WHERE calendar_year_month_day = to_date(date_trunc('month', dateadd(month,-1,current_date)),'yyyy-mm-dd')) as curr_qtr
            ,(SELECT mon_num_by_fyq FROM fin_insights.vw_dim_month WHERE calendar_year_month_day = to_date(date_trunc('month', dateadd(month,-2,current_date)),'yyyy-mm-dd'))::char(1) as max_mon_num_act
            -- ,(SELECT DISTINCT fiscal_year_quarter_code FROM fin_insights.vw_dim_month WHERE calendar_year_month_day = to_date(date_trunc('month', dateadd(month,-4,current_date)),'yyyy-mm-dd')) as prev_qtr
            , CASE WHEN RIGHT(curr_qtr,2) in ('Q2','Q3','Q4') THEN LEFT(curr_qtr,7)||RIGHT(curr_qtr,1)-1 ELSE 'FY'|| substring(curr_qtr,3,4)-1 || 'Q4' END AS prev_qtr
            ,CASE WHEN max_qtr_act = curr_qtr THEN curr_qtr
                  WHEN max_mon_num_act = 1 THEN curr_qtr 
             ELSE prev_qtr END AS min_qtr_flsh
            ,CASE WHEN fls.fiscalyearquartercode >= min_qtr_flsh THEN RIGHT(fls.fiscalyearquartercode,2) || substring(fls.fiscalyearquartercode,5,2) || ' ' || flash_month || ' Flash'  
             else RIGHT(fls.fiscalyearquartercode,2) || substring(fls.fiscalyearquartercode,5,2) || ' ' || flash_month || ' Act' END AS load_type
            ,pbi.curr_fls_mon as flash_month
            ,'previous' as flash_month_version
            ,region
            ,programtype
            ,grossnewenrollments_k ::varchar(25)
            ,cumenrollees_k ::varchar(25)
            ,netrevenue_m ::varchar(25)
            ,grossmargin_m ::varchar(25)
    --      ,gm_pts ::varchar(25) as grossmarginbyprecent
            ,(ROUND((cast(gm_pts as float)*100),2))::varchar(25) || '%' as grossmarginbyprecent
            ,rd ::varchar(25)
            ,marketing ::varchar(25)
            ,admin ::varchar(25)
            ,opex ::varchar(25)
            ,op_m ::varchar(25)
    --       ,op_pts::varchar(25) as opbypercent
            ,(ROUND((cast(op_pts as float)*100),2))::varchar(25) || '%' as opbypercent
            ,CASE WHEN fls.fiscalyearquartercode >= min_qtr_flsh THEN RIGHT(fls.fiscalyearquartercode,2) || ' ' || flash_month || ' Flash'  
              else RIGHT(fls.fiscalyearquartercode,2) || ' ' || flash_month || ' Act' END AS metric_name
            ,fsc ::varchar(25)
            ,gne_operational ::varchar(25)
            ,paid_bc::varchar(25) as cum_operational       
      FROM fin_insights.flash_summary_fy24 fls
      join (select * from fin_insights.fls_pbi_config order by run_id desc limit 1) pbi on
      fls.flash_month = pbi.prev_fls_mon
      and fls.flash_period = pbi.prev_fls_mon||right(prev_fls_mon_fiscal_year,2)
      where programtype in ('Instant Ink','Instant Toner','Instant Paper','Instant Services') ;
     
      DROP TABLE IF EXISTS fin_insights.fy_total_from_prev_mon_fls ;
      CREATE TABLE fin_insights.fy_total_from_prev_mon_fls as 
      SELECT
            'ALL' as fiscalyearquartercode ,
            '' as max_qtr_act,
            '' as curr_qtr,
            '' as max_mon_num_act,
            '' as prev_qtr,
            '' as min_qtr_flsh, 
            'FY'||substring(max(fiscalyearquartercode),5,2) || ' ' || (select prev_fls_mon from fin_insights.fls_pbi_config order by run_id desc limit 1) || ' Flash' as load_type, 
            flash_month,
            flash_month_version,
            region,
            programtype,
            sum(grossnewenrollments_k::numeric(38,2))::varchar(25) as grossnewenrollments_k ,
            max(cumenrollees_k::numeric(38,2))::varchar(25) as cumenrollees_k
           ,sum(netrevenue_m::numeric(38,2))::varchar(25) as netrevenue_m
           ,sum(grossmargin_m::numeric(38,2))::varchar(25) as grossmargin_m
           ,ROUND((sum(grossmargin_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as grossmarginbyprecent
           ,sum(rd::numeric(38,2))::varchar(25) as rd
           ,sum(marketing::numeric(38,2))::varchar(25) as marketing
           ,sum(admin::numeric(38,2))::varchar(25) as admin
           ,sum(opex::numeric(38,2))::varchar(25) as opex
           ,sum(op_m::numeric(38,2))::varchar(25) as op_m
           ,ROUND((sum(op_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as opbypercent
           ,('FY'|| ' ' || (select prev_fls_mon from fin_insights.fls_pbi_config order by run_id desc limit 1) || ' Flash')::varchar(12) as metric_name
           ,sum(fsc::numeric(38,2))::varchar(25) as fsc
           ,sum(gne_operational::numeric(38,2))::varchar(25) as gne_operational 
           ,max(cum_operational::numeric(38,2))::varchar(25) as cum_operational       
      FROM fin_insights.rev_from_prev_mon_flsh
      GROUP BY flash_month,flash_month_version,region,programtype ;
     
      DROP TABLE IF EXISTS fin_insights.inst_svc_per_reg_prev_mon_fls ;
      CREATE TABLE fin_insights.inst_svc_per_reg_prev_mon_fls as 
      SELECT 
            curr.fiscalyearquartercode ,
            '' as max_qtr_act,
            '' as curr_qtr,
            '' as max_mon_num_act,
            '' as prev_qtr,
            '' as min_qtr_flash, 
            curr.load_type, 
            curr.flash_month,
            curr.flash_month_version,
            curr.region,
            'Instant Services' as programtype,
            sum(case when curr.programtype in ('Instant Ink', 'Instant Toner') then curr.grossnewenrollments_k end::numeric(38,2))::varchar(25) as grossnewenrollments_k,
            sum(case when curr.programtype in ('Instant Ink', 'Instant Toner') then curr.cumenrollees_k end::numeric(38,2))::varchar(25) as cumenrollees_k 
           ,sum(curr.netrevenue_m::numeric(38,2))::varchar(25) as netrevenue_m
           ,sum(curr.grossmargin_m::numeric(38,2))::varchar(25) as grossmargin_m
           ,ROUND((sum(curr.grossmargin_m::numeric(38,2))/sum(nullif(curr.netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as grossmarginbyprecent
           ,sum(rd::numeric(38,2))::varchar(25) as rd
           ,sum(marketing::numeric(38,2))::varchar(25) as marketing
           ,sum(admin::numeric(38,2))::varchar(25) as admin
           ,sum(opex::numeric(38,2))::varchar(25) as opex
           ,sum(op_m::numeric(38,2))::varchar(25) as op_m
           ,ROUND((sum(op_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as opbypercent
           ,metric_name
           ,sum(fsc::numeric(38,2))::varchar(25) as fsc
           ,sum(case when curr.programtype in ('Instant Ink', 'Instant Toner') then curr.gne_operational end::numeric(38,2))::varchar(25) as gne_operational
           ,sum(case when curr.programtype in ('Instant Ink', 'Instant Toner') then curr.cum_operational end::numeric(38,2))::varchar(25) as cum_operational     
      FROM fin_insights.rev_from_prev_mon_flsh curr
      where region <> 'WW'
      GROUP BY curr.fiscalyearquartercode,curr.load_type,curr.flash_month,curr.flash_month_version,curr.region,metric_name ;
     
/*    This part is commented as data is flowing directly from previous flash period csv files
       
      DROP TABLE IF EXISTS fin_insights.inst_svc_ww_prev_mon_fls ;
      CREATE TABLE fin_insights.inst_svc_ww_prev_mon_fls as 
      SELECT 
      curr.fiscalyearquartercode ,
      '' as max_qtr_act,
      '' as curr_qtr,
      '' as max_mon_num_act,
      '' as prev_qtr,
      '' as min_qtr_flash, 
      curr.load_type, 
      curr.flash_month,
      'previous' as flash_month_version,
  --    case when curr.region = 'WW' and programtype = 'Instant Paper' then 'NA' else curr.region end as region1,
      curr.region,
      'Instant Services' as programtype,
      sum(case when curr.programtype in ('Instant Ink', 'Instant Toner') then curr.grossnewenrollments_k end::numeric(38,2))::varchar(25) as grossnewenrollments_k,
      sum(case when curr.programtype in ('Instant Ink', 'Instant Toner') then curr.cumenrollees_k end::numeric(38,2))::varchar(25) as cumenrollees_k 
     ,sum(curr.netrevenue_m::numeric(38,2))::varchar(25) as netrevenue_m
     ,sum(curr.grossmargin_m::numeric(38,2))::varchar(25) as grossmargin_m
     ,ROUND((sum(curr.grossmargin_m::numeric(38,2))/sum(nullif(curr.netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as grossmarginbyprecent
     ,sum(rd::numeric(38,2))::varchar(25) as rd
     ,sum(marketing::numeric(38,2))::varchar(25) as marketing
     ,sum(admin::numeric(38,2))::varchar(25) as admin
     ,sum(opex::numeric(38,2))::varchar(25) as opex
     ,sum(op_m::numeric(38,2))::varchar(25) as op_m
     ,ROUND((sum(op_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as opbypercent
     ,sum(fsc::numeric(38,2))::varchar(25) as fsc
      FROM fin_insights.rev_from_prev_mon_flsh curr
      where curr.region = 'WW' and curr.programtype in ('Instant Ink','Instant Toner','Instant Paper')
      GROUP BY curr.fiscalyearquartercode,curr.load_type,curr.flash_month,curr.region ; 
      
 */
         
      DROP TABLE IF EXISTS fin_insights.inst_svc_per_reg_fy_prev_mon_fls ;
      CREATE TABLE fin_insights.inst_svc_per_reg_fy_prev_mon_fls as 
      SELECT 
            cfy.fiscalyearquartercode ,
            '' as max_qtr_act,
            '' as curr_qtr,
            '' as max_mon_num_act,
            '' as prev_qtr,
            '' as min_qtr_flash, 
            cfy.load_type, 
            cfy.flash_month,
            cfy.flash_month_version,
            cfy.region,
            'Instant Services' as programtype,
            sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.grossnewenrollments_k end::numeric(38,2))::varchar(25) as grossnewenrollments_k,
            sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.cumenrollees_k end::numeric(38,2))::varchar(25) as cumenrollees_k 
           ,sum(cfy.netrevenue_m::numeric(38,2))::varchar(25) as netrevenue_m
           ,sum(cfy.grossmargin_m::numeric(38,2))::varchar(25) as grossmargin_m
           ,ROUND((sum(cfy.grossmargin_m::numeric(38,2))/sum(nullif(cfy.netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as grossmarginbyprecent
           ,sum(rd::numeric(38,2))::varchar(25) as rd
           ,sum(marketing::numeric(38,2))::varchar(25) as marketing
           ,sum(admin::numeric(38,2))::varchar(25) as admin
           ,sum(opex::numeric(38,2))::varchar(25) as opex
           ,sum(op_m::numeric(38,2))::varchar(25) as op_m
           ,ROUND((sum(op_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as opbypercent
           ,metric_name
           ,sum(fsc::numeric(38,2))::varchar(25) as fsc
           ,sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.gne_operational end::numeric(38,2))::varchar(25) as gne_operational
           ,sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.cum_operational end::numeric(38,2))::varchar(25) as cum_operational     
      FROM fin_insights.fy_total_from_prev_mon_fls cfy
      where region <> 'WW'
      GROUP BY cfy.fiscalyearquartercode,cfy.load_type,cfy.flash_month,cfy.flash_month_version,cfy.region,metric_name ;

/*    This part is commented as data is flowing directly from previous flash cycle csv file
     
      DROP TABLE IF EXISTS fin_insights.inst_svc_ww_fy_prev_mon_fls ;
      CREATE TABLE fin_insights.inst_svc_ww_fy_prev_mon_fls as 
      SELECT 
            cfy.fiscalyearquartercode ,
            '' as max_qtr_act,
            '' as curr_qtr,
            '' as max_mon_num_act,
            '' as prev_qtr,
            '' as min_qtr_flash, 
            cfy.load_type, 
            cfy.flash_month,
            cfy.flash_month_version,
            cfy.region,
            'Instant Services' as programtype,
            sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.grossnewenrollments_k end::numeric(38,2))::varchar(25) as grossnewenrollments_k,
            sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.cumenrollees_k end::numeric(38,2))::varchar(25) as cumenrollees_k 
           ,sum(cfy.netrevenue_m::numeric(38,2))::varchar(25) as netrevenue_m
           ,sum(cfy.grossmargin_m::numeric(38,2))::varchar(25) as grossmargin_m
           ,ROUND((sum(cfy.grossmargin_m::numeric(38,2))/sum(nullif(cfy.netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as grossmarginbyprecent
           ,sum(rd::numeric(38,2))::varchar(25) as rd
           ,sum(marketing::numeric(38,2))::varchar(25) as marketing
           ,sum(admin::numeric(38,2))::varchar(25) as admin
           ,sum(opex::numeric(38,2))::varchar(25) as opex
           ,sum(op_m::numeric(38,2))::varchar(25) as op_m
           ,ROUND((sum(op_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as opbypercent
           ,metric_name
           ,sum(fsc::numeric(38,2))::varchar(25) as fsc
           ,sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.gne_operational end::numeric(38,2))::varchar(25) as gne_operational
           ,sum(case when cfy.programtype in ('Instant Ink', 'Instant Toner') then cfy.cum_operational end::numeric(38,2))::varchar(25) as cum_operational      
      FROM fin_insights.fy_total_from_prev_mon_fls cfy
      where cfy.region = 'WW'  and cfy.programtype in ('Instant Ink','Instant Toner','Instant Paper')
      GROUP BY cfy.fiscalyearquartercode,cfy.load_type,cfy.flash_month,cfy.flash_month_version,cfy.region,metric_name ;

 */          
                                 
      DROP TABLE IF EXISTS fin_insights.rev_from_prev_fy_act ;
      CREATE TABLE fin_insights.rev_from_prev_fy_act as
      SELECT
            act.fiscalyearquartercode
           ,'' as max_qtr_act
           ,'' as curr_qtr
           ,'' as max_mon_num_act
           , '' prev_qtr
           ,'' min_qtr_flsh
           , RIGHT(act.fiscalyearquartercode,2) || RIGHT(pbi.curr_fls_mon_fiscal_year,2)-1 || ' Act' as load_type
           ,pbi.curr_fls_mon as flash_month
           ,'n/a' as flash_month_version
           ,region
           ,programtype
           ,grossnewenrollments_k ::varchar(25)
           ,cumenrollees_k ::varchar(25)
           ,netrevenue_m ::varchar(25)
           ,grossmargin_m ::varchar(25)
           ,grossmarginbyprecent ::varchar(25)
           ,rd ::varchar(25)
           ,marketing ::varchar(25)
           ,admin ::varchar(25)
           ,opex ::varchar(25)
           ,op_m ::varchar(25)
           ,opbypercent ::varchar(25)
           ,RIGHT(act.fiscalyearquartercode ,2) || ' Act' as metric_name
           ,fsc ::varchar(25)
           ,gne_operational ::varchar(25)
           ,cum_operational ::varchar(25)      
           ,dense_rank() over (order by right(act.fiscalyearquartercode,1) desc) as prev_fy_prev_q_rnk
      FROM fin_insights.actuals_summary act
      inner join (select * from fin_insights.fls_pbi_config order by run_id desc limit 1) pbi
      on act.actuals_period = 'FY'|| RIGHT(pbi.curr_fls_mon_fiscal_year,2)-1
      and programtype in ('Instant Ink','Instant Toner','Instant Paper','Instant Services') ;
      
/*    This part is commented as data is directly coming from previous FY actuals csv file
      
      DROP TABLE IF EXISTS fin_insights.inst_svc_per_reg_prev_fy_act ;
      CREATE TABLE fin_insights.inst_svc_per_reg_prev_fy_act as 
      SELECT 
            act.fiscalyearquartercode,
            '' as max_qtr_act,
            '' as curr_qtr,
            '' as max_mon_num_act,
            '' as prev_qtr,
            '' as min_qtr_flash, 
            act.load_type, 
            act.flash_month,
            act.flash_month_version,
            act.region,
            'Instant Services' as programtype,
            sum(case when act.programtype in ('Instant Ink', 'Instant Toner') then act.grossnewenrollments_k end::numeric(38,2))::varchar(25) as grossnewenrollments_k,
            sum(case when act.programtype in ('Instant Ink', 'Instant Toner') then act.cumenrollees_k end::numeric(38,2))::varchar(25) as cumenrollees_k 
           ,sum(act.netrevenue_m::numeric(38,2))::varchar(25) as netrevenue_m
           ,sum(act.grossmargin_m::numeric(38,2))::varchar(25) as grossmargin_m
           ,ROUND((sum(act.grossmargin_m::numeric(38,2))/sum(nullif(act.netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as grossmarginbyprecent
           ,sum(rd::numeric(38,2))::varchar(25) as rd
           ,sum(marketing::numeric(38,2))::varchar(25) as marketing
           ,sum(admin::numeric(38,2))::varchar(25) as admin
           ,sum(opex::numeric(38,2))::varchar(25) as opex
           ,sum(op_m::numeric(38,2))::varchar(25) as op_m
           ,ROUND((sum(op_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as opbypercent
           ,metric_name
           ,sum(fsc::numeric(38,2))::varchar(25) as fsc
           ,sum(case when act.programtype in ('Instant Ink', 'Instant Toner') then act.gne_operational end::numeric(38,2))::varchar(25) as gne_operational
           ,sum(case when act.programtype in ('Instant Ink', 'Instant Toner') then act.cum_operational end::numeric(38,2))::varchar(25) as cum_operational     
           ,dense_rank() over (order by right(act.fiscalyearquartercode,1) desc) as prev_fy_prev_q_rnk
      FROM fin_insights.rev_from_prev_fy_act act
      WHERE region <> 'WW' 
      GROUP BY act.fiscalyearquartercode,act.load_type,act.flash_month,act.flash_month_version,act.region,metric_name ;
      
 */     
      
      DROP TABLE IF EXISTS fin_insights.fy_total_from_prev_year_act ;
      CREATE TABLE fin_insights.fy_total_from_prev_year_act as
      SELECT 
            'ALL'as fiscalyearquartercode,
            '' as max_qtr_act,
            '' as curr_qtr,
            '' as max_mon_num_act,
            '' as prev_qtr,
            '' as min_qtr_flash, 
            'FY'||substring(max(fiscalyearquartercode),5,2) || ' Act' as load_type, 
            act.flash_month,
            act.flash_month_version,
            region,
            programtype,
            sum(grossnewenrollments_k::numeric(38,2)) ::varchar(25) as grossnewenrollments_k ,
            max(cumenrollees_k::numeric(38,2)) ::varchar(25) as cumenrollees_k
           ,sum(netrevenue_m::numeric(38,2)) ::varchar(25) as netrevenue_m
           ,sum(grossmargin_m::numeric(38,2)) ::varchar(25) as grossmargin_m
           ,ROUND((sum(grossmargin_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as grossmarginbyprecent
           ,sum(rd::numeric(38,2)) ::varchar(25) as rd
           ,sum(marketing::numeric(38,2)) ::varchar(25) as marketing
           ,sum(admin::numeric(38,2)) ::varchar(25) as admin
           ,sum(opex::numeric(38,2)) ::varchar(25) as opex
           ,sum(op_m::numeric(38,2)) ::varchar(25) as op_m
           ,ROUND((sum(op_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as opbypercent
           ,'FY'|| ' Act' as metric_name
           ,sum(fsc::numeric(38,2)) ::varchar(25) as fsc
           ,sum(gne_operational::numeric(38,2))::varchar(25) as gne_operational 
           ,max(cum_operational::numeric(38,2))::varchar(25) as cum_operational
           ,dense_rank() over (order by right(act.fiscalyearquartercode,1) desc) as prev_fy_prev_q_rnk
      FROM fin_insights.rev_from_prev_fy_act act
      GROUP BY flash_month,act.flash_month_version,region,programtype ; 
 
/*    This part is commented as data is flowing directly from previous FY actuals csv file
      DROP TABLE IF EXISTS fin_insights.inst_svc_per_reg_prev_fy_total ;
      CREATE TABLE fin_insights.inst_svc_per_reg_prev_fy_total as
      SELECT 
            'ALL'as fiscalyearquartercode,
            '' as max_qtr_act,
            '' as curr_qtr,
            '' as max_mon_num_act,
            '' as prev_qtr,
            '' as min_qtr_flash, 
            'FY'||substring(max(fiscalyearquartercode),5,2) || ' Act' as load_type, 
            act.flash_month,
            act.flash_month_version,
            region,
            programtype,
            sum(grossnewenrollments_k::numeric(38,2)) ::varchar(25) as grossnewenrollments_k ,
            max(cumenrollees_k::numeric(38,2)) ::varchar(25) as cumenrollees_k
           ,sum(netrevenue_m::numeric(38,2)) ::varchar(25) as netrevenue_m
           ,sum(grossmargin_m::numeric(38,2)) ::varchar(25) as grossmargin_m
           ,ROUND((sum(grossmargin_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as grossmarginbyprecent
           ,sum(rd::numeric(38,2)) ::varchar(25) as rd
           ,sum(marketing::numeric(38,2)) ::varchar(25) as marketing
           ,sum(admin::numeric(38,2)) ::varchar(25) as admin
           ,sum(opex::numeric(38,2)) ::varchar(25) as opex
           ,sum(op_m::numeric(38,2)) ::varchar(25) as op_m
           ,ROUND((sum(op_m::numeric(38,2))/sum(nullif(netrevenue_m::numeric(38,2),0)))*100,2) ||'%' as opbypercent
           ,'FY'|| ' Act' as metric_name
           ,sum(fsc::numeric(38,2)) ::varchar(25) as fsc
           ,sum(gne_operational::numeric(38,2))::varchar(25) as gne_operational 
           ,max(cum_operational::numeric(38,2))::varchar(25) as cum_operational      
      FROM fin_insights.inst_svc_per_reg_prev_fy_act act
      GROUP BY flash_month,act.flash_month_version,region,programtype ;  
      
*/              
           
      INSERT INTO fin_insights.rev_summ
      SELECT fiscalyearquartercode
            ,load_type
            ,fls.flash_month
            ,flash_month_version
            ,region
            ,programtype
            ,case when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%'  then to_char(grossnewenrollments_k::numeric(38,2), 'FM999,999,999,990D') else grossnewenrollments_k end as gross_new_enrolls_k
            ,case when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' then to_char(cumenrollees_k::numeric(38,2), 'FM999,999,999,990D') else cumenrollees_k end as cum_enrolls_k
            ,case when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' and programtype in ('Instant Ink', 'Instant Services') then to_char(netrevenue_m::numeric(38,2), 'FM$999,999,999,990D')
                  when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' and programtype in ('Instant Toner', 'Instant Paper') then to_char(netrevenue_m::numeric(38,2), 'FM$999,999,999,990D0')
             else netrevenue_m end as net_rev_m
            ,case when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' and programtype in ('Instant Ink', 'Instant Services') then to_char(grossmargin_m::numeric(38,2), 'FM$999,999,999,990D')
                  when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' and programtype in ('Instant Toner', 'Instant Paper') then to_char(grossmargin_m::numeric(38,2), 'FM$999,999,999,990D0')
             else grossmargin_m end as gm_m
            ,case when programtype in ('Instant Ink', 'Instant Services') then round((replace(grossmarginbyprecent,'%','')::numeric(38,2))) || '%' 
                  when programtype in ('Instant Toner', 'Instant Paper') then round((replace(grossmarginbyprecent,'%','')::numeric(38,2)),1) || '%' 
            else grossmarginbyprecent end as gm_pts
            ,case when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' and programtype in ('Instant Ink', 'Instant Services') then to_char(rd::numeric(38,2), 'FM$999,999,999,990D') 
                  when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' and programtype in ('Instant Toner', 'Instant Paper') then to_char(rd::numeric(38,2), 'FM$999,999,999,990D0') 
             else rd end as r_d
            ,case when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' and programtype in ('Instant Ink', 'Instant Services') then to_char(marketing::numeric(38,2), 'FM$999,999,999,990D') 
                  when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' and programtype in ('Instant Toner', 'Instant Paper') then to_char(marketing::numeric(38,2), 'FM$999,999,999,990D0') 
             else marketing end as marketing
            ,case when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' and programtype in ('Instant Ink', 'Instant Services') then to_char(admin::numeric(38,2), 'FM$999,999,999,990D') 
                  when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' and programtype in ('Instant Toner', 'Instant Paper') then to_char(admin::numeric(38,2), 'FM$999,999,999,990D0') 
             else admin end as admin
            ,case when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' and programtype in ('Instant Ink', 'Instant Services') then to_char(opex::numeric(38,2), 'FM$999,999,999,990D') 
                  when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' and programtype in ('Instant Toner', 'Instant Paper') then to_char(opex::numeric(38,2), 'FM$999,999,999,990D0')
             else opex end as opex
            ,case when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%'  and programtype in ('Instant Ink', 'Instant Services') then to_char(op_m::numeric(38,2), 'FM$999,999,999,990D') 
                  when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%'  and programtype in ('Instant Toner', 'Instant Paper') then to_char(op_m::numeric(38,2), 'FM$999,999,999,990D0') 
             else op_m end as oop_m
            ,case when programtype in ('Instant Ink', 'Instant Services') then round((replace(opbypercent,'%','')::numeric(38,2))) || '%' 
                  when programtype in ('Instant Toner', 'Instant Paper') then round((replace(opbypercent,'%','')::numeric(38,2)),1) || '%' 
             else opbypercent end as oop_pts
            ,pbi.mtr_rnk
            ,null as yoy_rnk
            ,date(current_timestamp)::timestamp as load_date
            ,1 as is_active
            ,(select distinct flash_period from fin_insights.fls_pbi_config order by run_id desc limit 1) as flash_period
            ,case when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' and programtype in ('Instant Ink', 'Instant Services') then to_char(fsc::numeric(38,2), 'FM$999,999,999,990D') 
                  when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' and programtype in ('Instant Toner', 'Instant Paper') then to_char(fsc::numeric(38,2), 'FM$999,999,999,990D0') 
             else fsc end as fsc
            ,case when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%'  then to_char(gne_operational::numeric(38,2), 'FM999,999,999,990D') else gne_operational end as gne_operational
            ,case when load_type not like 'YoY_%' and load_type not like 'QoQ_%' and load_type not like '%FoF_%' and load_type not like 'FoB_%' then to_char(cum_operational::numeric(38,2), 'FM999,999,999,990D') else cum_operational end as cum_operational
             
      FROM ( 
             select * from fin_insights.rev_from_curr_bdg
             
             union all
             
             select * from fin_insights.fy_total_from_curr_bdg            
            
             union all
             
             select fiscalyearquartercode
                   ,max_qtr_act 
                   ,curr_qtr
                   ,max_mon_num_act
                   ,prev_qtr
                   ,min_qtr_flsh
                   ,load_type
                   ,flash_month
                   ,flash_month_version
                   ,region
                   ,programtype
                   ,GrossNewEnrollments_K
                   ,CumEnrollees_K
                   ,NetRevenue_M
                   ,GrossMargin_M
                   ,GrossMarginByPrecent
                   ,RD
                   ,Marketing
                   ,admin
                   ,OPEX
                   ,OP_M
                   ,OPByPercent
                   ,metric_name
                   ,fsc
                   ,gne_operational
                   ,cum_operational
             from fin_insights.rev_from_curr_mon_fls 
             
             union all
             
             SELECT * FROM fin_insights.fy_total_from_curr_mon_fls 
             
             UNION all
             
             select fiscalyearquartercode
                   ,max_qtr_act 
                   ,curr_qtr
                   ,max_mon_num_act
                   ,prev_qtr
                   ,min_qtr_flsh
                   ,load_type
                   ,flash_month
                   ,flash_month_version
                   ,region
                   ,programtype
                   ,GrossNewEnrollments_K
                   ,CumEnrollees_K
                   ,NetRevenue_M
                   ,GrossMargin_M
                   ,GrossMarginByPrecent
                   ,RD
                   ,Marketing
                   ,admin
                   ,OPEX
                   ,OP_M
                   ,OPByPercent
                   ,metric_name
                   ,fsc
                   ,gne_operational
                   ,cum_operational                   
             from fin_insights.inst_svc_per_reg_curr_mon_fls
             
             union all
             
             SELECT * FROM fin_insights.inst_svc_per_reg_fy_curr_mon_fls 
             
 /*        
             union all
             
             SELECT * FROM fin_insights.inst_svc_ww_fy_curr_mon_fls
             
*/             
             
             union all
             
             select fiscalyearquartercode
                   ,max_qtr_act 
                   ,curr_qtr
                   ,max_mon_num_act
                   ,prev_qtr
                   ,min_qtr_flsh
                   ,load_type
                   ,flash_month
                   ,flash_month_version
                   ,region
                   ,programtype
                   ,GrossNewEnrollments_K
                   ,CumEnrollees_K
                   ,NetRevenue_M
                   ,GrossMargin_M
                   ,GrossMarginByPrecent
                   ,RD
                   ,Marketing
                   ,admin
                   ,OPEX
                   ,OP_M
                   ,OPByPercent
                   ,metric_name
                   ,fsc
                   ,gne_operational
                   ,cum_operational                   
             from fin_insights.rev_from_prev_mon_flsh 
             
             union all
             
             SELECT * FROM fin_insights.fy_total_from_prev_mon_fls
             
             union all
             
             select fiscalyearquartercode
                   ,max_qtr_act 
                   ,curr_qtr
                   ,max_mon_num_act
                   ,prev_qtr
                   ,min_qtr_flash as min_qtr_flsh
                   ,load_type
                   ,flash_month
                   ,flash_month_version
                   ,region
                   ,programtype
                   ,GrossNewEnrollments_K
                   ,CumEnrollees_K
                   ,NetRevenue_M
                   ,GrossMargin_M
                   ,GrossMarginByPrecent
                   ,RD
                   ,Marketing
                   ,admin
                   ,OPEX
                   ,OP_M
                   ,OPByPercent
                   ,metric_name
                   ,fsc
                   ,gne_operational
                   ,cum_operational                   
             from fin_insights.inst_svc_per_reg_prev_mon_fls
             
             union all
             
             SELECT * FROM fin_insights.inst_svc_per_reg_fy_prev_mon_fls
/*
             
             union all
             
             SELECT * FROM fin_insights.inst_svc_ww_fy_prev_mon_fls 
             
*/             
             
             union all
             
             select fiscalyearquartercode
                   ,max_qtr_act 
                   ,curr_qtr
                   ,max_mon_num_act
                   ,prev_qtr
                   ,min_qtr_flsh
                   ,load_type
                   ,flash_month
                   ,flash_month_version
                   ,region
                   ,programtype
                   ,GrossNewEnrollments_K
                   ,CumEnrollees_K
                   ,NetRevenue_M
                   ,GrossMargin_M
                   ,GrossMarginByPrecent
                   ,RD
                   ,Marketing
                   ,admin
                   ,OPEX
                   ,OP_M
                   ,OPByPercent
                   ,metric_name
                   ,fsc
                   ,gne_operational
                   ,cum_operational                   
             from fin_insights.rev_from_prev_fy_act
             
             union all
             
             SELECT * FROM fin_insights.fy_total_from_prev_year_act
/*             
             union all
             
             select fiscalyearquartercode
                   ,max_qtr_act 
                   ,curr_qtr
                   ,max_mon_num_act
                   ,prev_qtr
                   ,min_qtr_flash as min_qtr_flash
                   ,load_type
                   ,flash_month
                   ,flash_month_version
                   ,region
                   ,programtype
                   ,GrossNewEnrollments_K
                   ,CumEnrollees_K
                   ,NetRevenue_M
                   ,GrossMargin_M
                   ,GrossMarginByPrecent
                   ,RD
                   ,Marketing
                   ,admin
                   ,OPEX
                   ,OP_M
                   ,OPByPercent
                   ,metric_name
                   ,fsc
                   ,gne_operational
                   ,cum_operational                   
             from fin_insights.inst_svc_per_reg_prev_fy_act
             
             union all 
             
             select * from fin_insights.inst_svc_per_reg_prev_fy_total
             
 */            
             
             union all
             
             SELECT curr.fiscalyearquartercode ,
                   '' as max_qtr_act,
                   curr.curr_qtr,
                   '' as max_mon_num_act,
                   '' as prev_qtr,
                   '' as min_qtr_flsh, 
                   'vs ' || prev.load_type as load_type ,
                   curr.flash_month ,
                   curr.flash_month_version,
                   curr.region,
                   curr.programtype,
                   (curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)::varchar(25) as grossnewenrollments_k,
                   (curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)::varchar(25) as cumenrollees_k,
                   (curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)::varchar(25) as netrevenue_m,
                   (curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)::varchar(25) as grossmargin_m,
                   (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' as grossmarginbyprecent,
                   (curr.rd-prev.rd)::numeric(38,2)::varchar(25) as rd,
                   (curr.marketing-prev.marketing)::numeric(38,2)::varchar(25) as marketing,
                   (curr.admin-prev.admin)::numeric(38,2)::varchar(25) as admin,
                   (curr.opex-prev.opex)::numeric(38,2)::varchar(25) as opex,
                   (curr.op_m-prev.op_m)::numeric(38,2)::varchar(25) as op_m,
                   (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)) || '%' as opbypercent,
                   'vs ' || replace(prev.load_type,substring(prev.load_type,3,2),'') as metric_name,
                   (curr.fsc-prev.fsc)::numeric(38,2)::varchar(25) as fsc,
                   (curr.gne_operational-prev.gne_operational)::numeric(38,2)::varchar(25) as gne_operational,
                   (curr.cum_operational-prev.cum_operational)::numeric(38,2)::varchar(25) as cum_operational             
             FROM fin_insights.rev_from_curr_mon_fls curr
             INNER JOIN fin_insights.rev_from_prev_mon_flsh prev
             on curr.fiscalyearquartercode = prev.fiscalyearquartercode and curr.region = prev.region and curr.programtype = prev.programtype
             
             union all 
             
             SELECT 'ALL' as fiscalyearquartercode,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'vs ' || prev.load_type as load_type ,
                    curr.flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    (curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)::varchar(25) as grossnewenrollments_k,
                    (curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)::varchar(25) as cumenrollees_k,
                    (curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)::varchar(25) as netrevenue_m,
                    (curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)::varchar(25) as grossmargin_m,
                    (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' as grossmarginbyprecent,
                    (curr.rd-prev.rd)::numeric(38,2)::varchar(25) as rd,
                    (curr.marketing-prev.marketing)::numeric(38,2)::varchar(25) as marketing,
                    (curr.admin-prev.admin)::numeric(38,2)::varchar(25) as admin,
                    (curr.opex-prev.opex)::numeric(38,2)::varchar(25) as opex,
                    (curr.op_m-prev.op_m)::numeric(38,2)::varchar(25) as op_m,
                    (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)) || '%' as opbypercent,
                    'vs ' || replace(prev.load_type,substring(prev.load_type,3,2),'') as metric_name,
                    (curr.fsc-prev.fsc)::numeric(38,2)::varchar(25) as fsc,
                    (curr.gne_operational-prev.gne_operational)::numeric(38,2)::varchar(25) as gne_operational,
                    (curr.cum_operational-prev.cum_operational)::numeric(38,2)::varchar(25) as cum_operational              
             FROM fin_insights.fy_total_from_curr_mon_fls curr
             INNER JOIN fin_insights.fy_total_from_prev_mon_fls prev
             on curr.region = prev.region and curr.programtype = prev.programtype
             
             union all 
             
             SELECT curr.fiscalyearquartercode ,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'vs ' || prev.load_type as load_type ,
                    curr.flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    (curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)::varchar(25) as grossnewenrollments_k,
                    (curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)::varchar(25) as cumenrollees_k,
                    (curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)::varchar(25) as netrevenue_m,
                    (curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)::varchar(25) as grossmargin_m,
                    (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' as grossmarginbyprecent,
                    (curr.rd-prev.rd)::numeric(38,2)::varchar(25) as rd,
                    (curr.marketing-prev.marketing)::numeric(38,2)::varchar(25) as marketing,
                    (curr.admin-prev.admin)::numeric(38,2)::varchar(25) as admin,
                    (curr.opex-prev.opex)::numeric(38,2)::varchar(25) as opex,
                    (curr.op_m-prev.op_m)::numeric(38,2)::varchar(25) as op_m,
                    (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)) || '%' as opbypercent,
                     'vs ' || replace(prev.load_type,substring(prev.load_type,3,2),'') as metric_name,
                    (curr.fsc-prev.fsc)::numeric(38,2)::varchar(25) as fsc,
                    (curr.gne_operational-prev.gne_operational)::numeric(38,2)::varchar(25) as gne_operational,
                    (curr.cum_operational-prev.cum_operational)::numeric(38,2)::varchar(25) as cum_operational             
             FROM fin_insights.inst_svc_per_reg_curr_mon_fls curr
             INNER JOIN fin_insights.inst_svc_per_reg_prev_mon_fls prev
             on curr.fiscalyearquartercode = prev.fiscalyearquartercode and curr.region = prev.region and curr.programtype = prev.programtype
             
             union all
             
             SELECT curr.fiscalyearquartercode ,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'vs ' || prev.load_type as load_type ,
                    curr.flash_month as flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    (curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)::varchar(25) as grossnewenrollments_k,
                    (curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)::varchar(25) as cumenrollees_k,
                    (curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)::varchar(25) as netrevenue_m,
                    (curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)::varchar(25) as grossmargin_m,
                    (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' as grossmarginbyprecent,
                    (curr.rd-prev.rd)::numeric(38,2)::varchar(25) as rd,
                    (curr.marketing-prev.marketing)::numeric(38,2)::varchar(25) as marketing,
                    (curr.admin-prev.admin)::numeric(38,2)::varchar(25) as admin,
                    (curr.opex-prev.opex)::numeric(38,2)::varchar(25) as opex,
                    (curr.op_m-prev.op_m)::numeric(38,2)::varchar(25) as op_m,
                    (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)) || '%' as opbypercent,
                    'vs ' || replace(prev.load_type,substring(prev.load_type,3,2),'') as metric_name,
                    (curr.fsc-prev.fsc)::numeric(38,2)::varchar(25) as fsc,
                    (curr.gne_operational-prev.gne_operational)::numeric(38,2)::varchar(25) as gne_operational,
                    (curr.cum_operational-prev.cum_operational)::numeric(38,2)::varchar(25) as cum_operational             
             FROM fin_insights.inst_svc_per_reg_fy_curr_mon_fls curr
             INNER JOIN fin_insights.inst_svc_per_reg_fy_prev_mon_fls prev
             on curr.fiscalyearquartercode = prev.fiscalyearquartercode and curr.region = prev.region and curr.programtype = prev.programtype
             
             union all 
             
/*             SELECT curr.fiscalyearquartercode ,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'vs ' || prev.load_type as load_type ,
                    curr.flash_month as flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    (curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)::varchar(25) as grossnewenrollments_k,
                    (curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)::varchar(25) as cumenrollees_k,
                    (curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)::varchar(25) as netrevenue_m,
                    (curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)::varchar(25) as grossmargin_m,
                    (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' as grossmarginbyprecent,
                    (curr.rd-prev.rd)::numeric(38,2)::varchar(25) as rd,
                    (curr.marketing-prev.marketing)::numeric(38,2)::varchar(25) as marketing,
                    (curr.admin-prev.admin)::numeric(38,2)::varchar(25) as admin,
                    (curr.opex-prev.opex)::numeric(38,2)::varchar(25) as opex,
                    (curr.op_m-prev.op_m)::numeric(38,2)::varchar(25) as op_m,
                    (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)) || '%' as opbypercent,
                    'vs ' || replace(prev.load_type,substring(prev.load_type,3,2),'') as metric_name,
                    (curr.fsc-prev.fsc)::numeric(38,2)::varchar(25) as fsc,
                    (curr.gne_operational-prev.gne_operational)::numeric(38,2)::varchar(25) as gne_operational,
                    (curr.cum_operational-prev.cum_operational)::numeric(38,2)::varchar(25) as cum_operational             
             FROM fin_insights.inst_svc_ww_fy_curr_mon_fls curr
             INNER JOIN fin_insights.inst_svc_ww_fy_prev_mon_fls prev
             on curr.fiscalyearquartercode = prev.fiscalyearquartercode and curr.region = prev.region and curr.programtype = prev.programtype 
             
             union all */
             
             SELECT curr.fiscalyearquartercode ,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'vs ' || prev.load_type as load_type ,
                    curr.flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    (curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)::varchar(25) as grossnewenrollments_k,
                    (curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)::varchar(25) as cumenrollees_k,
                    (curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)::varchar(25) as netrevenue_m,
                    (curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)::varchar(25) as grossmargin_m,
                    (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' as grossmarginbyprecent,
                    (curr.rd-prev.rd)::numeric(38,2)::varchar(25) as rd,
                    (curr.marketing-prev.marketing)::numeric(38,2)::varchar(25) as marketing,
                    (curr.admin-prev.admin)::numeric(38,2)::varchar(25) as admin,
                    (curr.opex-prev.opex)::numeric(38,2)::varchar(25) as opex,
                    (curr.op_m-prev.op_m)::numeric(38,2)::varchar(25) as op_m,
                    (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)) || '%' as opbypercent,
                    'vs ' || replace(prev.load_type,substring(prev.load_type,3,2),'') as metric_name,
                    (curr.fsc-prev.fsc)::numeric(38,2)::varchar(25) as fsc,
                    (curr.gne_operational-prev.gne_operational)::numeric(38,2)::varchar(25) as gne_operational,
                    (curr.cum_operational-prev.cum_operational)::numeric(38,2)::varchar(25) as cum_operational              
             FROM fin_insights.rev_from_curr_mon_fls curr
             left JOIN fin_insights.rev_from_curr_bdg prev
             on right(curr.fiscalyearquartercode,2) = right(prev.fiscalyearquartercode,2) 
             and curr.region = prev.region and curr.programtype = prev.programtype
         --    where upper(curr.fiscalyearquartercode) in ('FY2023Q1', 'FY2023Q2') and upper(prev.fiscalyearquartercode) in ('FY2023Q1', 'FY2023Q2')   
             
             union all
             
             SELECT curr.fiscalyearquartercode ,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'vs ' ||  prev.load_type as load_type ,
                    curr.flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    (curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)::varchar(25) as grossnewenrollments_k,
                    (curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)::varchar(25) as cumenrollees_k,
                    (curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)::varchar(25) as netrevenue_m,
                    (curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)::varchar(25) as grossmargin_m,
                    (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' as grossmarginbyprecent,
                    (curr.rd-prev.rd)::numeric(38,2)::varchar(25) as rd,
                    (curr.marketing-prev.marketing)::numeric(38,2)::varchar(25) as marketing,
                    (curr.admin-prev.admin)::numeric(38,2)::varchar(25) as admin,
                    (curr.opex-prev.opex)::numeric(38,2)::varchar(25) as opex,
                    (curr.op_m-prev.op_m)::numeric(38,2)::varchar(25) as op_m,
                    (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)) || '%' as opbypercent,
                    'vs ' || replace(prev.load_type,substring(prev.load_type,3,2),'') as metric_name,
                    (curr.fsc-prev.fsc)::numeric(38,2)::varchar(25) as fsc,
                    (curr.gne_operational-prev.gne_operational)::numeric(38,2)::varchar(25) as gne_operational,
                    (curr.cum_operational-prev.cum_operational)::numeric(38,2)::varchar(25) as cum_operational              
             FROM fin_insights.inst_svc_per_reg_curr_mon_fls curr
             left JOIN fin_insights.rev_from_curr_bdg prev
             on right(curr.fiscalyearquartercode,2) = right(prev.fiscalyearquartercode,2)
             and curr.region = prev.region and curr.programtype = prev.programtype
       --      where upper(curr.fiscalyearquartercode) in ('FY2023Q1', 'FY2023Q2') and upper(prev.fiscalyearquartercode) in ('FY2023Q1', 'FY2023Q2')
       
             union all 
             
             SELECT curr.fiscalyearquartercode ,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'vs ' || prev.load_type as load_type ,
                    curr.flash_month as flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    (curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)::varchar(25) as grossnewenrollments_k,
                    (curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)::varchar(25) as cumenrollees_k,
                    (curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)::varchar(25) as netrevenue_m,
                    (curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)::varchar(25) as grossmargin_m,
                    (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' as grossmarginbyprecent,
                    (curr.rd-prev.rd)::numeric(38,2)::varchar(25) as rd,
                    (curr.marketing-prev.marketing)::numeric(38,2)::varchar(25) as marketing,
                    (curr.admin-prev.admin)::numeric(38,2)::varchar(25) as admin,
                    (curr.opex-prev.opex)::numeric(38,2)::varchar(25) as opex,
                    (curr.op_m-prev.op_m)::numeric(38,2)::varchar(25) as op_m,
                    (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)) || '%' as opbypercent,
                    'vs ' || replace(prev.load_type,substring(prev.load_type,3,2),'') as metric_name,
                    (curr.fsc-prev.fsc)::numeric(38,2)::varchar(25) as fsc,
                    (curr.gne_operational-prev.gne_operational)::numeric(38,2)::varchar(25) as gne_operational,
                    (curr.cum_operational-prev.cum_operational)::numeric(38,2)::varchar(25) as cum_operational          
             FROM fin_insights.fy_total_from_curr_mon_fls curr
             INNER JOIN fin_insights.fy_total_from_curr_bdg prev on 
             curr.region = prev.region and curr.programtype = prev.programtype    
             
             union all 
             
             SELECT curr.fiscalyearquartercode ,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'vs ' || prev.load_type as load_type ,
                    curr.flash_month as flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    (curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)::varchar(25) as grossnewenrollments_k,
                    (curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)::varchar(25) as cumenrollees_k,
                    (curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)::varchar(25) as netrevenue_m,
                    (curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)::varchar(25) as grossmargin_m,
                    (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' as grossmarginbyprecent,
                    (curr.rd-prev.rd)::numeric(38,2)::varchar(25) as rd,
                    (curr.marketing-prev.marketing)::numeric(38,2)::varchar(25) as marketing,
                    (curr.admin-prev.admin)::numeric(38,2)::varchar(25) as admin,
                    (curr.opex-prev.opex)::numeric(38,2)::varchar(25) as opex,
                    (curr.op_m-prev.op_m)::numeric(38,2)::varchar(25) as op_m,
                    (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)) || '%' as opbypercent,
                    'vs ' || replace(prev.load_type,substring(prev.load_type,3,2),'') as metric_name,
                    (curr.fsc-prev.fsc)::numeric(38,2)::varchar(25) as fsc,
                    (curr.gne_operational-prev.gne_operational)::numeric(38,2)::varchar(25) as gne_operational,
                    (curr.cum_operational-prev.cum_operational)::numeric(38,2)::varchar(25) as cum_operational             
             FROM fin_insights.inst_svc_per_reg_fy_curr_mon_fls curr
             INNER JOIN fin_insights.fy_total_from_curr_bdg prev on 
             curr.region = prev.region and curr.programtype = prev.programtype              
             
             union all
      
             SELECT curr.fiscalyearquartercode ,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    case when curr.fy_curr_q_rnk = 1 then 'vs PriorQ ' || right(curr.fiscalyearquartercode,2) || ' vs ' || right(prev.fiscalyearquartercode,2)  
                         when curr.fy_curr_q_rnk in (2,3,4)  then 'vs PriorQ ' || right(curr.fiscalyearquartercode,2) || ' vs ' || right(curr1.fiscalyearquartercode,2)  end as load_type ,
                    curr.flash_month as flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    case when curr.fy_curr_q_rnk = 1 then (curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.grossnewenrollments_k-curr1.grossnewenrollments_k)::numeric(38,2)::varchar(25) end  as grossnewenrollments_k,
                    case when curr.fy_curr_q_rnk = 1 then (curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.cumenrollees_k-curr1.cumenrollees_k)::numeric(38,2)::varchar(25) end  as cumenrollees_k,
                    case when curr.fy_curr_q_rnk = 1 then (curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then  (curr.netrevenue_m-curr1.netrevenue_m)::numeric(38,2)::varchar(25) end as netrevenue_m,
                    case when curr.fy_curr_q_rnk = 1 then (curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.grossmargin_m-curr1.grossmargin_m)::numeric(38,2)::varchar(25) end  as grossmargin_m,
                    case when curr.fy_curr_q_rnk = 1 then (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(curr1.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' end as grossmarginbyprecent,
                    case when curr.fy_curr_q_rnk = 1 then (curr.rd-prev.rd)::numeric(38,2)::varchar(25) 
                         when curr.fy_curr_q_rnk in (2,3,4) then  (curr.rd-curr1.rd)::numeric(38,2)::varchar(25)  end as rd,
                    case when curr.fy_curr_q_rnk = 1 then (curr.marketing-prev.marketing)::numeric(38,2)::varchar(25) 
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.marketing-curr1.marketing)::numeric(38,2)::varchar(25) end as marketing,
                    case when curr.fy_curr_q_rnk = 1 then (curr.admin-prev.admin)::numeric(38,2)::varchar(25) 
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.admin-curr1.admin)::numeric(38,2)::varchar(25)  end as admin,
                    case when curr.fy_curr_q_rnk = 1 then (curr.opex-prev.opex)::numeric(38,2)::varchar(25) 
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.opex-curr1.opex)::numeric(38,2)::varchar(25)  end as opex,
                    case when curr.fy_curr_q_rnk = 1 then (curr.op_m-prev.op_m)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.op_m-curr1.op_m)::numeric(38,2)::varchar(25) end as op_m,
                    case when curr.fy_curr_q_rnk = 1 then (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(curr1.opbypercent,'%','')::numeric(38,2)) || '%' end as opbypercent,
                    case when curr.fy_curr_q_rnk = 1 then 'vs PriorQ ' || right(curr.fiscalyearquartercode,2) || ' vs ' || right(prev.fiscalyearquartercode,2)  
                         when curr.fy_curr_q_rnk in (2,3,4)  then 'vs PriorQ ' || right(curr.fiscalyearquartercode,2) || ' vs ' || right(curr1.fiscalyearquartercode,2)  end as metric_name ,
                    case when curr.fy_curr_q_rnk = 1 then (curr.fsc-prev.fsc)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.fsc-curr1.fsc)::numeric(38,2)::varchar(25) end as fsc,
                    case when curr.fy_curr_q_rnk = 1 then (curr.gne_operational-prev.gne_operational)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.gne_operational-curr1.gne_operational)::numeric(38,2)::varchar(25) end  as gne_operational,
                    case when curr.fy_curr_q_rnk = 1 then (curr.cum_operational-prev.cum_operational)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.cum_operational-curr1.cum_operational)::numeric(38,2)::varchar(25) end  as cum_operational                 
             FROM fin_insights.rev_from_curr_mon_fls curr
             left join fin_insights.rev_from_curr_mon_fls curr1
             on curr.fy_curr_q_rnk = curr1.fy_prev_q_rnk and curr.region = curr1.region and curr.programtype = curr1.programtype 
             and curr.fy_curr_q_rnk in (2,3,4)
             left JOIN fin_insights.rev_from_prev_fy_act prev
             on curr.fy_curr_q_rnk = prev.prev_fy_prev_q_rnk 
             and curr.fy_curr_q_rnk = 1
             and curr.region = prev.region and curr.programtype = prev.programtype 
      --       where curr.fiscalyearquartercode = 'FY2023Q1' and prev.fiscalyearquartercode = 'FY2022Q4' 
             
             union all 
             
             SELECT curr.fiscalyearquartercode ,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    case when curr.fy_curr_q_rnk = 1 then 'vs PriorQ ' || right(curr.fiscalyearquartercode,2) || ' vs ' || right(prev.fiscalyearquartercode,2)  
                         when curr.fy_curr_q_rnk in (2,3,4)  then 'vs PriorQ ' || right(curr.fiscalyearquartercode,2) || ' vs ' || right(curr1.fiscalyearquartercode,2)  end as load_type ,
                    curr.flash_month as flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    case when curr.fy_curr_q_rnk = 1 then (curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.grossnewenrollments_k-curr1.grossnewenrollments_k)::numeric(38,2)::varchar(25) end  as grossnewenrollments_k,
                    case when curr.fy_curr_q_rnk = 1 then (curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.cumenrollees_k-curr1.cumenrollees_k)::numeric(38,2)::varchar(25) end  as cumenrollees_k,
                    case when curr.fy_curr_q_rnk = 1 then (curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then  (curr.netrevenue_m-curr1.netrevenue_m)::numeric(38,2)::varchar(25) end as netrevenue_m,
                    case when curr.fy_curr_q_rnk = 1 then (curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.grossmargin_m-curr1.grossmargin_m)::numeric(38,2)::varchar(25) end  as grossmargin_m,
                    case when curr.fy_curr_q_rnk = 1 then (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(curr1.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' end as grossmarginbyprecent,
                    case when curr.fy_curr_q_rnk = 1 then (curr.rd-prev.rd)::numeric(38,2)::varchar(25) 
                         when curr.fy_curr_q_rnk in (2,3,4) then  (curr.rd-curr1.rd)::numeric(38,2)::varchar(25)  end as rd,
                    case when curr.fy_curr_q_rnk = 1 then (curr.marketing-prev.marketing)::numeric(38,2)::varchar(25) 
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.marketing-curr1.marketing)::numeric(38,2)::varchar(25) end as marketing,
                    case when curr.fy_curr_q_rnk = 1 then (curr.admin-prev.admin)::numeric(38,2)::varchar(25) 
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.admin-curr1.admin)::numeric(38,2)::varchar(25)  end as admin,
                    case when curr.fy_curr_q_rnk = 1 then (curr.opex-prev.opex)::numeric(38,2)::varchar(25) 
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.opex-curr1.opex)::numeric(38,2)::varchar(25)  end as opex,
                    case when curr.fy_curr_q_rnk = 1 then (curr.op_m-prev.op_m)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.op_m-curr1.op_m)::numeric(38,2)::varchar(25) end as op_m,
                    case when curr.fy_curr_q_rnk = 1 then (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(curr1.opbypercent,'%','')::numeric(38,2)) || '%' end as opbypercent,
                    case when curr.fy_curr_q_rnk = 1 then 'vs PriorQ ' || right(curr.fiscalyearquartercode,2) || ' vs ' || right(prev.fiscalyearquartercode,2)  
                         when curr.fy_curr_q_rnk in (2,3,4)  then 'vs PriorQ ' || right(curr.fiscalyearquartercode,2) || ' vs ' || right(curr1.fiscalyearquartercode,2)  end as metric_name ,                  
                    case when curr.fy_curr_q_rnk = 1 then (curr.fsc-prev.fsc)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.fsc-curr1.fsc)::numeric(38,2)::varchar(25) end as fsc,
                    case when curr.fy_curr_q_rnk = 1 then (curr.gne_operational-prev.gne_operational)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.gne_operational-curr1.gne_operational)::numeric(38,2)::varchar(25) end  as gne_operational,
                    case when curr.fy_curr_q_rnk = 1 then (curr.cum_operational-prev.cum_operational)::numeric(38,2)::varchar(25)
                         when curr.fy_curr_q_rnk in (2,3,4) then (curr.cum_operational-curr1.cum_operational)::numeric(38,2)::varchar(25) end  as cum_operational                                   
             FROM fin_insights.inst_svc_per_reg_curr_mon_fls curr
             left join fin_insights.inst_svc_per_reg_curr_mon_fls curr1
             on curr.fy_curr_q_rnk = curr1.fy_prev_q_rnk and curr.region = curr1.region and curr.programtype = curr1.programtype 
             and curr.fy_curr_q_rnk in (2,3,4)
             left JOIN fin_insights.rev_from_prev_fy_act prev
             on curr.fy_curr_q_rnk = prev.prev_fy_prev_q_rnk 
             and curr.fy_curr_q_rnk = 1
             and curr.region = prev.region and curr.programtype = prev.programtype 
    --         where curr.fiscalyearquartercode = 'FY2023Q1' and prev.fiscalyearquartercode = 'FY2022Q4'             
                       
             union all 
             
             SELECT curr.fiscalyearquartercode ,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'vs PriorY ' || right(curr.fiscalyearquartercode,2) as load_type ,
                    curr.flash_month as flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    (curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)::varchar(25) as grossnewenrollments_k,
                    (curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)::varchar(25) as cumenrollees_k,
                    (curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)::varchar(25) as netrevenue_m,
                    (curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)::varchar(25) as grossmargin_m,
                    (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' as grossmarginbyprecent,
                    (curr.rd-prev.rd)::numeric(38,2)::varchar(25) as rd,
                    (curr.marketing-prev.marketing)::numeric(38,2)::varchar(25) as marketing,
                    (curr.admin-prev.admin)::numeric(38,2)::varchar(25) as admin,
                    (curr.opex-prev.opex)::numeric(38,2)::varchar(25) as opex,
                    (curr.op_m-prev.op_m)::numeric(38,2)::varchar(25) as op_m,
                    (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)) || '%' as opbypercent,
                    'vs PriorY ' || right(curr.fiscalyearquartercode,2) as metric_name,
                    (curr.fsc-prev.fsc)::numeric(38,2)::varchar(25) as fsc,
                    (curr.gne_operational-prev.gne_operational)::numeric(38,2)::varchar(25) as gne_operational,
                    (curr.cum_operational-prev.cum_operational)::numeric(38,2)::varchar(25) as cum_operational              
             FROM fin_insights.rev_from_curr_mon_fls curr
             INNER JOIN fin_insights.rev_from_prev_fy_act prev
             on right(curr.fiscalyearquartercode,2) = right(prev.fiscalyearquartercode,2) 
             and curr.region = prev.region and curr.programtype = prev.programtype 
        --     where upper(curr.fiscalyearquartercode) in ('FY2023Q1', 'FY2023Q2') and upper(prev.fiscalyearquartercode) in ('FY2022Q1', 'FY2022Q2')   
             
             union all 
             
             SELECT curr.fiscalyearquartercode ,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'vs PriorY ' || right(curr.fiscalyearquartercode,2) as load_type ,
                    curr.flash_month as flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    (curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)::varchar(25) as grossnewenrollments_k,
                    (curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)::varchar(25) as cumenrollees_k,
                    (curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)::varchar(25) as netrevenue_m,
                    (curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)::varchar(25) as grossmargin_m,
                    (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' as grossmarginbyprecent,
                    (curr.rd-prev.rd)::numeric(38,2)::varchar(25) as rd,
                    (curr.marketing-prev.marketing)::numeric(38,2)::varchar(25) as marketing,
                    (curr.admin-prev.admin)::numeric(38,2)::varchar(25) as admin,
                    (curr.opex-prev.opex)::numeric(38,2)::varchar(25) as opex,
                    (curr.op_m-prev.op_m)::numeric(38,2)::varchar(25) as op_m,
                    (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)) || '%' as opbypercent,
                    'vs PriorY ' || right(curr.fiscalyearquartercode,2) as metric_name,
                    (curr.fsc-prev.fsc)::numeric(38,2)::varchar(25) as fsc,
                    (curr.gne_operational-prev.gne_operational)::numeric(38,2)::varchar(25) as gne_operational,
                    (curr.cum_operational-prev.cum_operational)::numeric(38,2)::varchar(25) as cum_operational             
             FROM fin_insights.inst_svc_per_reg_curr_mon_fls curr
             INNER JOIN fin_insights.rev_from_prev_fy_act prev
             on right(curr.fiscalyearquartercode,2) = right(prev.fiscalyearquartercode,2) 
             and curr.region = prev.region and curr.programtype = prev.programtype
        --     where upper(curr.fiscalyearquartercode) in ('FY2023Q1', 'FY2023Q2') and upper(prev.fiscalyearquartercode) in ('FY2022Q1', 'FY2022Q2')                
             
             union all 
             
             SELECT curr.fiscalyearquartercode ,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'vs PriorY ' || left(prev.load_type,4) as load_type ,
                    curr.flash_month as flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    (curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)::varchar(25) as grossnewenrollments_k,
                    (curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)::varchar(25) as cumenrollees_k,
                    (curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)::varchar(25) as netrevenue_m,
                    (curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)::varchar(25) as grossmargin_m,
                    (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' as grossmarginbyprecent,
                    (curr.rd-prev.rd)::numeric(38,2)::varchar(25) as rd,
                    (curr.marketing-prev.marketing)::numeric(38,2)::varchar(25) as marketing,
                    (curr.admin-prev.admin)::numeric(38,2)::varchar(25) as admin,
                    (curr.opex-prev.opex)::numeric(38,2)::varchar(25) as opex,
                    (curr.op_m-prev.op_m)::numeric(38,2)::varchar(25) as op_m,
                    (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)) || '%' as opbypercent,
                    'vs PriorY ' || left(prev.load_type,2) as metric_name,
                    (curr.fsc-prev.fsc)::numeric(38,2)::varchar(25) as fsc,
                    (curr.gne_operational-prev.gne_operational)::numeric(38,2)::varchar(25) as gne_operational,
                    (curr.cum_operational-prev.cum_operational)::numeric(38,2)::varchar(25) as cum_operational             
             FROM fin_insights.fy_total_from_curr_mon_fls curr
             INNER JOIN fin_insights.fy_total_from_prev_year_act prev on
             curr.region = prev.region and curr.programtype = prev.programtype
             
             union all
             
             SELECT curr.fiscalyearquartercode ,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'vs PriorY ' || left(prev.load_type,4) as load_type ,
                    curr.flash_month as flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    (curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)::varchar(25) as grossnewenrollments_k,
                    (curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)::varchar(25) as cumenrollees_k,
                    (curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)::varchar(25) as netrevenue_m,
                    (curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)::varchar(25) as grossmargin_m,
                    (replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)) || '%' as grossmarginbyprecent,
                    (curr.rd-prev.rd)::numeric(38,2)::varchar(25) as rd,
                    (curr.marketing-prev.marketing)::numeric(38,2)::varchar(25) as marketing,
                    (curr.admin-prev.admin)::numeric(38,2)::varchar(25) as admin,
                    (curr.opex-prev.opex)::numeric(38,2)::varchar(25) as opex,
                    (curr.op_m-prev.op_m)::numeric(38,2)::varchar(25) as op_m,
                    (replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)) || '%' as opbypercent,
                    'vs PriorY ' || left(prev.load_type,2) as metric_name,
                    (curr.fsc-prev.fsc)::numeric(38,2)::varchar(25) as fsc,
                    (curr.gne_operational-prev.gne_operational)::numeric(38,2)::varchar(25) as gne_operational,
                    (curr.cum_operational-prev.cum_operational)::numeric(38,2)::varchar(25) as cum_operational             
             FROM fin_insights.inst_svc_per_reg_fy_curr_mon_fls curr
             INNER JOIN fin_insights.fy_total_from_prev_year_act prev on
             curr.region = prev.region and curr.programtype = prev.programtype             
             
             union all
             
             SELECT curr.fiscalyearquartercode,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'YoY % ' || left(curr.load_type,4) as load_type,
                    curr.flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    round(((curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)/nullif(prev.grossnewenrollments_k::numeric(38,2),0))*100,0) || '%'  as grossnewenrollments_k,
                    round(((curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)/nullif(prev.cumenrollees_k::numeric(38,2),0))*100,0) || '%' as cumenrollees_k,
                    round(((curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)/nullif(prev.netrevenue_m::numeric(38,2),0))*100,0) || '%' as netrevenue_m,
                    round(((curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)/nullif(prev.grossmargin_m::numeric(38,2),0))*100,0) || '%' as grossmargin_m,
                    round((replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)),0) || '%' as grossmarginbyprecent,
                    round(((curr.rd-prev.rd)::numeric(38,2)/nullif(prev.rd::numeric(38,2),0))*100,0) || '%'  as rd,
                    round(((curr.marketing-prev.marketing)::numeric(38,2)/nullif(prev.marketing::numeric(38,2),0))*100,0) || '%' as marketing,
                    round(((curr.admin-prev.admin)::numeric(38,2)/nullif(prev.admin::numeric(38,2),0))*100,0) || '%' as admin,
                    round(((curr.opex-prev.opex)::numeric(38,2)/nullif(prev.opex::numeric(38,2),0))*100,0) || '%' as opex,
                    round(((curr.op_m-prev.op_m)::numeric(38,2)/nullif(prev.op_m::numeric(38,2),0))*100,0) || '%' as op_m,
                    round((replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)),0) || '%' as opbypercent,
                    'YoY % ' || left(curr.load_type,2) as metric_name,
                    round(((curr.fsc-prev.fsc)::numeric(38,2)/nullif(prev.fsc::numeric(38,2),0))*100,0) || '%' as fsc,
                    round(((curr.gne_operational-prev.gne_operational)::numeric(38,2)/nullif(prev.gne_operational::numeric(38,2),0))*100,0) || '%'  as gne_operational,
                    round(((curr.cum_operational-prev.cum_operational)::numeric(38,2)/nullif(prev.cum_operational::numeric(38,2),0))*100,0) || '%' as cum_operational              
             FROM fin_insights.rev_from_curr_mon_fls curr
             INNER JOIN fin_insights.rev_from_prev_fy_act prev
             on right(curr.fiscalyearquartercode,2) = right(prev.fiscalyearquartercode,2) 
             and curr.region = prev.region and curr.programtype = prev.programtype
      --       where upper(curr.fiscalyearquartercode) in ('FY2023Q1', 'FY2023Q2') and upper(prev.fiscalyearquartercode) in ('FY2022Q1', 'FY2022Q2')   
             
             union all 
             
             SELECT curr.fiscalyearquartercode,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'YoY % ' || left(curr.load_type,4) as load_type,
                    curr.flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    round(((curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)/nullif(prev.grossnewenrollments_k::numeric(38,2),0))*100,0) || '%'  as grossnewenrollments_k,
                    round(((curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)/nullif(prev.cumenrollees_k::numeric(38,2),0))*100,0) || '%' as cumenrollees_k,
                    round(((curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)/nullif(prev.netrevenue_m::numeric(38,2),0))*100,0) || '%' as netrevenue_m,
                    round(((curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)/nullif(prev.grossmargin_m::numeric(38,2),0))*100,0) || '%' as grossmargin_m,
                    round((replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)),0) || '%' as grossmarginbyprecent,
                    round(((curr.rd-prev.rd)::numeric(38,2)/nullif(prev.rd::numeric(38,2),0))*100,0) || '%'  as rd,
                    round(((curr.marketing-prev.marketing)::numeric(38,2)/nullif(prev.marketing::numeric(38,2),0))*100,0) || '%' as marketing,
                    round(((curr.admin-prev.admin)::numeric(38,2)/nullif(prev.admin::numeric(38,2),0))*100,0) || '%' as admin,
                    round(((curr.opex-prev.opex)::numeric(38,2)/nullif(prev.opex::numeric(38,2),0))*100,0) || '%' as opex,
                    round(((curr.op_m-prev.op_m)::numeric(38,2)/nullif(prev.op_m::numeric(38,2),0))*100,0) || '%' as op_m,
                    round((replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)),0) || '%' as opbypercent,
                    'YoY % ' || left(curr.load_type,2) as metric_name,
                    round(((curr.fsc-prev.fsc)::numeric(38,2)/nullif(prev.fsc::numeric(38,2),0))*100,0) || '%' as fsc,
                    round(((curr.gne_operational-prev.gne_operational)::numeric(38,2)/nullif(prev.gne_operational::numeric(38,2),0))*100,0) || '%'  as gne_operational,
                    round(((curr.cum_operational-prev.cum_operational)::numeric(38,2)/nullif(prev.cum_operational::numeric(38,2),0))*100,0) || '%' as cum_operational                           
             FROM fin_insights.inst_svc_per_reg_curr_mon_fls curr
             INNER JOIN fin_insights.rev_from_prev_fy_act prev
             on right(curr.fiscalyearquartercode,2) = right(prev.fiscalyearquartercode,2) 
             and curr.region = prev.region and curr.programtype = prev.programtype
      --       where upper(curr.fiscalyearquartercode) in ('FY2023Q1', 'FY2023Q2') and upper(prev.fiscalyearquartercode) in ('FY2022Q1', 'FY2022Q2')
             
             union all
      
             SELECT curr.fiscalyearquartercode,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'YoY % ' || left(curr.load_type,4) as load_type,
                    curr.flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    round(((curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)/nullif(prev.grossnewenrollments_k::numeric(38,2),0))*100,0) || '%'  as grossnewenrollments_k,
                    round(((curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)/nullif(prev.cumenrollees_k::numeric(38,2),0))*100,0) || '%' as cumenrollees_k,
                    round(((curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)/nullif(prev.netrevenue_m::numeric(38,2),0))*100,0) || '%' as netrevenue_m,
                    round(((curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)/nullif(prev.grossmargin_m::numeric(38,2),0))*100,0) || '%' as grossmargin_m,
                    round((replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)),0) || '%' as grossmarginbyprecent,
                    round(((curr.rd-prev.rd)::numeric(38,2)/nullif(prev.rd::numeric(38,2),0))*100,0) || '%'  as rd,
                    round(((curr.marketing-prev.marketing)::numeric(38,2)/nullif(prev.marketing::numeric(38,2),0))*100,0) || '%' as marketing,
                    round(((curr.admin-prev.admin)::numeric(38,2)/nullif(prev.admin::numeric(38,2),0))*100,0) || '%' as admin,
                    round(((curr.opex-prev.opex)::numeric(38,2)/nullif(prev.opex::numeric(38,2),0))*100,0) || '%' as opex,
                    round(((curr.op_m-prev.op_m)::numeric(38,2)/nullif(prev.op_m::numeric(38,2),0))*100,0) || '%' as op_m,
                    round((replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)),0) || '%' as opbypercent,
                    'YoY % ' || left(curr.load_type,2) as metric_name,
                    round(((curr.fsc-prev.fsc)::numeric(38,2)/nullif(prev.fsc::numeric(38,2),0))*100,0) || '%' as fsc,
                    round(((curr.gne_operational-prev.gne_operational)::numeric(38,2)/nullif(prev.gne_operational::numeric(38,2),0))*100,0) || '%'  as gne_operational,
                    round(((curr.cum_operational-prev.cum_operational)::numeric(38,2)/nullif(prev.cum_operational::numeric(38,2),0))*100,0) || '%' as cum_operational                           
             FROM fin_insights.fy_total_from_curr_mon_fls curr
             INNER join fin_insights.fy_total_from_prev_year_act prev on
             curr.region = prev.region and curr.programtype = prev.programtype
      --       where upper(curr.fiscalyearquartercode) in ('FY2023Q1', 'FY2023Q2') and upper(prev.fiscalyearquartercode) in ('FY2022Q1', 'FY2022Q2')  
             
             union all
      
             SELECT curr.fiscalyearquartercode,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'YoY % ' || left(curr.load_type,4) as load_type,
                    curr.flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    round(((curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)/nullif(prev.grossnewenrollments_k::numeric(38,2),0))*100,0) || '%'  as grossnewenrollments_k,
                    round(((curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)/nullif(prev.cumenrollees_k::numeric(38,2),0))*100,0) || '%' as cumenrollees_k,
                    round(((curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)/nullif(prev.netrevenue_m::numeric(38,2),0))*100,0) || '%' as netrevenue_m,
                    round(((curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)/nullif(prev.grossmargin_m::numeric(38,2),0))*100,0) || '%' as grossmargin_m,
                    round((replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)),0) || '%' as grossmarginbyprecent,
                    round(((curr.rd-prev.rd)::numeric(38,2)/nullif(prev.rd::numeric(38,2),0))*100,0) || '%'  as rd,
                    round(((curr.marketing-prev.marketing)::numeric(38,2)/nullif(prev.marketing::numeric(38,2),0))*100,0) || '%' as marketing,
                    round(((curr.admin-prev.admin)::numeric(38,2)/nullif(prev.admin::numeric(38,2),0))*100,0) || '%' as admin,
                    round(((curr.opex-prev.opex)::numeric(38,2)/nullif(prev.opex::numeric(38,2),0))*100,0) || '%' as opex,
                    round(((curr.op_m-prev.op_m)::numeric(38,2)/nullif(prev.op_m::numeric(38,2),0))*100,0) || '%' as op_m,
                    round((replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)),0) || '%' as opbypercent,
                    'YoY % ' || left(curr.load_type,2) as metric_name,
                    round(((curr.fsc-prev.fsc)::numeric(38,2)/nullif(prev.fsc::numeric(38,2),0))*100,0) || '%' as fsc,
                    round(((curr.gne_operational-prev.gne_operational)::numeric(38,2)/nullif(prev.gne_operational::numeric(38,2),0))*100,0) || '%'  as gne_operational,
                    round(((curr.cum_operational-prev.cum_operational)::numeric(38,2)/nullif(prev.cum_operational::numeric(38,2),0))*100,0) || '%' as cum_operational                           
             FROM fin_insights.inst_svc_per_reg_fy_curr_mon_fls curr
             INNER join fin_insights.fy_total_from_prev_year_act prev on
             curr.region = prev.region and curr.programtype = prev.programtype
      --       where upper(curr.fiscalyearquartercode) in ('FY2023Q1', 'FY2023Q2') and upper(prev.fiscalyearquartercode) in ('FY2022Q1', 'FY2022Q2')                
             
             union all 
             
             SELECT curr.fiscalyearquartercode,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    '% FoF ' || left(curr.load_type,4) as load_type,
                    curr.flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    round(((curr.GrossNewEnrollments_K)::numeric(38,2)/nullif(prev.GrossNewEnrollments_K::numeric(38,2),0))*100,0) || '%'  as grossnewenrollments_k,
                    round(((curr.cumenrollees_k)::numeric(38,2)/nullif(prev.cumenrollees_k::numeric(38,2),0))*100,0) || '%' as cumenrollees_k,
                    round(((curr.netrevenue_m)::numeric(38,2)/nullif(prev.netrevenue_m::numeric(38,2),0))*100,0) || '%' as netrevenue_m,
                    round(((curr.grossmargin_m)::numeric(38,2)/nullif(prev.grossmargin_m::numeric(38,2),0))*100,0) || '%' as grossmargin_m,
                    null as grossmarginbyprecent,
                    round(((curr.rd)::numeric(38,2)/nullif(prev.rd::numeric(38,2),0))*100,0) || '%'  as rd,
                    round(((curr.marketing)::numeric(38,2)/nullif(prev.marketing::numeric(38,2),0))*100,0) || '%' as marketing,
                    round(((curr.admin)::numeric(38,2)/nullif(prev.admin::numeric(38,2),0))*100,0) || '%' as admin,
                    round(((curr.opex)::numeric(38,2)/nullif(prev.opex::numeric(38,2),0))*100,0) || '%' as opex,
                    round(((curr.op_m)::numeric(38,2)/nullif(prev.op_m::numeric(38,2),0))*100,0) || '%' as op_m,
                    null as opbypercent,
                    '% FoF ' || left(curr.load_type,2) as metric_name,
                    round(((curr.fsc)::numeric(38,2)/nullif(prev.fsc::numeric(38,2),0))*100,0) || '%' as fsc,
                    round(((curr.gne_operational)::numeric(38,2)/nullif(prev.gne_operational::numeric(38,2),0))*100,0) || '%'  as gne_operational,
                    round(((curr.cum_operational)::numeric(38,2)/nullif(prev.cum_operational::numeric(38,2),0))*100,0) || '%' as cum_operational            
             FROM fin_insights.rev_from_curr_mon_fls curr 
             INNER JOIN fin_insights.rev_from_prev_mon_flsh prev
             on curr.region = prev.region and curr.programtype = prev.programtype
             and RIGHT(curr.fiscalyearquartercode,2) = RIGHT(prev.fiscalyearquartercode,2)
     --        where upper(curr.fiscalyearquartercode) in ('FY2023Q1', 'FY2023Q2') and upper(prev.fiscalyearquartercode) in ('FY2023Q1', 'FY2023Q2')   
             
             union all 
             
             SELECT curr.fiscalyearquartercode,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    '% FoF ' || left(curr.load_type,4) as load_type,
                    curr.flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    round(((curr.GrossNewEnrollments_K)::numeric(38,2)/nullif(prev.GrossNewEnrollments_K::numeric(38,2),0))*100,0) || '%'  as grossnewenrollments_k,
                    round(((curr.cumenrollees_k)::numeric(38,2)/nullif(prev.cumenrollees_k::numeric(38,2),0))*100,0) || '%' as cumenrollees_k,
                    round(((curr.netrevenue_m)::numeric(38,2)/nullif(prev.netrevenue_m::numeric(38,2),0))*100,0) || '%' as netrevenue_m,
                    round(((curr.grossmargin_m)::numeric(38,2)/nullif(prev.grossmargin_m::numeric(38,2),0))*100,0) || '%' as grossmargin_m,
                    null as grossmarginbyprecent,
                    round(((curr.rd)::numeric(38,2)/nullif(prev.rd::numeric(38,2),0))*100,0) || '%'  as rd,
                    round(((curr.marketing)::numeric(38,2)/nullif(prev.marketing::numeric(38,2),0))*100,0) || '%' as marketing,
                    round(((curr.admin)::numeric(38,2)/nullif(prev.admin::numeric(38,2),0))*100,0) || '%' as admin,
                    round(((curr.opex)::numeric(38,2)/nullif(prev.opex::numeric(38,2),0))*100,0) || '%' as opex,
                    round(((curr.op_m)::numeric(38,2)/nullif(prev.op_m::numeric(38,2),0))*100,0) || '%' as op_m,
                    null as opbypercent,
                    '% FoF ' || left(curr.load_type,2) as metric_name,
                    round(((curr.fsc)::numeric(38,2)/nullif(prev.fsc::numeric(38,2),0))*100,0) || '%' as fsc,
                    round(((curr.gne_operational)::numeric(38,2)/nullif(prev.gne_operational::numeric(38,2),0))*100,0) || '%'  as gne_operational,
                    round(((curr.cum_operational)::numeric(38,2)/nullif(prev.cum_operational::numeric(38,2),0))*100,0) || '%' as cum_operational              
             FROM fin_insights.inst_svc_per_reg_curr_mon_fls curr 
             INNER JOIN fin_insights.inst_svc_per_reg_prev_mon_fls prev
             on curr.region = prev.region and curr.programtype = prev.programtype
             and RIGHT(curr.fiscalyearquartercode,2) = RIGHT(prev.fiscalyearquartercode,2)
       --      where upper(curr.fiscalyearquartercode) in ('FY2023Q1', 'FY2023Q2') and upper(prev.fiscalyearquartercode) in ('FY2023Q1', 'FY2023Q2')              
             
             union all 
             
             SELECT curr.fiscalyearquartercode,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    case when curr.fy_curr_q_rnk = 1 then 'QoQ % ' || left(prev.load_type,4) 
                         when curr.fy_curr_q_rnk in (2,3,4) then 'QoQ % ' || left(curr1.load_type,4) end as load_type,
                    curr.flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)/nullif(prev.grossnewenrollments_k::numeric(38,2),0))*100,0) || '%'  
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.grossnewenrollments_k-curr1.grossnewenrollments_k)::numeric(38,2)/nullif(curr1.grossnewenrollments_k::numeric(38,2),0))*100,0) || '%' end as grossnewenrollments_k,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)/nullif(prev.cumenrollees_k::numeric(38,2),0))*100,0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.cumenrollees_k-curr1.cumenrollees_k)::numeric(38,2)/nullif(curr1.cumenrollees_k::numeric(38,2),0))*100,0) || '%' end as cumenrollees_k,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)/nullif(prev.netrevenue_m::numeric(38,2),0))*100,0) || '%'
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.netrevenue_m-curr1.netrevenue_m)::numeric(38,2)/nullif(curr1.netrevenue_m::numeric(38,2),0))*100,0) || '%' end as netrevenue_m,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)/nullif(prev.grossmargin_m::numeric(38,2),0))*100,0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.grossmargin_m-curr1.grossmargin_m)::numeric(38,2)/nullif(curr1.grossmargin_m::numeric(38,2),0))*100,0) || '%'end as grossmargin_m,
                    case when curr.fy_curr_q_rnk = 1 then round((replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)),0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round((replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(curr1.grossmarginbyprecent,'%','')::numeric(38,2)),0) || '%' end as grossmarginbyprecent,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.rd-prev.rd)::numeric(38,2)/nullif(prev.rd::numeric(38,2),0))*100,0) || '%'
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.rd-curr1.rd)::numeric(38,2)/nullif(curr1.rd::numeric(38,2),0))*100,0) || '%' end as rd,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.marketing-prev.marketing)::numeric(38,2)/nullif(prev.marketing::numeric(38,2),0))*100,0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.marketing-curr1.marketing)::numeric(38,2)/nullif(curr1.marketing::numeric(38,2),0))*100,0) || '%' end as marketing,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.admin-prev.admin)::numeric(38,2)/nullif(prev.admin::numeric(38,2),0))*100,0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.admin-curr1.admin)::numeric(38,2)/nullif(curr1.admin::numeric(38,2),0))*100,0) || '%' end as admin,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.opex-prev.opex)::numeric(38,2)/nullif(prev.opex::numeric(38,2),0))*100,0) || '%'
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.opex-curr1.opex)::numeric(38,2)/nullif(curr1.opex::numeric(38,2),0))*100,0) || '%' end as opex,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.op_m-prev.op_m)::numeric(38,2)/nullif(prev.op_m::numeric(38,2),0))*100,0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.op_m-curr1.op_m)::numeric(38,2)/nullif(curr1.op_m::numeric(38,2),0))*100,0) || '%' end as op_m,
                    case when curr.fy_curr_q_rnk = 1 then round((replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)),0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round((replace(curr.opbypercent,'%','')::numeric(38,2)-replace(curr1.opbypercent,'%','')::numeric(38,2)),0) || '%' end as opbypercent,
                    case when curr.fy_curr_q_rnk = 1 then 'QoQ % ' || left(prev.load_type,2) 
                         when curr.fy_curr_q_rnk in (2,3,4) then 'QoQ % ' || left(curr1.load_type,2) end as metric_name,      
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.fsc-prev.fsc)::numeric(38,2)/nullif(prev.fsc::numeric(38,2),0))*100,0) || '%'
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.fsc-curr1.fsc)::numeric(38,2)/nullif(curr1.fsc::numeric(38,2),0))*100,0) || '%' end as fsc,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.gne_operational-prev.gne_operational)::numeric(38,2)/nullif(prev.gne_operational::numeric(38,2),0))*100,0) || '%'  
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.gne_operational-curr1.gne_operational)::numeric(38,2)/nullif(curr1.gne_operational::numeric(38,2),0))*100,0) || '%' end as gne_operational,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.cum_operational-prev.cum_operational)::numeric(38,2)/nullif(prev.cum_operational::numeric(38,2),0))*100,0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.cum_operational-curr1.cum_operational)::numeric(38,2)/nullif(curr1.cum_operational::numeric(38,2),0))*100,0) || '%' end as cum_operational                  
             FROM fin_insights.rev_from_curr_mon_fls curr
             left join fin_insights.rev_from_curr_mon_fls curr1
             on curr.fy_curr_q_rnk = curr1.fy_prev_q_rnk and curr.region = curr1.region and curr.programtype = curr1.programtype 
             and curr.fy_curr_q_rnk in (2,3,4)
             left join fin_insights.rev_from_prev_fy_act prev
             on curr.fy_curr_q_rnk = prev.prev_fy_prev_q_rnk 
             and curr.fy_curr_q_rnk = 1
             and curr.region = prev.region and curr.programtype = prev.programtype 
       --      where curr.fiscalyearquartercode in ('FY2023Q1','FY2023Q2') and prev.fiscalyearquartercode = 'FY2022Q4' 
             
             union all 
             
             SELECT curr.fiscalyearquartercode,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    case when curr.fy_curr_q_rnk = 1 then 'QoQ % ' || left(prev.load_type,4) 
                         when curr.fy_curr_q_rnk in (2,3,4) then 'QoQ % ' || left(curr1.load_type,4) end as load_type,
                    curr.flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.grossnewenrollments_k-prev.grossnewenrollments_k)::numeric(38,2)/nullif(prev.grossnewenrollments_k::numeric(38,2),0))*100,0) || '%'  
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.grossnewenrollments_k-curr1.grossnewenrollments_k)::numeric(38,2)/nullif(curr1.grossnewenrollments_k::numeric(38,2),0))*100,0) || '%' end as grossnewenrollments_k,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.cumenrollees_k-prev.cumenrollees_k)::numeric(38,2)/nullif(prev.cumenrollees_k::numeric(38,2),0))*100,0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.cumenrollees_k-curr1.cumenrollees_k)::numeric(38,2)/nullif(curr1.cumenrollees_k::numeric(38,2),0))*100,0) || '%' end as cumenrollees_k,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.netrevenue_m-prev.netrevenue_m)::numeric(38,2)/nullif(prev.netrevenue_m::numeric(38,2),0))*100,0) || '%'
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.netrevenue_m-curr1.netrevenue_m)::numeric(38,2)/nullif(curr1.netrevenue_m::numeric(38,2),0))*100,0) || '%' end as netrevenue_m,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.grossmargin_m-prev.grossmargin_m)::numeric(38,2)/nullif(prev.grossmargin_m::numeric(38,2),0))*100,0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.grossmargin_m-curr1.grossmargin_m)::numeric(38,2)/nullif(curr1.grossmargin_m::numeric(38,2),0))*100,0) || '%'end as grossmargin_m,
                    case when curr.fy_curr_q_rnk = 1 then round((replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(prev.grossmarginbyprecent,'%','')::numeric(38,2)),0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round((replace(curr.grossmarginbyprecent,'%','')::numeric(38,2)-replace(curr1.grossmarginbyprecent,'%','')::numeric(38,2)),0) || '%' end as grossmarginbyprecent,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.rd-prev.rd)::numeric(38,2)/nullif(prev.rd::numeric(38,2),0))*100,0) || '%'
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.rd-curr1.rd)::numeric(38,2)/nullif(curr1.rd::numeric(38,2),0))*100,0) || '%' end as rd,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.marketing-prev.marketing)::numeric(38,2)/nullif(prev.marketing::numeric(38,2),0))*100,0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.marketing-curr1.marketing)::numeric(38,2)/nullif(curr1.marketing::numeric(38,2),0))*100,0) || '%' end as marketing,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.admin-prev.admin)::numeric(38,2)/nullif(prev.admin::numeric(38,2),0))*100,0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.admin-curr1.admin)::numeric(38,2)/nullif(curr1.admin::numeric(38,2),0))*100,0) || '%' end as admin,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.opex-prev.opex)::numeric(38,2)/nullif(prev.opex::numeric(38,2),0))*100,0) || '%'
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.opex-curr1.opex)::numeric(38,2)/nullif(curr1.opex::numeric(38,2),0))*100,0) || '%' end as opex,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.op_m-prev.op_m)::numeric(38,2)/nullif(prev.op_m::numeric(38,2),0))*100,0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.op_m-curr1.op_m)::numeric(38,2)/nullif(curr1.op_m::numeric(38,2),0))*100,0) || '%' end as op_m,
                    case when curr.fy_curr_q_rnk = 1 then round((replace(curr.opbypercent,'%','')::numeric(38,2)-replace(prev.opbypercent,'%','')::numeric(38,2)),0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round((replace(curr.opbypercent,'%','')::numeric(38,2)-replace(curr1.opbypercent,'%','')::numeric(38,2)),0) || '%' end as opbypercent,
                    case when curr.fy_curr_q_rnk = 1 then 'QoQ % ' || left(prev.load_type,2) 
                         when curr.fy_curr_q_rnk in (2,3,4) then 'QoQ % ' || left(curr1.load_type,2) end as metric_name,                   
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.fsc-prev.fsc)::numeric(38,2)/nullif(prev.fsc::numeric(38,2),0))*100,0) || '%'
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.fsc-curr1.fsc)::numeric(38,2)/nullif(curr1.fsc::numeric(38,2),0))*100,0) || '%' end as fsc,
                            case when curr.fy_curr_q_rnk = 1 then round(((curr.gne_operational-prev.gne_operational)::numeric(38,2)/nullif(prev.gne_operational::numeric(38,2),0))*100,0) || '%'  
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.gne_operational-curr1.gne_operational)::numeric(38,2)/nullif(curr1.gne_operational::numeric(38,2),0))*100,0) || '%' end as gne_operational,
                    case when curr.fy_curr_q_rnk = 1 then round(((curr.cum_operational-prev.cum_operational)::numeric(38,2)/nullif(prev.cum_operational::numeric(38,2),0))*100,0) || '%' 
                         when curr.fy_curr_q_rnk in (2,3,4) then round(((curr.cum_operational-curr1.cum_operational)::numeric(38,2)/nullif(curr1.cum_operational::numeric(38,2),0))*100,0) || '%' end as cum_operational                            
             FROM fin_insights.inst_svc_per_reg_curr_mon_fls curr
             left join fin_insights.inst_svc_per_reg_curr_mon_fls curr1
             on curr.fy_curr_q_rnk = curr1.fy_prev_q_rnk and curr.region = curr1.region and curr.programtype = curr1.programtype 
             and curr.fy_curr_q_rnk in (2,3,4)
             left join fin_insights.rev_from_prev_fy_act prev
             on curr.fy_curr_q_rnk = prev.prev_fy_prev_q_rnk 
             and curr.fy_curr_q_rnk = 1
             and curr.region = prev.region and curr.programtype = prev.programtype 
       --      where curr.fiscalyearquartercode IN('FY2023Q1','FY2023Q2') and prev.fiscalyearquartercode IN ('FY2022Q4') and curr1.fiscalyearquartercode in ('FY2023Q1')
             
             union all 
             
             SELECT curr.fiscalyearquartercode,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'FoB % ' || left(curr.load_type,4) as load_type,
                    curr.flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    round(((curr.GrossNewEnrollments_K)::numeric(38,2)/nullif(prev.GrossNewEnrollments_K::numeric(38,2),0))*100,0) || '%'  as grossnewenrollments_k,
                    round(((curr.cumenrollees_k)::numeric(38,2)/nullif(prev.cumenrollees_k::numeric(38,2),0))*100,0) || '%' as cumenrollees_k,
                    round(((curr.netrevenue_m)::numeric(38,2)/nullif(prev.netrevenue_m::numeric(38,2),0))*100,0) || '%' as netrevenue_m,
                    round(((curr.grossmargin_m)::numeric(38,2)/nullif(prev.grossmargin_m::numeric(38,2),0))*100,0) || '%' as grossmargin_m,
                    null as grossmarginbyprecent,
                    round(((curr.rd)::numeric(38,2)/nullif(prev.rd::numeric(38,2),0))*100,0) || '%'  as rd,
                    round(((curr.marketing)::numeric(38,2)/nullif(prev.marketing::numeric(38,2),0))*100,0) || '%' as marketing,
                    round(((curr.admin)::numeric(38,2)/nullif(prev.admin::numeric(38,2),0))*100,0) || '%' as admin,
                    round(((curr.opex)::numeric(38,2)/nullif(prev.opex::numeric(38,2),0))*100,0) || '%' as opex,
                    round(((curr.op_m)::numeric(38,2)/nullif(prev.op_m::numeric(38,2),0))*100,0) || '%' as op_m,
                    null as opbypercent,
                    'FoB % ' || left(curr.load_type,2) as metric_name,
                    round(((curr.fsc)::numeric(38,2)/nullif(prev.fsc::numeric(38,2),0))*100,0) || '%' as fsc,
                    round(((curr.gne_operational)::numeric(38,2)/nullif(prev.gne_operational::numeric(38,2),0))*100,0) || '%'  as gne_operational,
                    round(((curr.cum_operational)::numeric(38,2)/nullif(prev.cum_operational::numeric(38,2),0))*100,0) || '%' as cum_operational             
             FROM fin_insights.rev_from_curr_mon_fls curr 
             INNER JOIN fin_insights.rev_from_curr_bdg prev
             on curr.region = prev.region and curr.programtype = prev.programtype
             and RIGHT(curr.fiscalyearquartercode,2) = RIGHT(prev.fiscalyearquartercode,2)
      --       where upper(curr.fiscalyearquartercode) IN ('FY2023Q1','FY2023Q2') and upper(prev.fiscalyearquartercode) IN ('FY2023Q1','FY2023Q2') 
             
             union all 
             
             SELECT curr.fiscalyearquartercode,
                    '' as max_qtr_act,
                    curr.curr_qtr,
                    '' as max_mon_num_act,
                    '' as prev_qtr,
                    '' as min_qtr_flsh, 
                    'FoB % ' || left(curr.load_type,4) as load_type,
                    curr.flash_month,
                    curr.flash_month_version,
                    curr.region,
                    curr.programtype,
                    round(((curr.GrossNewEnrollments_K)::numeric(38,2)/nullif(prev.GrossNewEnrollments_K::numeric(38,2),0))*100,0) || '%'  as grossnewenrollments_k,
                    round(((curr.cumenrollees_k)::numeric(38,2)/nullif(prev.cumenrollees_k::numeric(38,2),0))*100,0) || '%' as cumenrollees_k,
                    round(((curr.netrevenue_m)::numeric(38,2)/nullif(prev.netrevenue_m::numeric(38,2),0))*100,0) || '%' as netrevenue_m,
                    round(((curr.grossmargin_m)::numeric(38,2)/nullif(prev.grossmargin_m::numeric(38,2),0))*100,0) || '%' as grossmargin_m,
                    null as grossmarginbyprecent,
                    round(((curr.rd)::numeric(38,2)/nullif(prev.rd::numeric(38,2),0))*100,0) || '%'  as rd,
                    round(((curr.marketing)::numeric(38,2)/nullif(prev.marketing::numeric(38,2),0))*100,0) || '%' as marketing,
                    round(((curr.admin)::numeric(38,2)/nullif(prev.admin::numeric(38,2),0))*100,0) || '%' as admin,
                    round(((curr.opex)::numeric(38,2)/nullif(prev.opex::numeric(38,2),0))*100,0) || '%' as opex,
                    round(((curr.op_m)::numeric(38,2)/nullif(prev.op_m::numeric(38,2),0))*100,0) || '%' as op_m,
                    null as opbypercent,
                    'FoB % ' || left(curr.load_type,2) as metric_name,
                    round(((curr.fsc)::numeric(38,2)/nullif(prev.fsc::numeric(38,2),0))*100,0) || '%' as fsc,
                    round(((curr.gne_operational)::numeric(38,2)/nullif(prev.gne_operational::numeric(38,2),0))*100,0) || '%'  as gne_operational,
                    round(((curr.cum_operational)::numeric(38,2)/nullif(prev.cum_operational::numeric(38,2),0))*100,0) || '%' as cum_operational              
             FROM fin_insights.inst_svc_per_reg_curr_mon_fls curr 
             INNER JOIN fin_insights.rev_from_curr_bdg prev
             on curr.region = prev.region and curr.programtype = prev.programtype
             and RIGHT(curr.fiscalyearquartercode,2) = RIGHT(prev.fiscalyearquartercode,2)
        --     where upper(curr.fiscalyearquartercode) IN ('FY2023Q1','FY2023Q2') and upper(prev.fiscalyearquartercode) IN ('FY2023Q1','FY2023Q2')             
             
            ) fls
      left join fin_insights.sort_fls_metrics_pbi pbi on
      fls.metric_name = pbi.metrics_name
      and fls.flash_month = pbi.flash_month
      and fls.load_type is not null;
     
     UPDATE A
	 SET master_load_status = 1, dt_load_end_time = current_timestamp::timestamp 
	 FROM fin_insights.fls_pbi_config A
	 WHERE run_id = (select max(run_id) from fin_insights.fls_pbi_config) ;
	 
	 grant all on fin_insights.rev_from_curr_bdg to ramdassu,annamary,nairakh,patel,halapets,lingaiah;
	 grant all on fin_insights.fy_total_from_curr_bdg to ramdassu,annamary,nairakh,patel,halapets,lingaiah;
	 grant all on fin_insights.fy_total_from_curr_bdg to ramdassu,annamary,nairakh,patel,halapets,lingaiah;
	 grant all on fin_insights.rev_from_curr_mon_fls to ramdassu,annamary,nairakh,patel,halapets,lingaiah;
	 grant all on fin_insights.fy_total_from_curr_mon_fls to ramdassu,annamary,nairakh,patel,halapets,lingaiah;
	 grant all on fin_insights.inst_svc_per_reg_curr_mon_fls to ramdassu,annamary,nairakh,patel,halapets,lingaiah;
	 grant all on fin_insights.inst_svc_per_reg_curr_mon_fls to ramdassu,annamary,nairakh,patel,halapets,lingaiah;
--	 grant all on fin_insights.inst_svc_ww_curr_mon_fls to ramdassu;
	 grant all on fin_insights.inst_svc_per_reg_fy_curr_mon_fls to ramdassu,annamary,nairakh,patel,halapets,lingaiah;
--	 grant all on fin_insights.inst_svc_ww_fy_curr_mon_fls to ramdassu;
	 grant all on fin_insights.rev_from_prev_mon_flsh to ramdassu,annamary,nairakh,patel,halapets,lingaiah;
	 grant all on fin_insights.fy_total_from_prev_mon_fls to ramdassu,annamary,nairakh,patel,halapets,lingaiah;
	 grant all on fin_insights.inst_svc_per_reg_prev_mon_fls to ramdassu,annamary,nairakh,patel,halapets,lingaiah;
--	 grant all on fin_insights.inst_svc_ww_prev_mon_fls to ramdassu;
	 grant all on fin_insights.inst_svc_per_reg_fy_prev_mon_fls to ramdassu,annamary,nairakh,patel,halapets,lingaiah;
--	 grant all on fin_insights.inst_svc_ww_fy_prev_mon_fls to ramdassu;
	 grant all on fin_insights.rev_from_prev_fy_act to ramdassu,annamary,nairakh,patel,halapets,lingaiah;
--	 grant all on fin_insights.inst_svc_per_reg_prev_fy_act to ramdassu;
	 grant all on fin_insights.fy_total_from_prev_year_act to ramdassu,annamary,nairakh,patel,halapets,lingaiah;	
--	 grant all on fin_insights.inst_svc_per_reg_prev_fy_total to ramdassu;	
           
     end if;

EXCEPTION
  WHEN OTHERS THEN
    RAISE INFO 'Exception Occurred';

END;

$_$
