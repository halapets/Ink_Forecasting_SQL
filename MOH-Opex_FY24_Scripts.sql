CREATE TABLE fin_insights.opex_summ_fy24 
(
    fiscalyearquartercode character varying(8) ENCODE lzo
   ,load_type character varying(25) ENCODE lzo
   ,flash_month character varying(3) ENCODE lzo
   ,flash_month_version character varying(25) ENCODE lzo
   ,region character varying(10) ENCODE lzo
   ,program_type character varying(25) ENCODE lzo
   ,overhead                 numeric(38,2) ENCODE az64
   ,ptc_oh                   numeric(38,2) ENCODE az64
   ,phs_oh                   numeric(38,2) ENCODE az64
   ,office_print_oh          numeric(38,2) ENCODE az64
   ,print_category_oh        numeric(38,2) ENCODE az64
   ,personal_systems_oh      numeric(38,2) ENCODE az64
   ,commercial_org_oh        numeric(38,2) ENCODE az64
   ,sbo_oh                   numeric(38,2) ENCODE az64
   ,rd                       numeric(38,2) ENCODE az64
   ,ptc_rd                   numeric(38,2) ENCODE az64
   ,phs_rd                   numeric(38,2) ENCODE az64
   ,office_print_rd          numeric(38,2) ENCODE az64
   ,print_category_rd        numeric(38,2) ENCODE az64
   ,personal_systems_rd      numeric(38,2) ENCODE az64
   ,commercial_org_rd        numeric(38,2) ENCODE az64
   ,marketing                numeric(38,2) ENCODE az64
   ,cmp_marketing            numeric(38,2) ENCODE az64
   ,ptc_mkt                  numeric(38,2) ENCODE az64
   ,print_category_mkt       numeric(38,2) ENCODE az64
   ,commercial_org_mkt       numeric(38,2) ENCODE az64
   ,sbo_mkt                  numeric(38,2) ENCODE az64
   ,wfss                     numeric(38,2) ENCODE az64
   ,admin                    numeric(38,2) ENCODE az64
   ,credit_cards_fees        numeric(38,2) ENCODE az64
   ,print_category_adm       numeric(38,2) ENCODE az64
   ,print_staff              numeric(38,2) ENCODE az64
   ,print_strategy_transform numeric(38,2) ENCODE az64
   ,operations_adm           numeric(38,2) ENCODE az64
   ,other_commercial         numeric(38,2) ENCODE az64
   ,fsc                      numeric(38,2) ENCODE az64
   ,ptc_fsc                  numeric(38,2) ENCODE az64
   ,print_category_fsc       numeric(38,2) ENCODE az64
   ,pch_placeholder          numeric(38,2) ENCODE az64
   ,commercial_org_fsc       numeric(38,2) ENCODE az64
   ,operations_fsc           numeric(38,2) ENCODE az64
   ,total_opex               numeric(38,2) ENCODE az64
   ,total_spend              numeric(38,2) ENCODE az64
   ,ptc_spend                numeric(38,2) ENCODE az64
   ,opex_rank                integer ENCODE az64
   ,load_date timestamp without time zone DEFAULT ('now'::character varying)::timestamp with time zone ENCODE az64
   ,is_active integer ENCODE az64
   ,flash_period character varying(25) ENCODE lzo
)
DISTSTYLE AUTO;

grant select on fin_insights.opex_summ_fy24  to srv_power_bi_fin;
grant execute on procedure fin_insights.load_moh_opex_data_pbi_fy24() to auto_prdii;

CREATE OR REPLACE PROCEDURE fin_insights.load_moh_opex_data_pbi_fy24()
 LANGUAGE plpgsql
AS $$

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
	
	   get_master_load_flag := 'select master_load_flag from fin_insights.opex_fls_pbi_config order by run_id desc limit 1'; 
	   EXECUTE get_master_load_flag INTO mst_load_flag ;
	  
	   if mst_load_flag = 1 then
	  
	   call fin_insights.load_moh_opex_curr_fls_data_fy24();
	   call fin_insights.load_moh_opex_prev_fls_data_fy24();  
   --  call fin_insights.load_moh_opex_bdg_data();
   --  call fin_insights.load_moh_opex_act_data();
	   
	   end if ;
      
	  get_curr_fls_mon := 'select curr_fls_mon from fin_insights.opex_fls_pbi_config order by run_id desc limit 1';
	  get_prev_fls_mon :=  'select prev_fls_mon from fin_insights.opex_fls_pbi_config order by run_id desc limit 1';
	  get_curr_fls_mon_fiscal_year := 'select curr_fls_mon_fiscal_year from fin_insights.opex_fls_pbi_config order by run_id desc limit 1'; 
	  get_prev_fls_mon_fiscal_year := 'select prev_fls_mon_fiscal_year from fin_insights.opex_fls_pbi_config order by run_id desc limit 1';   
	  get_curr_fls_mon_dt_load_status := 'select curr_fls_mon_dt_load_status from fin_insights.opex_fls_pbi_config order by run_id desc limit 1'; 
  	  get_prev_fls_mon_dt_load_status := 'select prev_fls_mon_dt_load_status from fin_insights.opex_fls_pbi_config order by run_id desc limit 1'; 
 	  get_curr_bdg_dt_load_status := 'select curr_bdg_dt_load_status from fin_insights.opex_fls_pbi_config order by run_id desc limit 1'; 
 	  get_prev_act_fy_dt_load_status := 'select prev_act_fy_dt_load_status from fin_insights.opex_fls_pbi_config order by run_id desc limit 1'; 	  
 	  
 	  EXECUTE get_curr_fls_mon INTO curr_fls_mon ;
	  EXECUTE get_prev_fls_mon INTO prev_fls_mon ;
	  EXECUTE get_curr_fls_mon_fiscal_year INTO curr_fls_mon_fiscal_year ;
	  EXECUTE get_prev_fls_mon_fiscal_year INTO prev_fls_mon_fiscal_year ;
	  EXECUTE get_curr_fls_mon_dt_load_status INTO curr_fls_mon_dt_load_status ;
	  EXECUTE get_prev_fls_mon_dt_load_status INTO prev_fls_mon_dt_load_status ;
	  EXECUTE get_curr_bdg_dt_load_status INTO curr_bdg_dt_load_status ;
	  EXECUTE get_prev_act_fy_dt_load_status INTO prev_act_fy_dt_load_status ;	  
	  
	  if (curr_fls_mon_dt_load_status = 1 and prev_fls_mon_dt_load_status = 1) THEN --and curr_bdg_dt_load_status = 1 and prev_act_fy_dt_load_status = 1) 
	  
	  DELETE FROM fin_insights.opex_summ_fy24 where flash_period = (select flash_period from fin_insights.opex_fls_pbi_config order by run_id desc limit 1);
	 
	  UPDATE fin_insights.opex_summ_fy24
      SET is_active = 0;

      drop table if exists fin_insights.curr_mon_flsh_opex;
      create table fin_insights.curr_mon_flsh_opex as
      select 
             fls.fiscalyearquartercode
             ,(select distinct fiscal_year_quarter_code from fin_insights.vw_dim_month where calendar_year_month_day = to_date(date_trunc('month', dateadd(month,-1,current_date)),'yyyy-mm-dd')) as max_qtr_act
             ,(select distinct fiscal_year_quarter_code from fin_insights.vw_dim_month where calendar_year_month_day = to_date(date_trunc('month', current_date),'yyyy-mm-dd')) as curr_qtr
             ,(select mon_num_by_fyq from fin_insights.vw_dim_month where calendar_year_month_day = to_date(date_trunc('month', dateadd(month,-1,current_date)),'yyyy-mm-dd'))::char(1) as max_mon_num_act
             ,case when right(curr_qtr,2) in ('Q2','Q3','Q4') then left(curr_qtr,7)||right(curr_qtr,1)-1 else 'FY'|| substring(curr_qtr,3,4)-1 || 'Q4' end as prev_qtr
             ,case when max_qtr_act = curr_qtr then curr_qtr
                   when max_mon_num_act = 1 then curr_qtr 
              else prev_qtr end as min_qtr_flsh
             ,CASE WHEN fls.fiscalyearquartercode >= min_qtr_flsh THEN RIGHT(fls.fiscalyearquartercode,2) || substring(fls.fiscalyearquartercode,5,2) || ' ' || flash_month || ' Flash'  
              else RIGHT(fls.fiscalyearquartercode,2) || substring(fls.fiscalyearquartercode,5,2) || flash_month || ' Act' END AS load_type
             ,flash_month as flash_month
             ,'current' as flash_month_version
             ,region
             ,programtype
			 ,overhead                 
			 ,ptc_oh                   
			 ,phs_oh                   
			 ,office_print_oh          
			 ,print_category_oh        
			 ,personal_systems_oh      
			 ,commercial_org_oh        
			 ,sbo_oh                   
			 ,rd                       
			 ,ptc_rd                   
			 ,phs_rd                   
			 ,office_print_rd          
			 ,print_category_rd        
			 ,personal_systems_rd      
			 ,commercial_org_rd        
			 ,marketing                
			 ,cmp_marketing            
			 ,ptc_mkt                  
			 ,print_category_mkt       
			 ,commercial_org_mkt       
			 ,sbo_mkt                  
			 ,wfss                     
			 ,admin                    
			 ,credit_cards_fees        
			 ,print_category_adm       
			 ,print_staff              
			 ,print_strategy_transform 
			 ,operations_adm           
			 ,other_commercial         
			 ,fsc                      
			 ,ptc_fsc                  
			 ,print_category_fsc       
			 ,pch_placeholder          
			 ,commercial_org_fsc       
			 ,operations_fsc           
			 ,total_opex               
			 ,total_spend              
			 ,ptc_spend      
      from fin_insights.opex_flash_summary_fy24 fls
      join (select * from fin_insights.opex_fls_pbi_config order by run_id desc limit 1) pbi on
      fls.flash_period = pbi.flash_period ;


      drop table if exists fin_insights.fy_total_from_curr_mon_fls_opex ;
      create table fin_insights.fy_total_from_curr_mon_fls_opex as 
      select 
             'ALL' as fiscalyearquartercode 
             ,'' as max_qtr_act
             ,'' as curr_qtr
             ,'' as max_mon_num_act
             ,'' as prev_qtr
             ,'' as min_qtr_flash
             ,'FY'||substring(max(fiscalyearquartercode),5,2) || ' ' || flash_month || ' Flash' as load_type 
             ,flash_month
             ,flash_month_version
             ,region
             ,programtype
             ,SUM(overhead) AS overhead                 
             ,SUM(ptc_oh) AS ptc_oh                   
             ,SUM(phs_oh) AS phs_oh                   
             ,SUM(office_print_oh) AS office_print_oh          
             ,SUM(print_category_oh) AS print_category_oh        
             ,SUM(personal_systems_oh) AS personal_systems_oh      
             ,SUM(commercial_org_oh) AS commercial_org_oh        
             ,SUM(sbo_oh) AS sbo_oh                   
             ,SUM(rd) AS rd                       
             ,SUM(ptc_rd) AS ptc_rd                   
             ,SUM(phs_rd) AS phs_rd                   
             ,SUM(office_print_rd) AS office_print_rd          
             ,SUM(print_category_rd) AS print_category_rd        
             ,SUM(personal_systems_rd) AS personal_systems_rd      
             ,SUM(commercial_org_rd) AS commercial_org_rd        
             ,SUM(marketing) AS marketing                
             ,SUM(cmp_marketing) AS cmp_marketing            
             ,SUM(ptc_mkt) AS ptc_mkt                  
             ,SUM(print_category_mkt) AS print_category_mkt       
             ,SUM(commercial_org_mkt) AS commercial_org_mkt       
             ,SUM(sbo_mkt) AS sbo_mkt                  
             ,SUM(wfss) AS wfss                     
             ,SUM(admin) AS admin                    
             ,SUM(credit_cards_fees) AS credit_cards_fees        
             ,SUM(print_category_adm) AS print_category_adm       
             ,SUM(print_staff) AS print_staff              
             ,SUM(print_strategy_transform) AS print_strategy_transform 
             ,SUM(operations_adm) AS operations_adm           
             ,SUM(other_commercial) AS other_commercial         
             ,SUM(fsc) AS fsc                      
             ,SUM(ptc_fsc) AS ptc_fsc                  
             ,SUM(print_category_fsc) AS print_category_fsc       
             ,SUM(pch_placeholder) AS pch_placeholder          
             ,SUM(commercial_org_fsc) AS commercial_org_fsc       
             ,SUM(operations_fsc) AS operations_fsc           
             ,SUM(total_opex) AS total_opex               
             ,SUM(total_spend) AS total_spend              
             ,SUM(ptc_spend) AS ptc_spend                
      from fin_insights.curr_mon_flsh_opex
      group by flash_month,flash_month_version,region,programtype ;

      drop table if exists fin_insights.prev_mon_flsh_opex ;
      create table fin_insights.prev_mon_flsh_opex as
      select 
             fls.fiscalyearquartercode
             ,(select distinct fiscal_year_quarter_code from fin_insights.vw_dim_month where calendar_year_month_day = to_date(date_trunc('month', dateadd(month,-2,current_date)),'yyyy-mm-dd')) as max_qtr_act
             ,(select distinct fiscal_year_quarter_code from fin_insights.vw_dim_month where calendar_year_month_day = to_date(date_trunc('month', dateadd(month,-1,current_date)),'yyyy-mm-dd')) as curr_qtr
             ,(select mon_num_by_fyq from fin_insights.vw_dim_month where calendar_year_month_day = to_date(date_trunc('month', dateadd(month,-2,current_date)),'yyyy-mm-dd'))::char(1) as max_mon_num_act
             -- ,(select distinct fiscal_year_quarter_code from fin_insights.vw_dim_month where calendar_year_month_day = to_date(date_trunc('month', dateadd(month,-4,current_date)),'yyyy-mm-dd')) as prev_qtr
             , case when right(curr_qtr,2) in ('Q2','Q3','Q4') then left(curr_qtr,7)||right(curr_qtr,1)-1 else 'FY'|| substring(curr_qtr,3,4)-1 || 'Q4' end as prev_qtr
             ,case when max_qtr_act = curr_qtr then curr_qtr
                   when max_mon_num_act = 1 then curr_qtr 
              else prev_qtr end as min_qtr_flsh
             ,CASE WHEN fls.fiscalyearquartercode >= min_qtr_flsh THEN RIGHT(fls.fiscalyearquartercode,2) || substring(fls.fiscalyearquartercode,5,2) || ' ' || flash_month || ' Flash'  
              else RIGHT(fls.fiscalyearquartercode,2) || substring(fls.fiscalyearquartercode,5,2) || flash_month || ' Act' END AS load_type
             ,pbi.curr_fls_mon as flash_month
             ,'previous' as flash_month_version
             ,region
             ,programtype
			 ,overhead                 
			 ,ptc_oh                   
			 ,phs_oh                   
			 ,office_print_oh          
			 ,print_category_oh        
			 ,personal_systems_oh      
			 ,commercial_org_oh        
			 ,sbo_oh                   
			 ,rd                       
			 ,ptc_rd                   
			 ,phs_rd                   
			 ,office_print_rd          
			 ,print_category_rd        
			 ,personal_systems_rd      
			 ,commercial_org_rd        
			 ,marketing                
			 ,cmp_marketing            
			 ,ptc_mkt                  
			 ,print_category_mkt       
			 ,commercial_org_mkt       
			 ,sbo_mkt                  
			 ,wfss                     
			 ,admin                    
			 ,credit_cards_fees        
			 ,print_category_adm       
			 ,print_staff              
			 ,print_strategy_transform 
			 ,operations_adm           
			 ,other_commercial         
			 ,fsc                      
			 ,ptc_fsc                  
			 ,print_category_fsc       
			 ,pch_placeholder          
			 ,commercial_org_fsc       
			 ,operations_fsc           
			 ,total_opex               
			 ,total_spend              
			 ,ptc_spend             
      from fin_insights.opex_flash_summary_fy24 fls
      join (select * from fin_insights.opex_fls_pbi_config order by run_id desc limit 1) pbi on
      fls.flash_month = pbi.prev_fls_mon
      and fls.flash_period = pbi.prev_fls_mon|| right(prev_fls_mon_fiscal_year,2) ;
							
      drop table if exists fin_insights.fy_total_from_prev_mon_fls_opex ;
      create table fin_insights.fy_total_from_prev_mon_fls_opex as 
      select 
             'ALL' as fiscalyearquartercode 
             ,'' as max_qtr_act
             ,'' as curr_qtr
             ,'' as max_mon_num_act
             ,'' as prev_qtr
             ,'' as min_qtr_flash 
             ,'FY'||substring(max(fiscalyearquartercode),5,2) || ' ' || flash_month || ' Flash' as load_type
             ,flash_month
             ,flash_month_version
             ,region
             ,programtype
             ,SUM(overhead) AS overhead                 
             ,SUM(ptc_oh) AS ptc_oh                   
             ,SUM(phs_oh) AS phs_oh                   
             ,SUM(office_print_oh) AS office_print_oh          
             ,SUM(print_category_oh) AS print_category_oh        
             ,SUM(personal_systems_oh) AS personal_systems_oh      
             ,SUM(commercial_org_oh) AS commercial_org_oh        
             ,SUM(sbo_oh) AS sbo_oh                   
             ,SUM(rd) AS rd                       
             ,SUM(ptc_rd) AS ptc_rd                   
             ,SUM(phs_rd) AS phs_rd                   
             ,SUM(office_print_rd) AS office_print_rd          
             ,SUM(print_category_rd) AS print_category_rd        
             ,SUM(personal_systems_rd) AS personal_systems_rd      
             ,SUM(commercial_org_rd) AS commercial_org_rd        
             ,SUM(marketing) AS marketing                
             ,SUM(cmp_marketing) AS cmp_marketing            
             ,SUM(ptc_mkt) AS ptc_mkt                  
             ,SUM(print_category_mkt) AS print_category_mkt       
             ,SUM(commercial_org_mkt) AS commercial_org_mkt       
             ,SUM(sbo_mkt) AS sbo_mkt                  
             ,SUM(wfss) AS wfss                     
             ,SUM(admin) AS admin                    
             ,SUM(credit_cards_fees) AS credit_cards_fees        
             ,SUM(print_category_adm) AS print_category_adm       
             ,SUM(print_staff) AS print_staff              
             ,SUM(print_strategy_transform) AS print_strategy_transform 
             ,SUM(operations_adm) AS operations_adm           
             ,SUM(other_commercial) AS other_commercial         
             ,SUM(fsc) AS fsc                      
             ,SUM(ptc_fsc) AS ptc_fsc                  
             ,SUM(print_category_fsc) AS print_category_fsc       
             ,SUM(pch_placeholder) AS pch_placeholder          
             ,SUM(commercial_org_fsc) AS commercial_org_fsc       
             ,SUM(operations_fsc) AS operations_fsc           
             ,SUM(total_opex) AS total_opex               
             ,SUM(total_spend) AS total_spend              
             ,SUM(ptc_spend) AS ptc_spend                         
      from fin_insights.prev_mon_flsh_opex
      group by flash_month,flash_month_version,region,programtype ;	

  /*    drop table if exists fin_insights.opex_from_prev_fy_act ;
      create table fin_insights.opex_from_prev_fy_act as
      select
             act.fiscalyearquartercode
             ,'' as max_qtr_act
             ,'' as curr_qtr
             ,'' as max_mon_num_act
             ,'' prev_qtr
             ,'' min_qtr_flsh
             ,right(act.fiscalyearquartercode,2) || right(pbi.curr_fls_mon_fiscal_year,2)-1 || ' Act' as load_type
             ,pbi.curr_fls_mon as flash_month
             ,'current' as flash_month_version
             ,region
             ,programtype
			 ,overhead                 
			 ,ptc_oh                   
			 ,phs_oh                   
			 ,office_print_oh          
			 ,print_category_oh        
			 ,personal_systems_oh      
			 ,commercial_org_oh        
			 ,sbo_oh                   
			 ,rd                       
			 ,ptc_rd                   
			 ,phs_rd                   
			 ,office_print_rd          
			 ,print_category_rd        
			 ,personal_systems_rd      
			 ,commercial_org_rd        
			 ,marketing                
			 ,cmp_marketing            
			 ,ptc_mkt                  
			 ,print_category_mkt       
			 ,commercial_org_mkt       
			 ,sbo_mkt                  
			 ,wfss                     
			 ,admin                    
			 ,credit_cards_fees        
			 ,print_category_adm       
			 ,print_staff              
			 ,print_strategy_transform 
			 ,operations_adm           
			 ,other_commercial         
			 ,fsc                      
			 ,ptc_fsc                  
			 ,print_category_fsc       
			 ,pch_placeholder          
			 ,commercial_org_fsc       
			 ,operations_fsc           
			 ,total_opex               
			 ,total_spend              
			 ,ptc_spend  
      from fin_insights.opex_act_summary act
      inner join (select * from fin_insights.opex_fls_pbi_config order by run_id desc limit 1) pbi on 
      act.actuals_period = 'FY'|| RIGHT(pbi.curr_fls_mon_fiscal_year,2)-1 ;

      drop table if exists fin_insights.fy_total_from_prev_year_act_opex ;
      create table fin_insights.fy_total_from_prev_year_act_opex as
      select 
             'ALL'as fiscalyearquartercode
             ,'' as max_qtr_act
             ,'' as curr_qtr
             ,'' as max_mon_num_act
             ,'' as prev_qtr
             ,'' as min_qtr_flash 
             ,'FY'||substring(max(fiscalyearquartercode),5,2) || ' Act' as load_type
             ,flash_month
             ,flash_month_version
             ,region
             ,programtype
             ,SUM(overhead) AS overhead                 
             ,SUM(ptc_oh) AS ptc_oh                   
             ,SUM(phs_oh) AS phs_oh                   
             ,SUM(office_print_oh) AS office_print_oh          
             ,SUM(print_category_oh) AS print_category_oh        
             ,SUM(personal_systems_oh) AS personal_systems_oh      
             ,SUM(commercial_org_oh) AS commercial_org_oh        
             ,SUM(sbo_oh) AS sbo_oh                   
             ,SUM(rd) AS rd                       
             ,SUM(ptc_rd) AS ptc_rd                   
             ,SUM(phs_rd) AS phs_rd                   
             ,SUM(office_print_rd) AS office_print_rd          
             ,SUM(print_category_rd) AS print_category_rd        
             ,SUM(personal_systems_rd) AS personal_systems_rd      
             ,SUM(commercial_org_rd) AS commercial_org_rd        
             ,SUM(marketing) AS marketing                
             ,SUM(cmp_marketing) AS cmp_marketing            
             ,SUM(ptc_mkt) AS ptc_mkt                  
             ,SUM(print_category_mkt) AS print_category_mkt       
             ,SUM(commercial_org_mkt) AS commercial_org_mkt       
             ,SUM(sbo_mkt) AS sbo_mkt                  
             ,SUM(wfss) AS wfss                     
             ,SUM(admin) AS admin                    
             ,SUM(credit_cards_fees) AS credit_cards_fees        
             ,SUM(print_category_adm) AS print_category_adm       
             ,SUM(print_staff) AS print_staff              
             ,SUM(print_strategy_transform) AS print_strategy_transform 
             ,SUM(operations_adm) AS operations_adm           
             ,SUM(other_commercial) AS other_commercial         
             ,SUM(fsc) AS fsc                      
             ,SUM(ptc_fsc) AS ptc_fsc                  
             ,SUM(print_category_fsc) AS print_category_fsc       
             ,SUM(pch_placeholder) AS pch_placeholder          
             ,SUM(commercial_org_fsc) AS commercial_org_fsc       
             ,SUM(operations_fsc) AS operations_fsc           
             ,SUM(total_opex) AS total_opex               
             ,SUM(total_spend) AS total_spend              
             ,SUM(ptc_spend) AS ptc_spend  
      from fin_insights.opex_from_prev_fy_act 
      group by flash_month,flash_month_version,region,programtype ;
      
 */     

	insert into fin_insights.opex_summ_fy24
    select fiscalyearquartercode
          ,load_type
          ,flash_month
          ,flash_month_version
          ,region
          ,programtype
		  ,overhead                 
		  ,ptc_oh                   
		  ,phs_oh                   
		  ,office_print_oh          
		  ,print_category_oh        
		  ,personal_systems_oh      
		  ,commercial_org_oh        
		  ,sbo_oh                   
		  ,rd                       
		  ,ptc_rd                   
		  ,phs_rd                   
		  ,office_print_rd          
		  ,print_category_rd        
		  ,personal_systems_rd      
		  ,commercial_org_rd        
		  ,marketing                
		  ,cmp_marketing            
		  ,ptc_mkt                  
		  ,print_category_mkt       
		  ,commercial_org_mkt       
		  ,sbo_mkt                  
		  ,wfss                     
		  ,admin                    
		  ,credit_cards_fees        
		  ,print_category_adm       
		  ,print_staff              
		  ,print_strategy_transform 
		  ,operations_adm           
		  ,other_commercial         
		  ,fsc                      
		  ,ptc_fsc                  
		  ,print_category_fsc       
		  ,pch_placeholder          
		  ,commercial_org_fsc       
		  ,operations_fsc           
		  ,total_opex               
		  ,total_spend              
		  ,ptc_spend 
          ,case when flash_month_version = 'previous' and left(load_type,2) = 'Q1' then 1
                when flash_month_version = 'previous' and left(load_type,2) = 'Q2' then 2
                when flash_month_version = 'previous' and left(load_type,2) = 'Q3' then 3
                when flash_month_version = 'previous' and left(load_type,2) = 'Q4' then 4          
                when flash_month_version = 'current' and left(load_type,2) = 'Q1' then 5
                when flash_month_version = 'current' and left(load_type,2) = 'Q2' then 6
                when flash_month_version = 'current' and left(load_type,2) = 'Q3' then 7
                when flash_month_version = 'current' and left(load_type,2) = 'Q4' then 8
                when flash_month_version = 'current' and load_type Like 'Delta FoF Q1%' then 9
                when flash_month_version = 'current' and load_type Like 'Delta FoF Q2%' then 10
                when flash_month_version = 'current' and load_type Like 'Delta FoF Q3%' then 11
                when flash_month_version = 'current' and load_type Like 'Delta FoF Q4%' then 12
                when flash_month_version = 'current' and left(load_type,2) = 'FY' and right(load_type,3) = 'Act' then 13
                when flash_month_version = 'current' and left(load_type,2) = 'FY' and right(load_type,5) = 'Flash' then 14
                when flash_month_version = 'current' and load_type Like 'YoY%' then 15 else 16 end as opex_rank
          ,date(current_timestamp)::timestamp as load_date
          ,1 as is_active
          ,(select flash_period from fin_insights.opex_fls_pbi_config order by run_id desc limit 1) as flash_period           
    from (
           select * from fin_insights.curr_mon_flsh_opex

           union all
           
           select * from fin_insights.fy_total_from_curr_mon_fls_opex
           
           union all
            
           select * from fin_insights.prev_mon_flsh_opex
           
      /*   union all 
           
           select * from fin_insights.opex_from_prev_fy_act
           
           union all
           
           select * from fin_insights.fy_total_from_prev_year_act_opex 
           
           union all 

           select 
           'ALL' as fiscalyearquartercode
          ,'' as max_qtr_act
          ,curr.curr_qtr
          ,'' as max_mon_num_act
          ,'' as prev_qtr
          ,'' as min_qtr_flsh
          ,'YoY ' || left(prev.load_type,4) as load_type
          ,curr.flash_month
          ,curr.flash_month_version
          ,curr.region
          ,curr.programtype
          ,ISNULL(curr.overhead,0) - ISNULL(prev.overhead,0) AS overhead                 
          ,ISNULL(curr.ptc_oh,0) - ISNULL(prev.ptc_oh,0) AS ptc_oh                   
          ,ISNULL(curr.phs_oh,0) - ISNULL(prev.phs_oh,0) AS phs_oh                   
          ,ISNULL(curr.office_print_oh,0) - ISNULL(prev.office_print_oh,0) AS office_print_oh          
          ,ISNULL(curr.print_category_oh,0) - ISNULL(prev.print_category_oh,0) AS print_category_oh        
          ,ISNULL(curr.personal_systems_oh,0) - ISNULL(prev.personal_systems_oh,0) AS personal_systems_oh      
          ,ISNULL(curr.commercial_org_oh,0) - ISNULL(prev.commercial_org_oh,0) AS commercial_org_oh        
          ,ISNULL(curr.sbo_oh,0) - ISNULL(prev.sbo_oh,0) AS sbo_oh                   
          ,ISNULL(curr.rd,0) - ISNULL(prev.rd,0) AS rd                       
          ,ISNULL(curr.ptc_rd,0) - ISNULL(prev.ptc_rd,0) AS ptc_rd                   
          ,ISNULL(curr.phs_rd,0) - ISNULL(prev.phs_rd,0) AS phs_rd                   
          ,ISNULL(curr.office_print_rd,0) - ISNULL(prev.office_print_rd,0) AS office_print_rd          
          ,ISNULL(curr.print_category_rd,0) - ISNULL(prev.print_category_rd,0) AS print_category_rd        
          ,ISNULL(curr.personal_systems_rd,0) - ISNULL(prev.personal_systems_rd,0) AS personal_systems_rd      
          ,ISNULL(curr.commercial_org_rd,0) - ISNULL(prev.commercial_org_rd,0) AS commercial_org_rd        
          ,ISNULL(curr.marketing,0) - ISNULL(prev.marketing,0) AS marketing                
          ,ISNULL(curr.cmp_marketing,0) - ISNULL(prev.cmp_marketing,0) AS cmp_marketing            
          ,ISNULL(curr.ptc_mkt,0) - ISNULL(prev.ptc_mkt,0) AS ptc_mkt                  
          ,ISNULL(curr.print_category_mkt,0) - ISNULL(prev.print_category_mkt,0) AS print_category_mkt       
          ,ISNULL(curr.commercial_org_mkt,0) - ISNULL(prev.commercial_org_mkt,0) AS commercial_org_mkt       
          ,ISNULL(curr.sbo_mkt,0) - ISNULL(prev.sbo_mkt,0) AS sbo_mkt                  
          ,ISNULL(curr.wfss,0) - ISNULL(prev.wfss,0) AS wfss                     
          ,ISNULL(curr.admin,0) - ISNULL(prev.admin,0) AS admin                    
          ,ISNULL(curr.credit_cards_fees,0) - ISNULL(prev.credit_cards_fees,0) AS credit_cards_fees        
          ,ISNULL(curr.print_category_adm,0) - ISNULL(prev.print_category_adm,0) AS print_category_adm       
          ,ISNULL(curr.print_staff,0) - ISNULL(prev.print_staff,0) AS print_staff              
          ,ISNULL(curr.print_strategy_transform,0) - ISNULL(prev.print_strategy_transform,0) AS print_strategy_transform 
          ,ISNULL(curr.operations_adm,0) - ISNULL(prev.operations_adm,0) AS operations_adm           
          ,ISNULL(curr.other_commercial,0) - ISNULL(prev.other_commercial,0) AS other_commercial         
          ,ISNULL(curr.fsc,0) - ISNULL(prev.fsc,0) AS fsc                      
          ,ISNULL(curr.ptc_fsc,0) - ISNULL(prev.ptc_fsc,0) AS ptc_fsc                  
          ,ISNULL(curr.print_category_fsc,0) - ISNULL(prev.print_category_fsc,0) AS print_category_fsc       
          ,ISNULL(curr.pch_placeholder,0) - ISNULL(prev.pch_placeholder,0) AS pch_placeholder          
          ,ISNULL(curr.commercial_org_fsc,0) - ISNULL(prev.commercial_org_fsc,0) AS commercial_org_fsc       
          ,ISNULL(curr.operations_fsc,0) - ISNULL(prev.operations_fsc,0) AS operations_fsc           
          ,ISNULL(curr.total_opex,0) - ISNULL(prev.total_opex,0) AS total_opex               
          ,ISNULL(curr.total_spend,0) - ISNULL(prev.total_spend,0) AS total_spend              
          ,ISNULL(curr.ptc_spend,0) - ISNULL(prev.ptc_spend,0) AS ptc_spend
           from fin_insights.fy_total_from_curr_mon_fls_opex curr
           inner join fin_insights.fy_total_from_prev_year_act_opex prev 
           on curr.region = prev.region and curr.programtype = prev.programtype 
     
   */              
		   union all 
           
           select 
           curr.fiscalyearquartercode
          ,'' as max_qtr_act
          ,curr.curr_qtr
          ,'' as max_mon_num_act
          ,'' as prev_qtr
          ,'' as min_qtr_flsh
          ,'Delta FoF ' || right(curr.fiscalyearquartercode,2) || substring(curr.fiscalyearquartercode,5,2) as load_type
          ,curr.flash_month
          ,curr.flash_month_version
          ,curr.region
          ,curr.programtype
          ,ISNULL(curr.overhead,0) - ISNULL(prev.overhead,0) AS overhead                 
          ,ISNULL(curr.ptc_oh,0) - ISNULL(prev.ptc_oh,0) AS ptc_oh                   
          ,ISNULL(curr.phs_oh,0) - ISNULL(prev.phs_oh,0) AS phs_oh                   
          ,ISNULL(curr.office_print_oh,0) - ISNULL(prev.office_print_oh,0) AS office_print_oh          
          ,ISNULL(curr.print_category_oh,0) - ISNULL(prev.print_category_oh,0) AS print_category_oh        
          ,ISNULL(curr.personal_systems_oh,0) - ISNULL(prev.personal_systems_oh,0) AS personal_systems_oh      
          ,ISNULL(curr.commercial_org_oh,0) - ISNULL(prev.commercial_org_oh,0) AS commercial_org_oh        
          ,ISNULL(curr.sbo_oh,0) - ISNULL(prev.sbo_oh,0) AS sbo_oh                   
          ,ISNULL(curr.rd,0) - ISNULL(prev.rd,0) AS rd                       
          ,ISNULL(curr.ptc_rd,0) - ISNULL(prev.ptc_rd,0) AS ptc_rd                   
          ,ISNULL(curr.phs_rd,0) - ISNULL(prev.phs_rd,0) AS phs_rd                   
          ,ISNULL(curr.office_print_rd,0) - ISNULL(prev.office_print_rd,0) AS office_print_rd          
          ,ISNULL(curr.print_category_rd,0) - ISNULL(prev.print_category_rd,0) AS print_category_rd        
          ,ISNULL(curr.personal_systems_rd,0) - ISNULL(prev.personal_systems_rd,0) AS personal_systems_rd      
          ,ISNULL(curr.commercial_org_rd,0) - ISNULL(prev.commercial_org_rd,0) AS commercial_org_rd        
          ,ISNULL(curr.marketing,0) - ISNULL(prev.marketing,0) AS marketing                
          ,ISNULL(curr.cmp_marketing,0) - ISNULL(prev.cmp_marketing,0) AS cmp_marketing            
          ,ISNULL(curr.ptc_mkt,0) - ISNULL(prev.ptc_mkt,0) AS ptc_mkt                  
          ,ISNULL(curr.print_category_mkt,0) - ISNULL(prev.print_category_mkt,0) AS print_category_mkt       
          ,ISNULL(curr.commercial_org_mkt,0) - ISNULL(prev.commercial_org_mkt,0) AS commercial_org_mkt       
          ,ISNULL(curr.sbo_mkt,0) - ISNULL(prev.sbo_mkt,0) AS sbo_mkt                  
          ,ISNULL(curr.wfss,0) - ISNULL(prev.wfss,0) AS wfss                     
          ,ISNULL(curr.admin,0) - ISNULL(prev.admin,0) AS admin                    
          ,ISNULL(curr.credit_cards_fees,0) - ISNULL(prev.credit_cards_fees,0) AS credit_cards_fees        
          ,ISNULL(curr.print_category_adm,0) - ISNULL(prev.print_category_adm,0) AS print_category_adm       
          ,ISNULL(curr.print_staff,0) - ISNULL(prev.print_staff,0) AS print_staff              
          ,ISNULL(curr.print_strategy_transform,0) - ISNULL(prev.print_strategy_transform,0) AS print_strategy_transform 
          ,ISNULL(curr.operations_adm,0) - ISNULL(prev.operations_adm,0) AS operations_adm           
          ,ISNULL(curr.other_commercial,0) - ISNULL(prev.other_commercial,0) AS other_commercial         
          ,ISNULL(curr.fsc,0) - ISNULL(prev.fsc,0) AS fsc                      
          ,ISNULL(curr.ptc_fsc,0) - ISNULL(prev.ptc_fsc,0) AS ptc_fsc                  
          ,ISNULL(curr.print_category_fsc,0) - ISNULL(prev.print_category_fsc,0) AS print_category_fsc       
          ,ISNULL(curr.pch_placeholder,0) - ISNULL(prev.pch_placeholder,0) AS pch_placeholder          
          ,ISNULL(curr.commercial_org_fsc,0) - ISNULL(prev.commercial_org_fsc,0) AS commercial_org_fsc       
          ,ISNULL(curr.operations_fsc,0) - ISNULL(prev.operations_fsc,0) AS operations_fsc           
          ,ISNULL(curr.total_opex,0) - ISNULL(prev.total_opex,0) AS total_opex               
          ,ISNULL(curr.total_spend,0) - ISNULL(prev.total_spend,0) AS total_spend              
          ,ISNULL(curr.ptc_spend,0) - ISNULL(prev.ptc_spend,0) AS ptc_spend           
           from fin_insights.curr_mon_flsh_opex curr
           inner join fin_insights.prev_mon_flsh_opex prev on 
           curr.fiscalyearquartercode = prev.fiscalyearquartercode
           and curr.programtype = prev.programtype
           and curr.region = prev.region
         ) ;   
    UPDATE A
	SET master_load_status = 1, dt_load_end_time = current_timestamp::timestamp 
	FROM fin_insights.opex_fls_pbi_config A
	WHERE run_id = (select max(run_id) from fin_insights.opex_fls_pbi_config) ;

	grant all on fin_insights.curr_mon_flsh_opex to ramdassu,annamary,nairakh,patel,halapets,lingaiah ;
	grant all on fin_insights.fy_total_from_curr_mon_fls_opex to ramdassu,annamary,nairakh,patel,halapets,lingaiah ;
	grant all on fin_insights.prev_mon_flsh_opex to ramdassu,annamary,nairakh,patel,halapets,lingaiah ;
	grant all on fin_insights.fy_total_from_prev_mon_fls_opex to ramdassu,annamary,nairakh,patel,halapets,lingaiah ;
	grant all on fin_insights.opex_from_prev_fy_act to ramdassu,annamary,nairakh,patel,halapets,lingaiah ;
	grant all on fin_insights.fy_total_from_prev_year_act_opex to ramdassu,annamary,nairakh,patel,halapets,lingaiah ;
           
    end if;
   
   EXCEPTION
  WHEN OTHERS THEN
    RAISE INFO 'Exception Occurred';

END;

$$