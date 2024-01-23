-------------------------------------------------------------Rev Bridge------------------------------------------------------------------
CREATE TABLE fin_insights.stg_rev_bridge 
(
    prev_fiscal_year_quarter_code character varying(10) ENCODE lzo,
    prev_fiscal_year_month_code character varying(25) ENCODE lzo,
    prev_flash numeric(38,1) ENCODE az64,
    ink numeric(38,1) ENCODE az64,
    toner numeric(38,1) ENCODE az64,
    paper numeric(38,1) ENCODE az64,
    churn numeric(38,1) ENCODE az64,
    free_months numeric(38,1) ENCODE az64,
    price_increase numeric(38,1) ENCODE az64,
    cum_enrollees numeric(38,1) ENCODE az64,
    gross_new_enrollees numeric(38,1) ENCODE az64,
    one_timer_errors_corrections numeric(38,1) ENCODE az64,
    region_contra numeric(38,1) ENCODE az64,
    overage_usage_decay numeric(38,1) ENCODE az64,
    curr_fiscal_year_quarter_code character varying(10) ENCODE lzo,
    curr_fiscal_year_month_code character varying(25) ENCODE lzo,
    curr_flash numeric(38,1) ENCODE az64
)
DISTSTYLE AUTO;

CREATE TABLE fin_insights.rev_bridge 
(
    prev_fiscal_year_quarter_code character varying(10) ENCODE lzo,
    prev_fiscal_year_month_code character varying(25) ENCODE lzo,
    prev_flash numeric(38,1) ENCODE az64,
    ink numeric(38,1) ENCODE az64,
    toner numeric(38,1) ENCODE az64,
    paper numeric(38,1) ENCODE az64,
    churn numeric(38,1) ENCODE az64,
    free_months numeric(38,1) ENCODE az64,
    price_increase numeric(38,1) ENCODE az64,
    cum_enrollees numeric(38,1) ENCODE az64,
    gross_new_enrollees numeric(38,1) ENCODE az64,
    one_timer_errors_corrections numeric(38,1) ENCODE az64,
    region_contra numeric(38,1) ENCODE az64,
    overage_usage_decay numeric(38,1) ENCODE az64,
    curr_fiscal_year_quarter_code character varying(10) ENCODE lzo,
    curr_fiscal_year_month_code character varying(25) ENCODE lzo,
    curr_flash numeric(38,1) ENCODE az64,
    flash_period character varying(25) ENCODE lzo,
    load_date timestamp without time zone DEFAULT ('now'::text)::timestamp with time zone ENCODE az64
)
DISTSTYLE AUTO;

CREATE OR REPLACE PROCEDURE fin_insights.load_rev_bridge()
 LANGUAGE plpgsql
AS $$

DECLARE get_flash_mon varchar(max) := '';
        flash_mon varchar(3) := '';
        copy_cmd varchar(max) :=  '';
        delete_cmd varchar(max) :=  '';
        get_flash_period varchar(max) := '';
        fls_period varchar(5) := '';

begin
	  get_flash_period := 'select flash_period from fin_insights.fls_pbi_config order by run_id desc limit 1'; 
	  EXECUTE get_flash_period INTO fls_period ;
	 
	  delete from fin_insights.stg_rev_bridge ;
	
	  copy_cmd := 'copy fin_insights.stg_rev_bridge
      from ''s3://instant-ink-finance/team-iifin/cos_rev_bridge/rev_bridge_' || lower(fls_period) || '_flash.csv''
      iam_role ''arn:aws:iam::828361281741:role/team-iifin''
      ignoreheader as 1
      maxerror 10
      emptyasnull
      blanksasnull
      delimiter as '',''
      DATEFORMAT ''auto''';

      execute copy_cmd ;

      delete_cmd = 'delete from fin_insights.rev_bridge where lower(flash_period) = ''' || lower(fls_period) || '''' ;

      execute delete_cmd ;

      insert into fin_insights.rev_bridge
      select
            prev_fiscal_year_quarter_code
           ,prev_fiscal_year_month_code
           ,prev_flash
           ,ink
           ,toner
           ,paper
           ,churn
           ,free_months
           ,price_increase
           ,cum_enrollees
           ,gross_new_enrollees
           ,one_timer_errors_corrections
           ,region_contra
           ,overage_usage_decay
           ,curr_fiscal_year_quarter_code
           ,curr_fiscal_year_month_code
           ,curr_flash
           ,(select flash_period from fin_insights.fls_pbi_config order by run_id desc limit 1) as flash_period
           ,date(current_timestamp)::timestamp
      from fin_insights.stg_rev_bridge ;

   EXCEPTION
  WHEN OTHERS THEN
    RAISE INFO 'Exception Occurred';

end;

$$

--select * from fin_insights.rev_bridge
------------------------------------------------------------Operating Profit (OP) Bridge--------------------------------------------------------------------------
CREATE TABLE fin_insights.stg_op_bridge 
(
    prev_fiscal_year_quarter_code character varying(10) ENCODE lzo,
    prev_fiscal_year_month_code character varying(25) ENCODE lzo,
    prev_flash numeric(38,1) ENCODE az64,
    gross_rev numeric(38,1) ENCODE az64,
    gross_margin numeric(38,1) ENCODE az64,
    ink numeric(38,1) ENCODE az64,
    toner numeric(38,1) ENCODE az64,
    paper numeric(38,1) ENCODE az64,
    cum_enrollees numeric(38,1) ENCODE az64,
    gross_new_enrollees numeric(38,1) ENCODE az64,
    sc_fcst_replenishment_kit numeric(38,1) ENCODE az64,
    cs_moh numeric(38,1) ENCODE az64,
    other_opex numeric(38,1) ENCODE az64,
    cmp_marketing numeric(38,1) ENCODE az64,
    diana_spend numeric(38,1) ENCODE az64,
    sw_spend numeric(38,1) ENCODE az64,
    overages numeric(38,1) ENCODE az64,
    curr_fiscal_year_quarter_code character varying(10) ENCODE lzo,
    curr_fiscal_year_month_code character varying(25) ENCODE lzo,
    curr_flash numeric(38,1) ENCODE az64
)
DISTSTYLE AUTO;

CREATE TABLE fin_insights.op_bridge 
(
    prev_fiscal_year_quarter_code character varying(10) ENCODE lzo,
    prev_fiscal_year_month_code character varying(25) ENCODE lzo,
    prev_flash numeric(38,1) ENCODE az64,
    gross_rev numeric(38,1) ENCODE az64,
    gross_margin numeric(38,1) ENCODE az64,
    ink numeric(38,1) ENCODE az64,
    toner numeric(38,1) ENCODE az64,
    paper numeric(38,1) ENCODE az64,
    cum_enrollees numeric(38,1) ENCODE az64,
    gross_new_enrollees numeric(38,1) ENCODE az64,
    sc_fcst_replenishment_kit numeric(38,1) ENCODE az64,
    cs_moh numeric(38,1) ENCODE az64,
    other_opex numeric(38,1) ENCODE az64,
    cmp_marketing numeric(38,1) ENCODE az64,
    diana_spend numeric(38,1) ENCODE az64,
    sw_spend numeric(38,1) ENCODE az64,
    curr_fiscal_year_quarter_code character varying(10) ENCODE lzo,
    curr_fiscal_year_month_code character varying(25) ENCODE lzo,
    curr_flash numeric(38,1) ENCODE az64,
    overages numeric(38,1) ENCODE az64,
    flash_period character varying(25) ENCODE lzo,
    load_date timestamp without time zone DEFAULT ('now'::text)::timestamp with time zone ENCODE az64
)
DISTSTYLE AUTO;

CREATE OR REPLACE PROCEDURE fin_insights.load_op_bridge()
 LANGUAGE plpgsql
AS $$

DECLARE get_flash_mon varchar(max) := '';
        flash_mon varchar(3) := '';
        copy_cmd varchar(max) :=  '';
        delete_cmd varchar(max) :=  '';
        get_flash_period varchar(max) := '';
        fls_period varchar(5) := '';

begin
	  get_flash_period := 'select flash_period from fin_insights.fls_pbi_config order by run_id desc limit 1'; 
	  EXECUTE get_flash_period INTO fls_period ;
	 
	  delete from fin_insights.stg_op_bridge ;
	
	  copy_cmd := 'copy fin_insights.stg_op_bridge
      from ''s3://instant-ink-finance/team-iifin/cos_rev_bridge/op_bridge_' || lower(fls_period) || '_flash.csv''
      iam_role ''arn:aws:iam::828361281741:role/team-iifin''
      ignoreheader as 1
      maxerror 10
      emptyasnull
      blanksasnull
      delimiter as '',''
      DATEFORMAT ''auto''';

      execute copy_cmd ;

      delete_cmd = 'delete from fin_insights.op_bridge where lower(flash_period) = ''' || lower(fls_period) || '''' ;

      execute delete_cmd ;

      insert into fin_insights.op_bridge
      select 
           prev_fiscal_year_quarter_code
          ,prev_fiscal_year_month_code
          ,prev_flash
          ,gross_rev
          ,gross_margin
          ,ink
          ,toner
          ,paper
          ,cum_enrollees
          ,gross_new_enrollees
          ,sc_fcst_replenishment_kit
          ,cs_moh
          ,other_opex
          ,cmp_marketing
          ,diana_spend
          ,sw_spend
          ,curr_fiscal_year_quarter_code
          ,curr_fiscal_year_month_code
          ,curr_flash
          ,overages
          ,(select flash_period from fin_insights.fls_pbi_config order by run_id desc limit 1) as flash_period
          ,date(current_timestamp)::timestamp
      from fin_insights.stg_op_bridge ; 
	 
   EXCEPTION
  WHEN OTHERS THEN
    RAISE INFO 'Exception Occurred';

end;

$$