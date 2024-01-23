CREATE OR REPLACE PROCEDURE fin_insights.sp_load_kit_partno_mapping()
 LANGUAGE plpgsql
AS $$
BEGIN
	drop table if exists fin_insights.stage_kit_part_number;
	
	create table fin_insights.stage_kit_part_number
(
  part_number VARCHAR(6) NOT NULL,
  kit_name VARCHAR(50) NOT NULL
);

copy fin_insights.stage_kit_part_number
from 's3://instant-ink-finance/team-iifin/profitability/kit_part_number.csv'
iam_role 'arn:aws:iam::828361281741:role/team-iifin'
ignoreheader as 1
maxerror 10
emptyasnull
blanksasnull
delimiter as ',';

delete  from fin_insights.xref_kit_part_number 
where (kit, part_number)  
in (select s.kit_name, s.part_number
from  fin_insights.xref_kit_part_number f, fin_insights.stage_kit_part_number s 
where f.part_number = s.part_number) ; 

insert into fin_insights.xref_kit_part_number
(kit, part_number) 
select distinct s.kit_name,s.part_number 
from fin_insights.stage_kit_part_number s
where not exists (	select top 1 * 
						from fin_insights.xref_kit_part_number f 
						where f.part_number = s.part_number 
					) ;

EXCEPTION WHEN OTHERS then 
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate)
     VALUES (
             'Kit_Part_Number',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
            );

END;
$$


call fin_insights.sp_load_kit_partno_mapping();
select count(1) from fin_insights.xref_kit_part_number --206 --206
-----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_cust_support_cost()
 LANGUAGE plpgsql
AS $$
BEGIN
	drop table if exists fin_insights.stage_cost_custmer_support_file ;

create table fin_insights.stage_cost_custmer_support_file
(
	month_year VARCHAR(22) NOT NULL,
	region VARCHAR(22) NOT NULL,
	CS_Cost  decimal(10,2)  NOT null
);

--s3://instant-ink-finance/team-iifin/
copy fin_insights.stage_cost_custmer_support_file
from 's3://instant-ink-finance/team-iifin/profitability/cost_customer_support.csv'
iam_role 'arn:aws:iam::828361281741:role/team-iifin'
ignoreheader as 1
maxerror 10
emptyasnull
blanksasnull
delimiter as ',';

delete select * from fin_insights.csc_cost_file 
where (month_year, region)  
in (select case when left(s.month_year,1) = 0 then right(s.month_year,6) else s.month_year end as month_year, s.region
from  fin_insights.csc_cost_file f, fin_insights.stage_cost_custmer_support_file s
where f.month_year = s.month_year and UPPER(f.region) = UPPER(s.region)) ;

insert into fin_insights.csc_cost_file
(month_year, region,CS_Cost)
select * from (select distinct case when left(s.month_year,1) = 0 then right(s.month_year,6) else s.month_year end as month_year, s.region, s.CS_Cost
from fin_insights.stage_cost_custmer_support_file s )k
where not exists (	select top 1 * 
						from fin_insights.csc_cost_file f 
						where f.month_year = k.month_year
						and f.region = k.region
				 ) ;

drop table if exists fin_insights.stage_cost_customer_support_per_case;

create table fin_insights.stage_cost_customer_support_per_case as  
select distinct
	  f.created_on_date_month year_month,
	  created_on_month_year month_year,
	  f.region_code, 
	  sum((s.cs_cost /f.case_code_count)) cost_per_case
	
from 
(
	select 
		TRUNC(date_trunc('month', f.created_on_date)) as created_on_date_month, 
		DATEPART(MONTH, created_on_date_month)|| '_' ||DATEPART(YEAR, created_on_date_month) as created_on_month_year,  
		c.region_code,
		count(f.case_code) as case_code_count
	from app_instant_ink_bi_fact.fact_cdax f
	left join fin_insights.vw_dim_country c on f.country_code = c.country_code2
	where f.program_type = 'Instant Ink' 
	and  f.subscription_type in ('Billable Customer','HP Employees')
	and created_on_date >= '2017-09-01 00:00:00'
	and c.region_code <> 'other'
	group by TRUNC(date_trunc('month', f.created_on_date)) ,c.region_code
) f 
left join (select *
           from fin_insights.csc_cost_file
          ) s on s.month_year = f.created_on_month_year and s.region = f.region_code
group by f.created_on_date_month,f.created_on_month_year,f.region_code ;

drop table if exists fin_insights.stage_cost_customer_support;

create table fin_insights.stage_cost_customer_support  as 
select subscription_id,
       year_month,
       country_code,
       region_code,
       cost_customer_support,
       cust_support_count
from (
       select f.subscription_id, 
	          f.year_month ,  
	          f.country_code, 
              f.region_code,	
	          sum(s.cost_per_case) as cost_customer_support,                                  -- calculate the sum of the customer support cost based on the subscription id
              count(s.cost_per_case) as cust_support_count,
              row_number () over (partition by f.subscription_id, f.year_month order by cost_customer_support desc) as rownum -- added to identify duplicate subscription in multiple countries
from (
	   select distinct f.case_code,
		               f.subscription_id,
		               --TRUNC(date_trunc('month', f.created_on_date)) as created_on_date_month,
		               TRUNC(date_trunc('month', created_on_date)) as year_month,
		               DATEPART(MONTH, year_month)|| '_' ||DATEPART(YEAR, year_month) as created_on_month_year,
		               --case c.region_code when 'NA' then 'AMS' else c.region_code end region_code,
		               c.region_code,
		               f.country_code
	   from app_instant_ink_bi_fact.fact_cdax f
	   left join fin_insights.vw_dim_country c on f.country_code = c.country_code2
	   where f.program_type = 'Instant Ink'
	   and  f.subscription_type in ('Billable Customer','HP Employees')
	   and created_on_date >= '2017-09-01 00:00:00'
	   and c.region_code <> 'other' 
     ) f 
left join fin_insights.stage_cost_customer_support_per_case s 
on s.month_year = f.created_on_month_year 
and s.region_code = f.region_code
group by f.subscription_id,f.year_month,f.country_code,f.region_code
     )
    where rownum = 1;	

 	
EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate) 
     VALUES (
             'Customer_Support',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
             );

END;
$$


call fin_insights.sp_load_cust_support_cost();
select count(1) from fin_insights.csc_cost_file --112 --116 --118 --120 --122 --124 --126 --128 --130 --132 --134 --136 --138 --140 --142 --144 --148
select count(1) from fin_insights.stage_cost_customer_support --7738523 --7763418 --7849243 --7858825 --7958253 --8212904 --8391975 --8664529 --8763319 --8688726 --8864705 --9073035 --9263760 --9441973--9462939 --10529257 --10725409 --10993482 --11216006 --12275026 --12552526 --12768008 --12940205 --10545579


select month_year,region,count(1) from fin_insights.csc_cost_file
group by month_year,region
having count(1) > 1

select * from (select year_month,region_code,round(sum(cost_customer_support),2) as cust_support_cost_subid,sum(cust_support_count) as cust_support_count
from fin_insights.stage_cost_customer_support 
group by year_month,region_code )
where region_code <> 'APJ' and year_month = '2023-11-01'
order by year_month desc,region_code asc 

--------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_esc_cost()
 LANGUAGE plpgsql
AS $$
BEGIN
     drop table if exists fin_insights.stage_cost_esc_file;

--create the stage table before loadign the data
create table fin_insights.stage_cost_esc_file
(
	sku VARCHAR(22) NOT NULL,
	tech VARCHAR(22) NOT NULL,
	configuration VARCHAR(30) NOT NULL,
	fiscal_year_quarter_code VARCHAR(22)  NOT NULL,
	cpu decimal(8,2)
);

--copy csv/excel file to stage table   
copy fin_insights.stage_cost_esc_file
from 's3://instant-ink-finance/team-iifin/profitability/esc_cost.csv'
iam_role 'arn:aws:iam::828361281741:role/team-iifin'
ignoreheader as 1
maxerror 10
emptyasnull
blanksasnull
delimiter as ',';

delete  from fin_insights.esc_cost_file
where (SKU, fiscal_year_quarter_code)  
in (select s.SKU, s.fiscal_year_quarter_code
from  fin_insights.esc_cost_file f, fin_insights.stage_cost_esc_file s
where f.SKU = s.SKU and f.fiscal_year_quarter_code = s.fiscal_year_quarter_code) ;

insert into fin_insights.esc_cost_file
(SKU, tech, configuration, fiscal_year_quarter_code, cpu)
select SKU, 
       tech, 
       configuration, 
       fiscal_year_quarter_code, 
       cpu
from fin_insights.stage_cost_esc_file s
where not exists (	select top 1 * 
						from fin_insights.esc_cost_file f 
						where f.SKU = s.SKU
						and f.fiscal_year_quarter_code = s.fiscal_year_quarter_code
					) ;

--drop stage table
drop table if exists fin_insights.stage_cost_esc;

--ESC cost allocation to Shipment base
create table fin_insights.stage_cost_esc as 
select distinct
	   f.subscription_id, 
	   TRUNC(date_trunc('month', f.create_timestamp)) as year_month,           -- based on the subscription id calculated the sum of esc cost on monthly wise
	   f.order_country_code country_code, 
	   c.region_code , 
	   sum(s.cpu) cost_esc,
	   sum(case when f.shipment_type = 'InkShipment' then s.cpu end) as cost_esc_ink_shipment,  
	   sum(case when f.shipment_type = 'WelcomeKitShipment' then s.cpu end)  as cost_esc_welcome_kit_shipment,
	   count(s.cpu) as esc_count
from bi_fact_pii.iink_ship_base 	f
left join app_instant_ink_bi_dim.dim_date_time 		d on TRUNC(date_trunc('month', f.create_timestamp)) = d.calendar_date
left join fin_insights.vw_dim_country 				c on f.order_country_code = c.country_code2
left join fin_insights.esc_cost_file			s on LEFT(f.part_number, 6) = s.sku and d.fiscal_year_quarter_code = s.fiscal_year_quarter_code
where f.subscription_type in ('Billable Customer','HP Employees') 
and f.create_timestamp >= '2016-11-01 00:00:00' and program_type = 'Instant Ink' and c.region_code is not null
group by f.subscription_id, TRUNC(date_trunc('month', f.create_timestamp)),f.order_country_code, c.region_code ;

EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate) 
     VALUES (
             'ESC',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
             );					
					
END;
$$

call fin_insights.sp_load_esc_cost() ;
select count(1) from fin_insights.esc_cost_file --4697 --4701 --4937 --4937 --5173 --5173 --5409 --5436 --5655 --5655 --5655 --5874--5874- --6093 --6093 --6312
select count(1) from fin_insights.stage_cost_esc --48667374 --48741494 --48995534 --48995534 --49550025 --49638387 --51246339 --52247839 --53731846 --54605283 --55704335 --56816792 --58073628 --59336508 --60507269 --60645237 --63394048 --64347132 --65847386 --68186521 --69321247 --70725212 --71778434 --72888134 --74041331- --76950446


select sku,tech,configuration, fiscal_year_quarter_code, count(1) from fin_insights.esc_cost_file
group by sku,tech,configuration, fiscal_year_quarter_code 
having count(1) >1 


select year_month,region_code,sum(cost_esc) as cost_esc, sum(cost_esc_ink_shipment) as cost_esc_ink_shipment
,sum(cost_esc_welcome_kit_shipment) as cost_esc_welcome_kit_shipment , sum(esc_count) as esc_count
from fin_insights.stage_cost_esc
where year_month >= '2018-10-01'
group by year_month,region_code
order by year_month desc,region_code asc
------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_kit_cost()
 LANGUAGE plpgsql
AS $$
BEGIN
    drop table if exists fin_insights.stage_cost_kit_file;

create table fin_insights.stage_cost_kit_file
(
	month_year VARCHAR(22) NOT NULL,
	region VARCHAR(22) NOT NULL,
	kit_name VARCHAR(50) NOT NULL,
	kit_cpu decimal(8,2)  NOT NULL
) ;

copy fin_insights.stage_cost_kit_file
from 's3://instant-ink-finance/team-iifin/profitability/cost_kitting.csv'
iam_role 'arn:aws:iam::828361281741:role/team-iifin'
ignoreheader as 1
maxerror 10
emptyasnull
blanksasnull
delimiter as ',';

delete  from fin_insights.kit_cost_file 
where (month_year, region)  
in (select case when left(s.month_year,1) = 0 then right(s.month_year,6) else s.month_year end as month_year, s.region
from  fin_insights.csc_cost_file f, fin_insights.stage_cost_kit_file s
where f.month_year = s.month_year and UPPER(f.region) = UPPER(s.region)) ;

insert into fin_insights.kit_cost_file
(month_year, region,kit_name,kit_cpu)
select distinct case when left(s.month_year,1) = 0 then right(s.month_year,6) else s.month_year end as month_year, s.region, s.kit_name, s.kit_cpu
from fin_insights.stage_cost_kit_file s
where not exists (	select top 1 * 
						from fin_insights.kit_cost_file f 
						where f.month_year = s.month_year
						and f.region = s.region
					) ;				
 
drop table if exists fin_insights.stage_cost_kit;

create table fin_insights.stage_cost_kit as 
select 
	   f.subscription_id,
	   f.create_date_month year_month,
	   f.country_code,
	   f.region_code,
	   sum(k.kit_cpu) as cost_kit, 
	   sum(case when f.shipment_type = 'InkShipment' then k.kit_cpu end) as cost_kit_ink_shipment,  
	   sum(case when f.shipment_type = 'WelcomeKitShipment' then k.kit_cpu end)  as cost_kit_welcome_kit_shipment
from
(
	select 
		f.subscription_id,
		TRUNC(date_trunc('month', f.create_timestamp)) as create_date_month, 
		DATEPART(MONTH,  f.create_timestamp) || '_' || DATEPART(YEAR,  f.create_timestamp) create_month_year,
		f.shipment_type, 
		case  when  shipping_speed in ('priority') then  'standard' else shipping_speed end AS shipping_speed, 
		f.supply_platform,  
		LEFT(f.part_number, 6) AS part_number_6, 
		f.order_country_code country_code,
		c.region_code,
		x.kit
	from bi_fact_pii.iink_ship_base f
	left join fin_insights.vw_dim_country c on f.order_country_code = c.country_code2
	left join fin_insights.xref_kit_part_number x on LEFT(f.part_number, 6) = x.part_number
	where f.subscription_type in ('Billable Customer', 'HP Employees')
	and create_timestamp >= '2016-11-01 00:00:00' and program_type = 'Instant Ink'and c.region_code is not null
) f
left join 
(
	select 
	month_year ,
	region as region_code,
	kit_name,
	kit_cpu
	from fin_insights.kit_cost_file
) k on f.create_month_year = k.month_year and f.region_code=k.region_code and f.kit = k.kit_name
group by f.subscription_id,f.create_date_month,f.country_code,f.region_code ;


EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate) 
     VALUES (
             'Kit',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
             );					
					
END;
$$

call fin_insights.sp_load_kit_cost() ;
select count(1) from fin_insights.kit_cost_file --756 --804 --836 --852 --868 --884 --900 --916 --932 --948 --964 --992--1004 --1016 --1040 --1056 --1072 --1088 --1100 --1124
select count(1) from fin_insights.stage_cost_kit --48601940 --48667374 --48741494 --48995534 --48995534 --49638387 --51246339 --53731846 --54605283 --55698072 --56816792 --58073628 --59336508 --60507269 --60645237 --63394048 --64347132 --65847386 --68186521 --69321247 --70725212 --71778434 --72888134 --74041331 --76950446

select MONTH_YEAR,region,kit_name,count(1) from fin_insights.kit_cost_file
group by MONTH_YEAR,region,kit_name 
having count(1) > 1 


select year_month,region_code,sum(cost_kit) as cost_kit, sum(cost_kit_ink_shipment) as cost_kit_ink_shipment
,sum(cost_kit_welcome_kit_shipment) as cost_kit_welcome_kit_shipment from fin_insights.stage_cost_kit
where year_month >= '2018-10-01'
group by year_month,region_code
order by year_month desc,region_code asc
-------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_logistics_cost()
 LANGUAGE plpgsql
AS $$
BEGIN
      drop table if exists fin_insights.stage_cost_logistics_file ;

create table fin_insights.stage_cost_logistics_file
(
	month_year VARCHAR(22) NOT NULL,
	country VARCHAR(22) NOT NULL,
	kit_name VARCHAR(50) NOT NULL,
	logistic_CPU decimal(8,4)  NOT NULL,
	shipping_speed VARCHAR(22) NOT null,
	Mapping VARCHAR(55)
) ;


copy fin_insights.stage_cost_logistics_file  
from 's3://instant-ink-finance/team-iifin/profitability/cost_logistics.csv'
iam_role 'arn:aws:iam::828361281741:role/team-iifin'
ignoreheader as 1
maxerror 10
emptyasnull
blanksasnull
delimiter as ',';

delete  from fin_insights.logistics_cost_file
where (month_year, country,kit_name,shipping_speed)  
in (select case when left(s.month_year,1) = 0 then right(s.month_year,6) else s.month_year end as month_year, s.country, s.kit_name, s.shipping_speed
from  fin_insights.logistics_cost_file f, fin_insights.stage_cost_logistics_file s
where f.month_year = s.month_year 
and UPPER(f.country) = UPPER(s.country)
and UPPER(f.kit_name) = UPPER(s.kit_name)
and UPPER(f.shipping_speed) = UPPER(s.shipping_speed)) ;

insert into fin_insights.logistics_cost_file
(month_year, country,kit_name,logistic_CPU,shipping_speed,Mapping)
select * from (select distinct case when left(s.month_year,1) = 0 then right(s.month_year,6) else s.month_year end as month_year, s.country, s.kit_name, s.logistic_CPU, s.shipping_speed, s.Mapping
from fin_insights.stage_cost_logistics_file s) k
where not exists (	select top 1 * 
						from fin_insights.logistics_cost_file f 
						where f.month_year = k.month_year
						and UPPER(f.country) = UPPER(k.country)
                        and UPPER(f.kit_name) = UPPER(k.kit_name)
                        and UPPER(f.shipping_speed) = UPPER(k.shipping_speed)
				  ) ;				

drop table if exists fin_insights.stage_cost_logistics ;

create table fin_insights.stage_cost_logistics as
select 
	   f.subscription_id,
	   f.create_date_month year_month,
	   f.country_code,
	   f.region_code,
	   sum(k.logistic_CPU) as cost_logistics,
       count(case when f.shipment_type = 'InkShipment' then 1 end ) as InkShipment,
       count(case when f.shipment_type = 'WelcomeKitShipment' then 1 end ) as WelcomeKitShipment,	
	   sum(case when f.shipment_type = 'InkShipment' then k.logistic_CPU end) as cost_logistics_ink_shipment,  
	   sum(case when f.shipment_type = 'WelcomeKitShipment' then k.logistic_CPU end)  as cost_logistics_welcome_kit_shipment,
	   count(CASE when f.shipping_speed = 'standard' then 1 end ) as shipping_speed_standard,                
       count(CASE when f.shipping_speed = 'express' then 1 end ) as shipping_speed_express                  
from (
      select f.subscription_id,
             f.create_date_month,
             f.country_code,
             f.region_code,
             f.shipment_type,
             f.shipping_speed,
             create_month_year|| '_' ||Region|| '_' ||kit|| '_' ||f.shipping_speed as Logistic_map 
      from (
	        select 
		          f.subscription_id,
		          TRUNC(date_trunc('month', f.create_timestamp)) as create_date_month, 
		          DATEPART(MONTH,  f.create_timestamp) || '_' || DATEPART(YEAR,  f.create_timestamp) create_month_year,
		          f.shipment_type, 
		          case  when  shipping_speed in ('priority') then  'standard' else shipping_speed end AS shipping_speed, 
		          LEFT(f.part_number, 6) AS part_number_6, 
		          f.order_country_code country_code,
		          case when order_country_code in ('US') then 'US'
                       when order_country_code in ('CA') then 'CA'   
                       when order_country_code in ('HU', 'CZ' ,'HR', 'BG', 'SK' ,'PL', 'ES', 'IT', 'NL', 'BE', 'PT', 'SE' ,'RO', 'FI' ,'GB', 'LU', 'IL', 'CH', 'IE', 'DE', 'DK', 'FR' ,'GR', 'AT', 'NO', 'CY', 'EE', 'LT', 'LV', 'MT', 'SI') then 'EMEA'
                       else 'other' END AS Region,
		          c.region_code,
		          x.kit
	        from bi_fact_pii.iink_ship_base f
	        left join fin_insights.vw_dim_country c on f.order_country_code = c.country_code2
	        left join fin_insights.xref_kit_part_number x on LEFT(f.part_number, 6) = x.part_number
	        where f.subscription_type in ('Billable Customer', 'HP Employees') and program_type = 'Instant Ink'and c.region_code is not null
	        and create_timestamp >= '2016-11-01 00:00:00'
           ) f 
	 ) f
left join 
(
	select 
	      f.month_year,
	      f.country as country_code,
	      f.kit_name,
	      f.shipping_speed,
	      f.logistic_CPU,
	      f.mapping
	from fin_insights.logistics_cost_file f
) k on f.Logistic_map = k.mapping 
group by f.subscription_id,f.create_date_month,f.country_code,f.region_code ;

grant select on fin_insights.stage_cost_logistics_file to auto_prdii;

	
EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate) 
     VALUES (
             'Logistics',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
             );					
					
END;
$$

call fin_insights.sp_load_logistics_cost() ;
select count(1) from fin_insights.logistics_cost_file --1912--1972 --2002 --2002 --2032 --2062 --2092 --2122 --2152 --2182 --2212 --2242 --2272 --2302 --2332 --2362 --2392 --2422 --2452 --2482 --2542
select count(1) from fin_insights.stage_cost_logistics --48667374 --48741494 --48995534 --49286470 --49638387 --52317972 --53731846 --55698072 --56816792 --58073628 --59336508 --60507269 --60645237 --63394048 --64347132 --65847386 --68186521 --69321247 --70725212 --71778434 --72888134 --74041331

select month_year,country,kit_name,shipping_speed, count(1) from fin_insights.logistics_cost_file
group by month_year,country,kit_name,shipping_speed
having count(1) > 1

select year_month,region_code,sum(cost_logistics) as cost_logistic, 
	  sum(inkshipment) asinkshipment , 
	  sum(welcomekitshipment) as welcomekitshipment, 
	  sum(cost_logistics_ink_shipment) as cost_logistic_ink_shipment, 
	  sum(cost_logistics_welcome_kit_shipment) as cost_logistic_welcome_kit_shippment,
      sum(shipping_speed_standard) as shipping_speed_standard, 
	  sum(shipping_speed_express)  as shipping_speed_express
	  from fin_insights.stage_cost_logistics
	  where year_month >= '2018-10-01'
group by year_month,region_code
order by year_month desc,region_code asc

----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_ptb_warranty_cost()
 LANGUAGE plpgsql
AS $$
BEGIN
     drop table if exists fin_insights.stage_cost_ptb_warranty_file ;

create table fin_insights.stage_cost_ptb_warranty_file
(
	month_year VARCHAR(22) NOT NULL,
	region VARCHAR(22) NOT NULL,
	ptb  decimal(10,3)  NOT NULL,
	warranty decimal(10,3) NOT NULL
);

copy fin_insights.stage_cost_ptb_warranty_file
from 's3://instant-ink-finance/team-iifin/profitability/cost_ptb_warranty.csv'
iam_role 'arn:aws:iam::828361281741:role/team-iifin'
ignoreheader as 1
maxerror 10
emptyasnull
blanksasnull
delimiter as ',';

delete  from fin_insights.ptb_warranty_cost_file 
where (month_year, region)  
in (select case when left(s.month_year,1) = 0 then right(s.month_year,6) else s.month_year end as month_year, s.region
from  fin_insights.ptb_warranty_cost_file f, fin_insights.stage_cost_ptb_warranty_file s
where f.month_year = s.month_year and UPPER(f.region) = UPPER(s.region)) ;

insert into fin_insights.ptb_warranty_cost_file
(month_year, region,ptb,warranty)
select * from (select distinct case when left(s.month_year,1) = 0 then right(s.month_year,6) else s.month_year end as month_year, s.region, s.ptb, s.warranty
from fin_insights.stage_cost_ptb_warranty_file s ) k
where not exists (	select top 1 * 
						from fin_insights.ptb_warranty_cost_file f 
						where f.month_year = k.month_year
						and f.region = k.region
					) ;	

drop table if exists fin_insights.stage_cost_ptb_warranty_per_subscription_id ;

create table fin_insights.stage_cost_ptb_warranty_per_subscription_id as  
select 
	   f.year_month,
	   f.month_year,
	   f.region_code, 
	   (s.ptb/subscription_id_count) as cost_ptb_per_subscription_id, 
	   (s.warranty/subscription_id_count) as cost_warranty_per_subscription_id  
from (		
      select *,
	         DATEPART(MONTH, year_month) || '_' || DATEPART(YEAR, year_month) month_year 
	  from (
	        SELECT
		          region_id region_code, 
		          TRUNC(date_trunc('month', billing_cycle_end_time)) as year_month,
		          --DATEPART(MONTH, billing_cycle_end_time) || '_' || DATEPART(YEAR, billing_cycle_end_time) as month_year,
		          count(subscription_id) as subscription_id_count 
		          from  bi_fact_pii.iink_billing_base
		          where subscription_type in ('Billable Customer', 'HP Employees') 
		          and billing_cycle_end_time >= '2016-11-01' and program_type = 'Instant Ink'
		          group by year_month, region_id 
		   ) A
		
     ) f 
left join (select month_year,
                  region,
                  ptb,
                  warranty
           from fin_insights.ptb_warranty_cost_file
           ) s on 
f.month_year=s.month_year 
and f.region_code = s.region ;

drop table if exists fin_insights.stage_cost_ptb_warranty ;

create table fin_insights.stage_cost_ptb_warranty as
with t1 as 
( select  subscription_id, region_id as region_code, country_id as country_code, billing_cycle_end_time, 
TRUNC(date_trunc('month', billing_cycle_end_time)) as year_month,
TRUNC(date_trunc('month', billing_cycle_start_time)) as Year_Month_bcst,
DATEPART(YEAR, billing_cycle_end_time) as year1,
DATEPART(MONTH, billing_cycle_end_time) as month1,  month1|| '_' ||year1 as Month_year
from  bi_fact_pii.iink_billing_base
where subscription_type in  ('Billable Customer', 'HP Employees') 
and program_type = 'Instant Ink' and billing_cycle_end_time >= '2016-11-01'
)
select t1.subscription_id,
       t1.year_month,
       t1.region_code,
       t1.country_code,
       T2.cost_ptb_per_subscription_id as cost_ptb, 
       t2.cost_warranty_per_subscription_id as cost_warranty FROM t1
left join fin_insights.stage_cost_ptb_warranty_per_subscription_id t2
on t1.Month_year = t2.month_year and 
   t1.region_code = t2.region_code  ;


EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate) 
     VALUES (
             'PTB_Warranty',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
             );					
					
END;
$$

call fin_insights.sp_load_ptb_warranty_cost() ;

select count(1) from fin_insights.ptb_warranty_cost_file --134 --138 --140 --142--146 --148 --150 --152 --154 --156 --158 --160 --162 --164 --166 --170
select count(1) from fin_insights.stage_cost_ptb_warranty --364715407 --365239836 --365599337 --367613725 --367614578 --367646880 --367923868 --373756639 --388028949 --397728689 --411922221 --421389833 --432096060 --442187874 --454010290 --464818230 --475808929 --489805799 --501770477 --512668560 --527234471 --539564758 --553143803 --568708531 --579867149 --590838015 --602265502
select count(1) from app_instant_ink_bi_fact.fact_billing_cycle_base 

select year_month as year_month,region_code,sum(cost_ptb) as cost_ptb,
sum(cost_warranty) as cost_warranty
from fin_insights.stage_cost_ptb_warranty
	  where year_month >= '2018-10-01'
group by year_month,region_code
order by year_month desc,region_code asc
--------------------------------------------------------------------------------------------------------------------------
drop table if exists fin_insights.vw_fact_cost_free_months ;

CREATE table fin_insights.vw_fact_cost_free_months
AS
SELECT 
	subscription_id, 
	TRUNC(date_trunc('month', billing_cycle_end_time)) AS year_month,
	region_id region_code,
	SUM(plan_price_in_cents) cost_free_months,
	COUNT(billing_cycle_id) AS freemonths_count                          -- added new columns in cost fact table
FROM bi_fact_pii.iink_billing_base
WHERE subscription_type IN ('Billable Customer', 'HP Employees') 
AND billing_cycle_end_time >= '2016-11-01' 
AND  payment_events_payment_engine_description = 'Free Month - No Charge' 
AND plan_price_in_cents <> 0 and program_type = 'Instant Ink'
GROUP BY subscription_id,  TRUNC(date_trunc('month', billing_cycle_end_time)), region_id ;

select count(1) from fin_insights.vw_fact_cost_free_months --34060725 --34147970 --34761666 --36300869 --37317130 --38801442 --39753613 --40835536 --41946786 --45520935 --48644548 --50139297 --52221229 --54331405 --56414734 --58768031 --60267809 --61587287 --63093571 --66036053

----------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_costofsales()
 LANGUAGE plpgsql
AS $$
BEGIN

	drop table if exists fin_insights.stage_cost;

CREATE TABLE fin_insights.stage_cost as
SELECT distinct COALESCE (T1.subscription_id, T2.subscription_id) as subscription_id,
	   COALESCE (T1.year_month, T2.year_month) as year_month,
	   T1.country_code,
	   T1.region_code,	
	   T1.cost_ptb , 
	   T1.cost_warranty,						
	   T1.cost_esc,
	   T1.cost_esc_ink_shipment,			
	   T1.cost_esc_welcome_kit_shipment,
	   T1.esc_count,
	   T1.cost_logistics,
	   T1.inkshipment,
	   T1.welcomekitshipment,			 
	   T1.cost_logistics_ink_shipment,
	   T1.cost_logistics_welcome_kit_shipment,
	   T1.shipping_speed_standard,
	   T1.shipping_speed_express,
	   T1.cost_kit, 
	   T1.cost_kit_ink_shipment, 
	   T1.cost_kit_welcome_kit_shipment,
	   T1.cost_customer_support, 
	   T1.cust_support_count,
	   T2.cost_free_months,
	   T2.freemonths_count
from (
		select subscription_id,
		       year_month,
		       country_code,
		       region_code,	
		       cost_ptb , 
		       cost_warranty,						
		       cost_esc,
		       cost_esc_ink_shipment,			
		       cost_esc_welcome_kit_shipment,
		       esc_count,
		       cost_logistics,
		       inkshipment,
		       welcomekitshipment,	   
		       cost_logistics_ink_shipment,
		       cost_logistics_welcome_kit_shipment,
		       shipping_speed_standard,
		       shipping_speed_express,
		       cost_kit, 
		       cost_kit_ink_shipment, 
		       cost_kit_welcome_kit_shipment,
		       cost_customer_support, 
		       cust_support_count,
		       seqnum
		from (
               SELECT subscription_id,
	                  year_month,
	                  country_code,
                      region_code,	
	                  cost_ptb , 
                      cost_warranty,						
	                  cost_esc,
	                  cost_esc_ink_shipment,			
	                  cost_esc_welcome_kit_shipment,
	                  esc_count,
	                  cost_logistics,
	                  inkshipment,
	                  welcomekitshipment,	   
	                  cost_logistics_ink_shipment,
	                  cost_logistics_welcome_kit_shipment,
	                  shipping_speed_standard,
                      shipping_speed_express,
	                  cost_kit, 
	                  cost_kit_ink_shipment, 
	                  cost_kit_welcome_kit_shipment,
	                  cost_customer_support, 
                      cust_support_count,
                      ROW_NUMBER() OVER (PARTITION BY subscription_id, year_month ORDER BY year_month desc) as seqnum 
               FROM (
					  SELECT COALESCE (T1.subscription_id, T2.subscription_id) as subscription_id,
							 COALESCE (T1.year_month, T2.year_month) as year_month,
							 T1.country_code,
							 COALESCE(T1.region_code, T2.region_code) as region_code,	
							 cost_ptb , 
							 cost_warranty,						
							 cost_esc,
							 cost_esc_ink_shipment,			
							 cost_esc_welcome_kit_shipment,
							 esc_count,
							 cost_logistics,
							 inkshipment,
							 welcomekitshipment,			 
							 cost_logistics_ink_shipment,
							 cost_logistics_welcome_kit_shipment,
							 shipping_speed_standard,
							 shipping_speed_express,
							 cost_kit, 
							 cost_kit_ink_shipment, 
							 cost_kit_welcome_kit_shipment,
							 cost_customer_support, 
							 cust_support_count
					 
					  FROM (
							SELECT 
								   COALESCE (T1.subscription_id, T2.subscription_id) as subscription_id ,
								   COALESCE (T1.year_month, T2.year_month) as year_month,
								   T1.country_code,
								   COALESCE(T1.region_code, T2.region_code) as region_code,	
								   cost_ptb , 
								   cost_warranty,
								   cost_esc,
								   cost_esc_ink_shipment,			
								   cost_esc_welcome_kit_shipment,	
								   esc_count,
								   cost_logistics,
								   inkshipment,
								   welcomekitshipment,				   
								   cost_logistics_ink_shipment,
								   cost_logistics_welcome_kit_shipment,
								   shipping_speed_standard,
								   shipping_speed_express,
								   cost_kit, 
								   cost_kit_ink_shipment, 
								   cost_kit_welcome_kit_shipment  
							FROM (
						           select COALESCE (T1.subscription_id, T2.subscription_id) as subscription_id ,
								          COALESCE (T1.year_month, T2.year_month) as year_month,
								          T1.country_code as country_code,
								          COALESCE(T1.region_code, T2.region_code) as region_code,	
								          cost_ptb , 
								          cost_warranty,
								          cost_esc,
								          cost_esc_ink_shipment,			--maintenance cost 
								          cost_esc_welcome_kit_shipment ,	--acquisition cost
								          esc_count,
								          cost_logistics,
								          inkshipment,
								          welcomekitshipment,				   
								          cost_logistics_ink_shipment ,
								          cost_logistics_welcome_kit_shipment,
								          shipping_speed_standard,
								          shipping_speed_express 
						           FROM (
                                          SELECT COALESCE (T1.subscription_id, T2.subscription_id) as subscription_id ,
	                                             COALESCE (T1.year_month, T2.year_month) as year_month,
	                                             COALESCE(t1.country_code, t2.country_code) as country_code,
                                                 COALESCE(T1.region_code, T2.region_code) as region_code,	
	                                             cost_ptb , 
                                                 cost_warranty,
	                                             cost_esc,
	                                             cost_esc_ink_shipment,			--maintenance cost 
	                                             cost_esc_welcome_kit_shipment ,	--acquisition cost
	                                             esc_count      
                                          FROM fin_insights.stage_cost_ptb_warranty T1
                                          FULL OUTER JOIN fin_insights.stage_cost_esc T2
                                          ON    T1.subscription_id = T2.subscription_id 
                                          AND   T1.year_month =  T2.year_month 
                                        ) T1
                                   FULL OUTER JOIN fin_insights.stage_cost_logistics T2
                                   ON    T1.subscription_id = T2.subscription_id 
                                   AND   T1.year_month =  T2.year_month 
                  
                                 ) T1    
                            FULL OUTER JOIN fin_insights.stage_cost_kit T2
                            ON    T1.subscription_id = T2.subscription_id 
                            AND   T1.year_month =  T2.year_month 
			               ) T1

                    FULL OUTER JOIN fin_insights.stage_cost_customer_support T2
                    ON   T1.subscription_id = T2.subscription_id 
                    AND  T1.year_month =  T2.year_month 
                    )
		     ) where seqnum = 1
     ) T1
LEFT JOIN fin_insights.vw_fact_cost_free_months T2
ON   T1.subscription_id = T2.subscription_id 
AND  T1.year_month =  T2.year_month ;


EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate) 
     VALUES (
             'Fact_Cost',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
             );					
					
END;
$$

call fin_insights.sp_load_costofsales() ;
 
select count(1) from fin_insights.stage_cost --378306989 --378845413 --379221005 --381302523 --381580110 --387527814 --402099906 --411979536 --426441825 --436066136 --447002646 --457356171 --469459849 --480534781 --491788061 --518481738 --529609064 --544516435- --557335310 --571307372 --587144192 --598526206 --609738786 --621405157

select 
      year_month,
      region_code,	
      sum(cost_ptb) as cost_ptb , 
      sum(cost_warranty) as cost_warranty,						
      sum(cost_esc) as cost_esc ,
      sum(cost_esc_ink_shipment) as cost_esc_ink_shipment,			
      sum(cost_esc_welcome_kit_shipment) as cost_esc_welcome_kit_shipment,
      sum(esc_count) as esc_count,
      sum(cost_logistics) ascost_logistics ,
	  sum(inkshipment) as inkshipment,
	  sum(welcomekitshipment) as welcomekitshipment,	  
      sum(cost_logistics_ink_shipment) as cost_logistics_ink_shipment,
      sum(cost_logistics_welcome_kit_shipment) as cost_logistics_welcome_kit_shipment,
      sum(shipping_speed_standard) as shipping_speed_standard,
      sum(shipping_speed_express) as shipping_speed_express,
      sum(cost_kit) as cost_kit, 
      sum(cost_kit_ink_shipment) as cost_kit_ink_shipment, 
      sum(cost_kit_welcome_kit_shipment) as cost_kit_welcome_kit_shipment,
      sum(cost_customer_support) as cost_customer_support, 
      sum(cust_support_count) as cust_support_count,
      sum(cost_free_months) as cost_freemonths,
      sum(freemonths_count) as freemonths_count
from fin_insights.stage_cost s
where year_month >= '2018-10-01'  and year_month <= '2023-11-01'                                                                                                                                                         
group by year_month,region_code
order by year_month desc, region_code asc             
      
--------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_new_enrollee()
 LANGUAGE plpgsql
AS $$ 
BEGIN 
	 drop table if exists fin_insights.stage_billing_cycle_base ;

CREATE TABLE fin_insights.stage_billing_cycle_base as 
SELECT 
      T2.subscription_id, 
      TRUNC(date_trunc('month', billing_cycle_end_time)) as Year_Month_bcet,
      T2.country_id as country_id,  
      T2.region_id as region_id, 
      T2.printer_model,  
	  TRUNC(date_trunc('month', billing_cycle_start_time)) as Year_Month_bcst, 
	  T2.replacement_index, 
	  T2.billing_cycle_pages_printed_number,  
	  T2.billing_cycle_free_pages_number, 
	  T2.billing_cycle_rollover_pages_number, 
	  T2.billing_cycle_overage_pages, 
	  T2.billing_cycle_free_month_type,  
	  T2.billing_cycle_plan_description,  
	  T2.plan_price_in_cents, 
	  T2.billing_cycle_plan_pages, 
	  T2.current_billing_cycle_overage_amount,  
	  T2.days_in_billing_cycle,
	  T2.billing_cycle_tax_amount_cents, 
	  T2.billing_cycle_pretax_amount_cents, 
	  T2.billing_cycle_pretax_amount_us_cents,
	  T2.billing_cycle_id
FROM  bi_fact_pii.iink_billing_base T2
where subscription_type in ('Billable Customer','HP Employees') and billing_cycle_end_time >= '2016-11-01'
and program_type = 'Instant Ink' ;

drop table if exists fin_insights.stage_customer_sub_base ; 

create table fin_insights.stage_customer_sub_base as  
select 
		subscription_id, 
		country_id, 
		region_id,
		subscription_date,  
		trunc(DATE_TRUNC('month', subscription_date )) as year_month_subscription,  
		enrolled_on_date, 
		trunc(DATE_TRUNC('month', enrolled_on_date )) as year_month_enroll,
		full_enrollment_date,                                                        -- new field added
        trunc(DATE_TRUNC('month', full_enrollment_date )) as year_month_full_enroll, -- new field added
        partial_enrollment_flag,                                                     -- new field added
		enrollment_id, 
		printer_serial_number, 
		platform, 
		enrollment_plan, 
	    current_plan, 
		p2_enrollment, 
		p2_category, 
--		printer_retailer_name,
		printer_model,
		hp_plus_activated_printer,
		hp_plus_eligible_printer
from  bi_fact_pii.iink_sub_base
where program_type = 'Instant Ink' and subscription_type in ('Billable Customer','HP Employees') ;

drop table if exists fin_insights.stage_cust_newenroll ; 

--  New Enrolles
create table fin_insights.stage_cust_newenroll as
select COALESCE (T1.subscription_id, T3.subscription_id) as subscription_id,
       COALESCE(T1.year_month,T3.year_month_enroll) AS year_month,
       COALESCE(T1.country_id,T3.country_id) AS country_id,
       t1.region_id,  
       t1.printer_model,  
       t1.Year_Month_bcst, 
       t1.replacement_index, 
       t1.billing_cycle_pages_printed_number,  
       t1.billing_cycle_free_pages_number, 
       t1.billing_cycle_rollover_pages_number, 
       t1.billing_cycle_overage_pages, 
       t1.billing_cycle_free_month_type,  
       t1.billing_cycle_plan_description,  
       t1.plan_price_in_cents, 
       t1.billing_cycle_plan_pages, 
       t1.current_billing_cycle_overage_amount,  
       t1.days_in_billing_cycle,
       T1.cost_ptb , 
       T1.cost_warranty,						
	   T1.cost_esc,
	   T1.cost_esc_ink_shipment,			
	   T1.cost_esc_welcome_kit_shipment,
	   T1.esc_count,
	   T1.cost_logistics,
	   T1.inkshipment,
	   T1.welcomekitshipment,
	   T1.cost_logistics_ink_shipment,
	   T1.cost_logistics_welcome_kit_shipment,
	   T1.shipping_speed_standard,
       T1.shipping_speed_express,
	   T1.cost_kit, 
	   T1.cost_kit_ink_shipment, 
	   T1.cost_kit_welcome_kit_shipment,
	   T1.cost_customer_support, 
       T1.cust_support_count,
       T1.cost_freemonths,
       T1.freemonths_count,
       t3.year_month_subscription,
       t3.enrolled_on_date, 
       t3.year_month_enroll,
       t3.full_enrollment_date,
       t3.year_month_full_enroll,
       t3.partial_enrollment_flag,
       t3.hp_plus_activated_printer,
       t3.hp_plus_eligible_printer 
from (
		select distinct 
			  COALESCE (T1.subscription_id, T2.subscription_id) as subscription_id,
			  T1.year_month,
			  T1.country_code as country_id,
			  t1.region_code as region_id,  
			  t2.printer_model,  
			  t2.Year_Month_bcst, 
			  t2.replacement_index, 
			  t2.billing_cycle_pages_printed_number,  
			  t2.billing_cycle_free_pages_number, 
			  t2.billing_cycle_rollover_pages_number, 
			  t2.billing_cycle_overage_pages, 
			  t2.billing_cycle_free_month_type,  
			  t2.billing_cycle_plan_description,  
			  t2.plan_price_in_cents, 
			  t2.billing_cycle_plan_pages, 
			  t2.current_billing_cycle_overage_amount,  
			  t2.days_in_billing_cycle,
			  T1.cost_ptb , 
			  T1.cost_warranty,						
			  T1.cost_esc,
			  T1.cost_esc_ink_shipment,			
			  T1.cost_esc_welcome_kit_shipment,
			  T1.esc_count,
			  T1.cost_logistics,
			  T1.inkshipment,
			  T1.welcomekitshipment,
			  T1.cost_logistics_ink_shipment,
			  T1.cost_logistics_welcome_kit_shipment,
			  T1.shipping_speed_standard,
			  T1.shipping_speed_express,
			  T1.cost_kit, 
			  T1.cost_kit_ink_shipment, 
			  T1.cost_kit_welcome_kit_shipment,
			  T1.cost_customer_support, 
			  T1.cust_support_count,
			  T1.cost_free_months as cost_freemonths,
			  T1.freemonths_count
		from fin_insights.stage_cost t1
		left join fin_insights.stage_billing_cycle_base T2
		ON    T1.subscription_id = T2.subscription_id 
		AND   T1.year_month =  T2.Year_Month_bcet 
	 ) T1
FULL OUTER JOIN fin_insights.stage_customer_sub_base T3
ON    T1.subscription_id = T3.subscription_id 
and   t1.year_month = t3.year_month_enroll ;


EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate) 
     VALUES (
             'NewEnrollee',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
             );

END;
$$

call fin_insights.sp_load_new_enrollee();

select count(1) from fin_insights.stage_cust_newenroll --380714790 --381249448 --381619133 --383697119 --383980546 --389928033 --404507178 --414386341 --428848542 --449433826 --459801466 --471926547 --482995808 --483254718 --494256303 --495887800 --521103391 --532300622 --547273245 --559895668 --573858213 --589694075 --623953773
--------------------------------------------------------------------------------------------------------------------------------------------------------------- 
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_bounty()
 LANGUAGE plpgsql
AS $$ 
BEGIN 
     drop table if exists GetReportingMonth_Bounty ;
        
--get reportingmonth for bounty
create temp table GetReportingMonth_Bounty as
select concat(date_part(year ,TRUNC(ADD_MONTHS(current_date ,-1))),date_part(month ,TRUNC(ADD_MONTHS(current_date ,-1)))) as ReportingMonth ;

drop table if exists fin_insights.stage_bounty ;      
-- Currency Conversion & Bountry Rate in USD
create table fin_insights.stage_bounty AS
SELECT 
      subscription_id, 
	  reporting_month, 
	  region_id, 
	  country_id,
	  bounty_rate, 
	  currency,
      case when currency = 'CHF' then chf 
           when currency = 'GBP' then gbp
           when currency = 'EUR' then eur
           when currency = 'SEK' then sek
           when currency = 'NOK' then nok
           when currency = 'DKK' then dkk
           when currency = 'CAD' then cad
           when currency = 'USD' then 1 end as currency_conversion, 
      case when currency = 'CHF' then ISNULL(bounty_rate,0) / currency_conversion
           when currency = 'GBP' then ISNULL(bounty_rate,0) * currency_conversion
           when currency = 'EUR' then ISNULL(bounty_rate,0) * currency_conversion
           when currency = 'USD' then ISNULL(bounty_rate,0) * currency_conversion
           when currency = 'SEK' then cast((ISNULL(bounty_rate,0) / currency_conversion) as float)
           when currency = 'NOK' then ISNULL(bounty_rate,0) / currency_conversion
           when currency = 'DKK' then ISNULL(bounty_rate,0) / currency_conversion
           when currency = 'CAD' then ISNULL(bounty_rate,0) / currency_conversion  end as bounty_rate_usd,
      TO_DATE(reporting_month ,'YYYYMMDD') as yy_mm_bounty 
FROM (
      SELECT 
	        subscription_id, 
			reporting_month, 
			region_id, 
			country_id, 
			enrolled_on_date, 
			final_plan, 
      --      printer_retailer_name_1, 
			compensation_type_b,
		    enrollment_type_b, 
			bounty_rate, 
			currency, 
		    year_month_enroll, 
		    month1,  
            year1,
            seqnum
       from (
              select subscription_id, 
			         reporting_month, 
			         region_id, 
			         country_id, 
			         enrolled_on_date, 
			         final_plan, 
              --     printer_retailer_name_1, 
			         compensation_type_b,
		             enrollment_type_b, 
			         bounty_rate, 
			         currency, 
		             trunc(DATE_TRUNC('month', enrolled_on_date )) as year_month_enroll, 
		             DATEPART(month, enrolled_on_date) as month1,  
                     DATEPART(YEAR, enrolled_on_date) as year1,
                     ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY REPORTING_MONTH desc) as seqnum
      FROM (
			SELECT  SUBSCRIPTION_ID,  REPORTING_MONTH, REGION_ID, COUNTRY_ID, ENROLLED_ON_DATE , FINAL_PLAN, --PRINTER_RETAILER_NAME AS PRINTER_RETAILER_NAME_1, 
			          COMPENSATION_TYPE AS COMPENSATION_TYPE_B, ENROLLMENT_TYPE AS ENROLLMENT_TYPE_B, BOUNTY_RATE, CURRENCY
			FROM (
                  SELECT * 
				  FROM RETAILER_COMPENSATION_SPECTRUM.RETAILER_RULES_BOUNTY_RESULT
			      WHERE REPORTING_MONTH >= '202005' and REPORTING_MONTH <= (select ReportingMonth from GetReportingMonth_Bounty)
				  AND subscription_type in ('Billable Customer', 'HP Employees') AND program_type = 'Instant Ink'
				  ) 
	        UNION  
			SELECT  SUBSCRIPTION_ID,  REPORTING_MONTH, REGION_ID, COUNTRY_ID, ENROLLED_ON_DATE , FINAL_PLAN, --PRINTER_RETAILER_NAME AS PRINTER_RETAILER_NAME_1, 
			          COMPENSATION_TYPE AS COMPENSATION_TYPE_B, ENROLLMENT_TYPE AS ENROLLMENT_TYPE_B, BOUNTY_RATE, CURRENCY
		    FROM ( 
			      SELECT * 
				  FROM retailer_compensation_history_spectrum.retailer_rules_bounty_result
			      WHERE reporting_month >= '201811' 
				  AND reporting_month <= '202004' 
				  and  subscription_type in ('Billable Customer', 'HP Employees')
				 )
	       )
           )WHERE seqnum=1
	  ) T1	  
LEFT JOIN  App_BM_Instant_Ink_Ops.base_hist_curr_val_v2 t2
ON t1.year1 = t2.year
AND t1.month1 = t2.[month] ;

drop table if exists fin_insights.stage_ptb_supl_cs_freemonths_Bounty ;

-- merge Bounty by subid
create table fin_insights.stage_ptb_supl_cs_freemonths_Bounty as
select 
      COALESCE( t1.subscription_id, t2.subscription_id) as subscription_id, 
      COALESCE( t1.year_month, t2.yy_mm_bounty) as year_month, 
      COALESCE( t1.region_id, t2.region_id) as region_id, 
      COALESCE( t1.country_id, t2.country_id) as country_id,
      t1.printer_model,  
      t1.Year_Month_bcst, 
      t1.replacement_index, 
      t1.billing_cycle_pages_printed_number,  
      t1.billing_cycle_free_pages_number, 
      t1.billing_cycle_rollover_pages_number, 
      t1.billing_cycle_overage_pages, 
      t1.billing_cycle_free_month_type,  
      t1.billing_cycle_plan_description,  
      t1.plan_price_in_cents, 
      t1.billing_cycle_plan_pages, 
      t1.current_billing_cycle_overage_amount,  
      t1.days_in_billing_cycle,
	  T1.cost_ptb , 
      T1.cost_warranty,						
	  T1.cost_esc,
	  T1.cost_esc_ink_shipment,			
	  T1.cost_esc_welcome_kit_shipment,
	  T1.esc_count,
	  T1.cost_logistics,
	  T1.inkshipment,
	  T1.welcomekitshipment,
	  T1.cost_logistics_ink_shipment,
	  T1.cost_logistics_welcome_kit_shipment,
	  T1.shipping_speed_standard,
      T1.shipping_speed_express,
	  T1.cost_kit, 
	  T1.cost_kit_ink_shipment, 
	  T1.cost_kit_welcome_kit_shipment,
	  T1.cost_customer_support, 
      T1.cust_support_count,
      T1.cost_freemonths,
      T1.freemonths_count,
      t1.year_month_subscription,
      t1.enrolled_on_date, 
      t1.year_month_enroll,
      t1.full_enrollment_date,
      t1.year_month_full_enroll,
      t1.partial_enrollment_flag,
      t1.hp_plus_activated_printer,
      t1.hp_plus_eligible_printer,
      t2.bounty_rate, 
      t2.currency,  
      t2.currency_conversion, 
      t2.bounty_rate_usd
from fin_insights.stage_cust_newenroll t1
FULL OUTER JOIN fin_insights.stage_bounty T2
ON    t1.subscription_id = t2.subscription_id 
and   t1.year_month = t2.yy_mm_bounty ;
	
EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate) 
     VALUES (
             'Bounty',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
             );

END;
$$

call fin_insights.sp_load_bounty();

select count(1) from fin_insights.stage_ptb_supl_cs_freemonths_Bounty; --381329168 --381698853 --383777055 --384060482 --390008453 --404588579 --414467944 --428930304 --438582958 --449517765--459885499 --472010730 --483080041 --483339171 --494340751 --495972242 --521188223 --532385654 --547358403 --559982233 --573944531 --589780628 --601166440 --612379370 -624040168 --651447722
----------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_revshare()
 LANGUAGE plpgsql
AS $$ 
BEGIN
      drop table if exists GetReportingMonth_RevShare ;

--get reportingmonth for revshare
create temp table GetReportingMonth_RevShare as
select concat(date_part(year ,TRUNC(ADD_MONTHS(current_date ,-1))),date_part(month ,TRUNC(ADD_MONTHS(current_date ,-1)))) as ReportingMonth ;

drop table if exists fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev ;

--get enrollment type and revshare rate
create table fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev as
with t2 as (
            select subscription_id, 
			TRUNC(date_trunc('month', billing_cycle_end_time)) as Year_Month_bcet, 
			country_id,   
			enrollment_type,
            revshare_rate,
			seqnum from (
                         select *,
                         ROW_NUMBER() OVER (PARTITION BY billing_cycle_id ORDER BY reporting_month desc) as seqnum
                         from (    
						       select  billing_cycle_id, reporting_month, subscription_id, billing_cycle_end_time, country_id, --compensation_retailer_name,printer_retailer_name 
							   retailer_kit_printer, enrollment_type, revshare_rate, compensation_type 
							   from ( 
							         select * from retailer_compensation_spectrum.retailer_rules_revshare_result
                                     where reporting_month >= '202005' and reporting_month <= (select ReportingMonth from GetReportingMonth_RevShare) and program_type = 'Instant Ink'
									)
                               Union  
                               select  billing_cycle_id, reporting_month, subscription_id, billing_cycle_end_time, country_id, --compensation_retailer_name, printer_retailer_name
							   retailer_kit_printer, enrollment_type, revshare_rate, compensation_type 
							   from ( 
							         select * from retailer_compensation_history_spectrum.retailer_rules_revshare_result
                                     WHERE reporting_month >= '201811' 
                                     AND reporting_month <= '202004' 
									 ) 
                               )
					    )
			where seqnum=1
		   )
select t1.subscription_id,
       t1.year_month,
       t1.region_id,
       t1.country_id,
       t1.printer_model,  
       t1.Year_Month_bcst, 
       t1.replacement_index, 
       t1.billing_cycle_pages_printed_number,  
       t1.billing_cycle_free_pages_number, 
       t1.billing_cycle_rollover_pages_number, 
       t1.billing_cycle_overage_pages, 
       t1.billing_cycle_free_month_type,  
       t1.billing_cycle_plan_description,  
       t1.plan_price_in_cents, 
       t1.billing_cycle_plan_pages, 
       t1.current_billing_cycle_overage_amount,  
       t1.days_in_billing_cycle,
	   T1.cost_ptb , 
       T1.cost_warranty,						
	   T1.cost_esc,
	   T1.cost_esc_ink_shipment,			
	   T1.cost_esc_welcome_kit_shipment,
	   T1.esc_count,
	   T1.cost_logistics,
	   T1.inkshipment,
	   T1.welcomekitshipment,
	   T1.cost_logistics_ink_shipment,
	   T1.cost_logistics_welcome_kit_shipment,
	   T1.shipping_speed_standard,
       T1.shipping_speed_express,
	   T1.cost_kit, 
	   T1.cost_kit_ink_shipment, 
	   T1.cost_kit_welcome_kit_shipment,
	   T1.cost_customer_support, 
       T1.cust_support_count,
       T1.cost_freemonths,
       T1.freemonths_count,
       t1.year_month_subscription,
       t1.enrolled_on_date, 
       t1.year_month_enroll,
       t1.full_enrollment_date,
       t1.year_month_full_enroll,
       t1.partial_enrollment_flag,
       t1.hp_plus_activated_printer,
       t1.hp_plus_eligible_printer,
       t1.bounty_rate, 
       t1.currency,  
       t1.currency_conversion, 
       t1.bounty_rate_usd,   
	   t2.enrollment_type, 
	   t2.revshare_rate
from fin_insights.stage_ptb_supl_cs_freemonths_Bounty t1
left join t2
on t1.subscription_id = t2.subscription_id
and  T1.year_month =  T2.Year_Month_bcet ;
 					
--*************************************************************************************************
-- running Enrollment date and subsription date 
--*************************************************************************************************
drop table if exists fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev_cust2 ;

create table  fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev_cust2 as
select t1.subscription_id,
       t1.year_month,
       t1.region_id,
       t1.country_id,
       t1.printer_model,  
       t1.Year_Month_bcst, 
       t1.replacement_index, 
       t1.billing_cycle_pages_printed_number,  
       t1.billing_cycle_free_pages_number, 
       t1.billing_cycle_rollover_pages_number, 
       t1.billing_cycle_overage_pages, 
       t1.billing_cycle_free_month_type,  
       t1.billing_cycle_plan_description,  
       t1.plan_price_in_cents, 
       t1.billing_cycle_plan_pages, 
       t1.current_billing_cycle_overage_amount,  
       t1.days_in_billing_cycle,
	   T1.cost_ptb , 
       T1.cost_warranty,						
	   T1.cost_esc,
	   T1.cost_esc_ink_shipment,			
	   T1.cost_esc_welcome_kit_shipment,
	   T1.esc_count,
	   T1.cost_logistics,
	   T1.inkshipment,
	   T1.welcomekitshipment,
	   T1.cost_logistics_ink_shipment,
	   T1.cost_logistics_welcome_kit_shipment,
	   T1.shipping_speed_standard,
       T1.shipping_speed_express,
	   T1.cost_kit, 
	   T1.cost_kit_ink_shipment, 
	   T1.cost_kit_welcome_kit_shipment,
	   T1.cost_customer_support, 
       T1.cust_support_count,
       T1.cost_freemonths,
       T1.freemonths_count,
       t1.year_month_subscription,
       t1.enrolled_on_date, 
       t1.year_month_enroll,
       t1.full_enrollment_date,
       t1.year_month_full_enroll,
       t1.partial_enrollment_flag,       
       t1.hp_plus_activated_printer,
       t1.hp_plus_eligible_printer,
       t1.bounty_rate, 
       t1.currency,  
       t1.currency_conversion, 
       t1.bounty_rate_usd,   
	   t1.enrollment_type, 
	   t1.revshare_rate,
	   t2.year_month_subscription as year_month_subscription_1,
	   t2.year_month_enroll as year_month_enroll_1,
	   t2.year_month_full_enroll as year_month_full_enroll_1
from fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev t1
left join (select * from (
                          select 
		                        subscription_id,
		                        year_month_subscription,
		                        year_month_enroll,
		                        year_month_full_enroll,
                                ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY subscription_date asc) as seqnum_oldest
                          from fin_insights.stage_customer_sub_base)
where seqnum_oldest= 1
          ) t2
ON  T1.subscription_id = T2.subscription_id ;
	
EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate) 
     VALUES (
             'RevShare',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
             );

END;
$$

call fin_insights.sp_load_revshare();
select count(1) from fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev; --383777533 --384060960 --390009033 --414468462 --428930820 --438583474 --449518281 --483080559 --521188743 --532386176 --
select count(1) from fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev_cust2; --380794307 --381329168 --381698853 --383777533 --390009033 -- 404589163 --414468462 --428930820 --449518281 --459886015 --472011248 --483080559 --483339689 --494341269 --495972760 --532386176 --547358925 --559982757 --573945059 --589781156 --601166968 --612379898 --624040696

--------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_p1_p2_qrt()
 LANGUAGE plpgsql
AS $$ 
BEGIN 
      drop table if exists fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev_cust3 ;

create table fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev_cust3 as 
select t1.subscription_id,
       t1.year_month,
       t1.region_id,
       t1.country_id,
       t1.printer_model,  
       t1.Year_Month_bcst, 
       t1.replacement_index, 
       t1.billing_cycle_pages_printed_number,  
       t1.billing_cycle_free_pages_number, 
       t1.billing_cycle_rollover_pages_number, 
       t1.billing_cycle_overage_pages, 
       t1.billing_cycle_free_month_type,  
       t1.billing_cycle_plan_description,  
       t1.plan_price_in_cents, 
       t1.billing_cycle_plan_pages, 
       t1.current_billing_cycle_overage_amount,  
       t1.days_in_billing_cycle,
	   T1.cost_ptb , 
       T1.cost_warranty,						
	   T1.cost_esc,
	   T1.cost_esc_ink_shipment,			
	   T1.cost_esc_welcome_kit_shipment,
	   T1.esc_count,
	   T1.cost_logistics,
	   T1.inkshipment,
	   T1.welcomekitshipment,
	   T1.cost_logistics_ink_shipment,
	   T1.cost_logistics_welcome_kit_shipment,
	   T1.shipping_speed_standard,
       T1.shipping_speed_express,
	   T1.cost_kit, 
	   T1.cost_kit_ink_shipment, 
	   T1.cost_kit_welcome_kit_shipment,
	   T1.cost_customer_support, 
       T1.cust_support_count,
       T1.cost_freemonths,
       T1.freemonths_count,
       t1.year_month_subscription,
       t1.enrolled_on_date, 
       t1.year_month_enroll,
       t1.full_enrollment_date,
       t1.year_month_full_enroll,
       t1.partial_enrollment_flag,       
       t1.hp_plus_activated_printer,
       t1.hp_plus_eligible_printer,
       t1.bounty_rate, 
       t1.currency,  
       t1.currency_conversion, 
       t1.bounty_rate_usd,   
	   t1.enrollment_type, 
	   t1.revshare_rate,
	   t1.year_month_subscription_1,
	   t1.year_month_enroll_1,
       t1.year_month_full_enroll_1,	   
	   t2.enrollment_id, 
	   t2.subscription_date,  
	   t2.printer_serial_number, 
	   t2.platform, 
	   t2.enrollment_plan, 
	   t2.current_plan,  
	   t2.p2_enrollment, 
	   t2.p2_category 
--	   t2.printer_retailer_name
from fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev_cust2 t1
left join ( select * from (
                           select  subscription_id, 
						           subscription_date, 
								   enrollment_id, 
								   printer_serial_number, 
								   platform, 
								   enrollment_plan, 
								   current_plan,   
								   p2_enrollment, 
								   p2_category, 
							--	   printer_retailer_name,
                                   ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY subscription_date desc) as seqnum_latest  
                          from fin_insights.stage_customer_sub_base)
            where seqnum_latest = 1 
          ) t2
ON  T1.subscription_id = T2.subscription_id ;

--*************************************************************************************************
--  /*fill missing Country and printer name and include Qrt from dim_date_time*/   
--*************************************************************************************************
drop table if exists fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev_cust4 ;

create table fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev_cust4 as 
select t1.subscription_id,
       t1.year_month,
       t1.region_id,
       t1.country_id,
       t1.printer_model,  
       t1.Year_Month_bcst, 
       t1.replacement_index, 
       t1.billing_cycle_pages_printed_number,  
       t1.billing_cycle_free_pages_number, 
       t1.billing_cycle_rollover_pages_number, 
       t1.billing_cycle_overage_pages, 
       t1.billing_cycle_free_month_type,  
       t1.billing_cycle_plan_description,  
       t1.plan_price_in_cents, 
       t1.billing_cycle_plan_pages, 
       t1.current_billing_cycle_overage_amount,  
       t1.days_in_billing_cycle,
	   T1.cost_ptb , 
       T1.cost_warranty,						
	   T1.cost_esc,
	   T1.cost_esc_ink_shipment,			
	   T1.cost_esc_welcome_kit_shipment,
	   T1.esc_count,
	   T1.cost_logistics,
	   T1.inkshipment,
	   T1.welcomekitshipment,
	   T1.cost_logistics_ink_shipment,
	   T1.cost_logistics_welcome_kit_shipment,
	   T1.shipping_speed_standard,
       T1.shipping_speed_express,
	   T1.cost_kit, 
	   T1.cost_kit_ink_shipment, 
	   T1.cost_kit_welcome_kit_shipment,
	   T1.cost_customer_support, 
       T1.cust_support_count,
       T1.cost_freemonths,
       T1.freemonths_count,
       t1.year_month_subscription,
       t1.enrolled_on_date, 
       t1.year_month_enroll,
       t1.full_enrollment_date,
       t1.year_month_full_enroll,
       t1.partial_enrollment_flag,       
       t1.hp_plus_activated_printer,
       t1.hp_plus_eligible_printer,
       t1.bounty_rate, 
       t1.currency,  
       t1.currency_conversion, 
       t1.bounty_rate_usd,   
	   t1.enrollment_type, 
	   t1.revshare_rate,
	   t1.year_month_subscription_1,
	   t1.year_month_enroll_1,
	   t1.year_month_full_enroll_1,
	   t1.enrollment_id, 
	   t1.subscription_date,  
	   t1.printer_serial_number, 
	   t1.platform, 
	   t1.enrollment_plan, 
	   t1.current_plan,  
	   t1.p2_enrollment, 
	   t1.p2_category,  
  --   t1.printer_retailer_name,
	   t2.country_id_m,  
	   t2.printer_model_m,
	   t3.fiscal_year_quarter_code
from fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev_cust3 t1
left join ( select * from (
                           select subscription_id,  
						          country_id as country_id_m, 
								  printer_model as printer_model_m,
                                  ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY subscription_date desc) as seqnum_latest  
                           from fin_insights.stage_customer_sub_base)
            where seqnum_latest = 1
		  ) t2 on
t1.subscription_id = t2.subscription_id
left Join app_instant_ink_bi_dim.dim_date_time t3 on 
t1.Year_Month = t3.calendar_date ;

--*************************************************************************************************
--  /**creating flag**/   
--*************************************************************************************************

drop table if exists fin_insights.stage_ptb_supl_cs_freemonths_Bounty4 ;

create table fin_insights.stage_ptb_supl_cs_freemonths_Bounty4 as 
select subscription_id,
       year_month,
       region_id,
       country_id,
       printer_model,  
       Year_Month_bcst, 
       replacement_index, 
       billing_cycle_pages_printed_number,  
       billing_cycle_free_pages_number, 
       billing_cycle_rollover_pages_number, 
       billing_cycle_overage_pages, 
       billing_cycle_free_month_type,  
       billing_cycle_plan_description,  
       plan_price_in_cents, 
       billing_cycle_plan_pages, 
       current_billing_cycle_overage_amount,  
       days_in_billing_cycle,
	   cost_ptb , 
       cost_warranty,						
	   cost_esc,
	   cost_esc_ink_shipment,			
	   cost_esc_welcome_kit_shipment,
	   esc_count,
	   cost_logistics,
	   inkshipment,
	   welcomekitshipment,
	   cost_logistics_ink_shipment,
	   cost_logistics_welcome_kit_shipment,
	   shipping_speed_standard,
       shipping_speed_express,
	   cost_kit, 
	   cost_kit_ink_shipment, 
	   cost_kit_welcome_kit_shipment,
	   cost_customer_support, 
       cust_support_count,
       cost_freemonths,
       freemonths_count,
       year_month_subscription,
       enrolled_on_date, 
       year_month_enroll,
       full_enrollment_date,
       year_month_full_enroll,
       partial_enrollment_flag,
       hp_plus_activated_printer,
       hp_plus_eligible_printer,
       bounty_rate, 
       currency,  
       currency_conversion, 
       bounty_rate_usd,   
	   enrollment_type, 
	   revshare_rate,
	   year_month_subscription_1,
	   year_month_enroll_1,
	   year_month_full_enroll_1,
	   enrollment_id, 
	   subscription_date,  
	   printer_serial_number, 
	   platform, 
	   enrollment_plan, 
	   current_plan,  
	   p2_enrollment, 
	   p2_category,  
--	   printer_retailer_name,
	   country_id_m,  
	   printer_model_m,
	   fiscal_year_quarter_code,
	 -- CASE when year_month < '2020-04-01' then  'pre_covid'
     -- when year_month >= '2020-04-01' then  'covid' end as covid,
       case when country_id is null then country_id_m else country_id end as country_id1,
       case when printer_model is null then printer_model_m else printer_model end as printer_model1,
       case when hp_plus_activated_printer = 'HP+ Activated' then 'HP+' else 'NonHP+' end as hp_plus_activated_printer1, --NEWLY ADDED
       DATEDIFF(month,  TRUNC(year_month_subscription_1), TRUNC(year_month)) as age_month,
       case when billing_cycle_plan_pages >= billing_cycle_pages_printed_number then 'within_plan_pages'
            when billing_cycle_plan_pages < billing_cycle_pages_printed_number then 'more_than_plan_pages'  
            when billing_cycle_pages_printed_number is null then 'no_pages'end as Pages_printed,      
       case when billing_cycle_pages_printed_number is null  then 'no_print' 
            when billing_cycle_pages_printed_number <= (0.2* ISNULL(billing_cycle_plan_pages, 0))  then '<20' 
            when billing_cycle_pages_printed_number between (0.2*ISNULL(billing_cycle_plan_pages, 0))  and  (0.4*ISNULL(billing_cycle_plan_pages, 0)) then '20-40'   
            when billing_cycle_pages_printed_number   between (0.4*ISNULL(billing_cycle_plan_pages, 0)) and  (0.6*ISNULL(billing_cycle_plan_pages, 0)) then '40-60'  
            when billing_cycle_pages_printed_number   between (0.6*ISNULL(billing_cycle_plan_pages, 0)) and   (0.8*ISNULL(billing_cycle_plan_pages, 0)) then '60-80'
            when billing_cycle_pages_printed_number    between (0.8*ISNULL(billing_cycle_plan_pages, 0)) and  (ISNULL(billing_cycle_plan_pages, 0)) then '80-100'
            when billing_cycle_pages_printed_number    between (ISNULL(billing_cycle_plan_pages, 0)) and  (1.5*ISNULL(billing_cycle_plan_pages, 0)) then '100-150'
            when billing_cycle_pages_printed_number    between (1.5*ISNULL(billing_cycle_plan_pages, 0)) and  (2.0*ISNULL(billing_cycle_plan_pages, 0)) then '150-200'
            when billing_cycle_pages_printed_number > (2.0*ISNULL(billing_cycle_plan_pages, 0)) then '>200'  end as plan_page_utilization_percent,
       case when billing_cycle_plan_description is null then lead(billing_cycle_plan_description) over(PARTITION BY subscription_id ORDER BY year_month ) else billing_cycle_plan_description end as billing_cycle_plan_description1,
       case when plan_price_in_cents is null then lead(plan_price_in_cents) over(PARTITION BY subscription_id ORDER BY year_month ) else plan_price_in_cents end as plan_price_in_cents1,
       case when billing_cycle_plan_pages is null then lead(billing_cycle_plan_pages) over(PARTITION BY subscription_id ORDER BY year_month ) else billing_cycle_plan_pages end as billing_cycle_plan_pages1,
       case when age_month  <= 12  then 'Upto_12' 
            when age_month is null then  'Upto_12' 
            when age_month between  13 and 24 then '13-24' 
            when age_month between  25 and  36 then '25-36'
            when age_month between  37 and 48 then '37-48'
            when age_month between  49 and 60 then '49-60'          
            when age_month  > 60 then '>60'  end as age_group,           
       sum(freemonths_count) over(PARTITION by subscription_id order by year_month rows unbounded preceding) as freemonths_count_subid, 
       sum(esc_count) over(PARTITION by subscription_id order by year_month rows unbounded preceding ) as Kits_count_subid, 
       sum(billing_cycle_pages_printed_number) over(PARTITION by subscription_id order by year_month rows unbounded preceding ) as total_pages_printed_ltd,
      --(sum(billing_cycle_pages_printed_number) over (PARTITION by subscription_id order by year_month rows unbounded preceding ))/Kits_count_subid as pages_kits_ratio,  -- pages per kit
      --age_month/Kits_count_subid as  months_kits_ratio,
      -- case when year_month_subscription_1 < '2020-04-01' then 'pre_covid_subscription'
           -- when year_month_subscription_1 >=  '2020-04-01' then 'covid_subscription' end as covid_subscription,
       case when enrolled_on_date is not null and subscription_date is null then 'not_subscribed' else 'subscribed' end as enrolled_not_subscribed,
       case when full_enrollment_date is not null and subscription_date is null then 'not_subscribed' else 'subscribed' end as full_enrolled_not_subscribed, --new added field
       case when billing_cycle_plan_pages1 = 15 then '15_page_plan' else 'not_15_page_plan' end as free_page_plan,
       lag(billing_cycle_plan_pages) over (partition by subscription_id order by year_month) as lag_billing_cycle_plan_pages,
       case when lag_billing_cycle_plan_pages is null then billing_cycle_plan_pages else lag_billing_cycle_plan_pages end as lag_billing_cycle_plan_pages_null,
       case when billing_cycle_plan_pages = lag_billing_cycle_plan_pages_null then 0 else 1 end as flag,
       case when replacement_index is not null then rank() over (PARTITION by subscription_id, replacement_index order by year_month ) end as rank_replacementindex
from fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev_cust4 ;

EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate) 
     VALUES (
             'P1_P2_Qrt',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
             );

END;
$$


call fin_insights.sp_load_p1_p2_qrt();
select count(1) from fin_insights.stage_ptb_supl_cs_freemonths_Bounty4 --380794307 --381329168 --381698853 --383777533 --384060960 --390009033 --404589163 --414468462 --428930820 --438583474 --449518281 --459886015 --472011248 --483080559 --483339689 --494341269 --495972760 --521188743 --532386176 --547358925 --559982757 --573945059 --589781156 --601166968 --612379898 --624040696--651448250
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_revenue()
 LANGUAGE plpgsql
AS $$ 
BEGIN 
	  drop table if exists fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue ;

create table fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue as 
select t1.subscription_id,
       t1.year_month,
       t1.region_id,
       t1.country_id,
       t1.printer_model,  
       t1.Year_Month_bcst, 
       t1.replacement_index, 
       t1.billing_cycle_pages_printed_number,  
       t1.billing_cycle_free_pages_number, 
       t1.billing_cycle_rollover_pages_number, 
       t1.billing_cycle_overage_pages, 
       t1.billing_cycle_free_month_type,  
       t1.billing_cycle_plan_description,  
       t1.plan_price_in_cents, 
       t1.billing_cycle_plan_pages, 
       t1.current_billing_cycle_overage_amount,  
       t1.days_in_billing_cycle,
	   T1.cost_ptb , 
       T1.cost_warranty,						
	   T1.cost_esc,
	   T1.cost_esc_ink_shipment,			
	   T1.cost_esc_welcome_kit_shipment,
	   T1.esc_count,
	   T1.cost_logistics,
	   T1.inkshipment,
	   T1.welcomekitshipment,
	   T1.cost_logistics_ink_shipment,
	   T1.cost_logistics_welcome_kit_shipment,
	   T1.shipping_speed_standard,
       T1.shipping_speed_express,
	   T1.cost_kit, 
	   T1.cost_kit_ink_shipment, 
	   T1.cost_kit_welcome_kit_shipment,
	   T1.cost_customer_support, 
       T1.cust_support_count,
       T1.cost_freemonths,
       T1.freemonths_count,
       t1.year_month_subscription,
       t1.enrolled_on_date, 
       t1.year_month_enroll,
       t1.full_enrollment_date,
       t1.year_month_full_enroll,
       t1.partial_enrollment_flag,       
       t1.hp_plus_activated_printer,
       t1.hp_plus_eligible_printer,
       t1.bounty_rate, 
       t1.currency,  
       t1.currency_conversion, 
       t1.bounty_rate_usd,   
	   t1.enrollment_type, 
	   t1.revshare_rate,
	   t1.year_month_subscription_1,
	   t1.year_month_enroll_1,
       t1.year_month_full_enroll_1,	   
	   t1.enrollment_id, 
	   t1.subscription_date,  
	   t1.printer_serial_number, 
	   t1.platform, 
	   t1.enrollment_plan, 
	   t1.current_plan,  
	   t1.p2_enrollment, 
	   t1.p2_category,  
--	   t1.printer_retailer_name,
	   t1.country_id_m,  
	   t1.printer_model_m,
	   t1.fiscal_year_quarter_code,
	   t1.country_id1,
	   t1.printer_model1,
	   t1.hp_plus_activated_printer1,
	   t1.age_month,
	   t1.Pages_printed,
	   t1.plan_page_utilization_percent,
	   t1.billing_cycle_plan_description1,
	   t1.plan_price_in_cents1,
	   t1.billing_cycle_plan_pages1,
	   t1.age_group,
	   t1.freemonths_count_subid,
	   t1.Kits_count_subid,
	   t1.total_pages_printed_ltd,
	   t1.enrolled_not_subscribed,
	   t1.full_enrolled_not_subscribed,	   
	   t1.free_page_plan,
	   t1.lag_billing_cycle_plan_pages,
	   t1.lag_billing_cycle_plan_pages_null,
	   t1.flag,
	   t1.rank_replacementindex,
	   t2.billing_cycle_tax_amount_cents, 
	   t2.billing_cycle_pretax_amount_cents, 
	   t2.billing_cycle_pretax_amount_us_cents,
       t2.billing_cycle_id
from fin_insights.stage_ptb_supl_cs_freemonths_Bounty4 t1
Left JOIN fin_insights.stage_billing_cycle_base t2
ON  T1.subscription_id = T2.subscription_id 
AND T1.year_month =  T2.Year_Month_bcet ;

--*************************************************************************************************
--  /**Revenue EMEA**/   
--*************************************************************************************************
drop table if exists fin_insights.stage_revenue_emea ;

create table fin_insights.stage_revenue_emea as 
select 
      year_month, 
	  region_id, 
	  country_id, 
	  billing_cycle_plan_pages1, 
	  plan_price_in_cents1 , 
      current_billing_cycle_overage_amount , 
	  billing_cycle_tax_amount_cents, 
	  billing_cycle_pretax_amount_cents, 
	  billing_cycle_pretax_amount_us_cents, 
	  count(subscription_id) as count1
from  fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue
where region_id = 'EMEA' 
and current_billing_cycle_overage_amount is null 
and country_id is not null
and billing_cycle_plan_pages1 is not null
and plan_price_in_cents1 is not null
and  billing_cycle_tax_amount_cents is not null
and  billing_cycle_pretax_amount_cents is  not null
and  billing_cycle_pretax_amount_us_cents is not null
group by year_month, region_id,country_id, billing_cycle_plan_pages1, plan_price_in_cents1,  
current_billing_cycle_overage_amount, billing_cycle_tax_amount_cents, billing_cycle_pretax_amount_cents, billing_cycle_pretax_amount_us_cents
order by year_month, country_id, plan_price_in_cents1, count1 ;

drop table if exists fin_insights.stage_revenue_emea1 ;

create table fin_insights.stage_revenue_emea1 as 
select * from (
               select year_month, 
			          region_id, 
			          country_id, 
			          billing_cycle_plan_pages1, 
			          plan_price_in_cents1 , 
                      current_billing_cycle_overage_amount , 
			          billing_cycle_tax_amount_cents, 
			          billing_cycle_pretax_amount_cents, 
			          billing_cycle_pretax_amount_us_cents,  
			          count1,
                      rank() over (partition by year_month, country_id, billing_cycle_plan_pages1 order by count1 desc ) as rank_order,
                     (billing_cycle_tax_amount_cents +billing_cycle_pretax_amount_cents) as billing_tax_amount 
			   from fin_insights.stage_revenue_emea
               where plan_price_in_cents1 = billing_tax_amount 
               order by year_month, country_id, billing_cycle_plan_pages1
			  )
where rank_order = 1
order by year_month, country_id, plan_price_in_cents1 ;

--*************************************************************************************************
--  /**Revenue EMEA + Other Regions**/   
--*************************************************************************************************

drop table if exists fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue_emea ;

create table fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue_emea as 
select t1.subscription_id,
       t1.year_month,
       t1.region_id,
       t1.country_id,
       t1.printer_model,  
       t1.Year_Month_bcst, 
       t1.replacement_index, 
       t1.billing_cycle_pages_printed_number,  
       t1.billing_cycle_free_pages_number, 
       t1.billing_cycle_rollover_pages_number, 
       t1.billing_cycle_overage_pages, 
       t1.billing_cycle_free_month_type,  
       t1.billing_cycle_plan_description,  
       t1.plan_price_in_cents, 
       t1.billing_cycle_plan_pages, 
       t1.current_billing_cycle_overage_amount,  
       t1.days_in_billing_cycle,
	   T1.cost_ptb , 
       T1.cost_warranty,						
	   T1.cost_esc,
	   T1.cost_esc_ink_shipment,			
	   T1.cost_esc_welcome_kit_shipment,
	   T1.esc_count,
	   T1.cost_logistics,
	   T1.inkshipment,
	   T1.welcomekitshipment,
	   T1.cost_logistics_ink_shipment,
	   T1.cost_logistics_welcome_kit_shipment,
	   T1.shipping_speed_standard,
       T1.shipping_speed_express,
	   T1.cost_kit, 
	   T1.cost_kit_ink_shipment, 
	   T1.cost_kit_welcome_kit_shipment,
	   T1.cost_customer_support, 
       T1.cust_support_count,
       T1.cost_freemonths,
       T1.freemonths_count,
       t1.year_month_subscription,
       t1.enrolled_on_date, 
       t1.year_month_enroll,
       t1.full_enrollment_date,
       t1.year_month_full_enroll,
       t1.partial_enrollment_flag,       
       t1.hp_plus_activated_printer,
       t1.hp_plus_eligible_printer,
       t1.bounty_rate, 
       t1.currency,  
       t1.currency_conversion, 
       t1.bounty_rate_usd,   
	   t1.enrollment_type, 
	   t1.revshare_rate,
	   t1.year_month_subscription_1,
	   t1.year_month_enroll_1,
       t1.year_month_full_enroll_1,	   
	   t1.enrollment_id, 
	   t1.subscription_date,  
	   t1.printer_serial_number, 
	   t1.platform, 
	   t1.enrollment_plan, 
	   t1.current_plan,  
	   t1.p2_enrollment, 
	   t1.p2_category,  
--	   t1.printer_retailer_name,
	   t1.country_id_m,  
	   t1.printer_model_m,
	   t1.fiscal_year_quarter_code,
	   t1.country_id1,
	   t1.printer_model1,
	   t1.hp_plus_activated_printer1,
	   t1.age_month,
	   t1.Pages_printed,
	   t1.plan_page_utilization_percent,
	   t1.billing_cycle_plan_description1,
	   t1.plan_price_in_cents1,
	   t1.billing_cycle_plan_pages1,
	   t1.age_group,
	   t1.freemonths_count_subid,
	   t1.Kits_count_subid,
	   t1.total_pages_printed_ltd,
	   t1.enrolled_not_subscribed,
       t1.full_enrolled_not_subscribed,	   
	   t1.free_page_plan,
	   t1.lag_billing_cycle_plan_pages,
	   t1.lag_billing_cycle_plan_pages_null,
	   t1.flag,
	   t1.rank_replacementindex,
	   t1.billing_cycle_tax_amount_cents, 
	   t1.billing_cycle_pretax_amount_cents, 
	   t1.billing_cycle_pretax_amount_us_cents,
	   t1.billing_cycle_id,
	   t2.billing_cycle_pretax_amount_us_cents as EMEA_pretax_amount_us_cents
from fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue t1  
left join fin_insights.stage_revenue_emea1 t2
ON  T1.year_month = T2.year_month 
and t1. country_id = t2.country_id
and t1.billing_cycle_plan_pages1 = t2.billing_cycle_plan_pages1 ;

EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate) 
     VALUES (
             'Revenue',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
             );

END;
$$


call fin_insights.sp_load_revenue();
select count(1) from fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue_emea --380794307 --381330890 --381700575 --383779717 --384063144 --390011319 --404591474 --414470764 --428933108 --438585747 --449520588 --459888343 --472013534 --483082852 --483342043 --494343754 --521191755 --532389512 --547362570 --559986488 --573948220 --589784223 --601170649 --612442440 --624043983 --651450802
-----------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_other_cost()
 LANGUAGE plpgsql
AS $$ 
BEGIN 
	  drop table if exists fin_insights.stage_total_cost_sales ;

create table fin_insights.stage_total_cost_sales
(
    month_year VARCHAR(22) NOT NULL,
    region VARCHAR(22) NOT NULL,
    Total_cost_sales decimal(13,3) NOT NULL
);

copy fin_insights.stage_total_cost_sales
from 's3://instant-ink-finance/team-iifin/profitability/total_cost_sales.csv'
iam_role 'arn:aws:iam::828361281741:role/team-iifin'
ignoreheader as 1
maxerror 10
emptyasnull
blanksasnull
delimiter as ',';

delete  from fin_insights.total_cost_sales
where (month_year, region)  
in (select case when left(s.month_year,1) = 0 then right(s.month_year,6) else s.month_year end as month_year, s.region
from  fin_insights.total_cost_sales f, fin_insights.stage_total_cost_sales s
where f.month_year = s.month_year and UPPER(f.region) = UPPER(s.region)) ;

insert into fin_insights.total_cost_sales
(month_year, region,Total_cost_sales)
select * from (select distinct case when left(s.month_year,1) = 0 then right(s.month_year,6) else s.month_year end as month_year, s.region, s.Total_cost_sales
from fin_insights.stage_total_cost_sales s ) k
where not exists (	select top 1 * 
						from fin_insights.total_cost_sales f 
						where f.month_year = k.month_year
						and f.region = k.region
					) ;				
				
drop table if exists fin_insights.stage_Total_cost_sales_red ;

create table fin_insights.stage_Total_cost_sales_red as 
select *, 
       DATEPART(YEAR, year_month) as year1, 
	   DATEPART(MONTH, year_month) as month1, 
	   month1|| '_' ||year1 as Month_year
from (
      select  year_month,
	          region_id, 
			  ptb_cost_subid , 
			  warranty_cost_subid,  
			  esc_cost_subid, 
              logistic_cost_subid , 
			  kitting_cost_subid , 
			  cust_support_cost_subid,
              ISNULL(ptb_cost_subid,0)+ISNULL(warranty_cost_subid,0) + ISNULL(esc_cost_subid,0) + ISNULL(logistic_cost_subid,0) + ISNULL(kitting_cost_subid,0) + ISNULL(cust_support_cost_subid,0) as Total_cost_redshift		  
      from (
            select year_month,
			       region_id, 
			       sum(cost_ptb) as ptb_cost_subid , 
				   sum(cost_warranty) as warranty_cost_subid,  
				   sum(cost_esc) as esc_cost_subid, 
                   sum(cost_logistics) as logistic_cost_subid , 
				   sum(cost_kit) as kitting_cost_subid , 
				   sum(cost_customer_support) as cust_support_cost_subid 
from fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue_emea
group by year_month, region_id
order by  region_id, year_month
           ) 
     ) ;
	 
drop table if exists fin_insights.stage_Total_cost_sales_red2 ;
	 
create table fin_insights.stage_Total_cost_sales_red2 as  
select *,
		case when Total_cost_redshift > Total_cost_sales  then Total_cost_redshift else Total_cost_sales end as Total_cost_sales_corrected, 
		(Total_cost_sales_corrected - Total_cost_redshift) as other_cost from
( 
select       
              t1.year_month,
	          t1.region_id, 
			  t1.ptb_cost_subid , 
			  t1.warranty_cost_subid,  
			  t1.esc_cost_subid, 
              t1.logistic_cost_subid , 
			  t1.kitting_cost_subid , 
			  t1.cust_support_cost_subid,  
			  t1.year1,
			  t1.month1,
			  t1.Month_year,
              t1.Total_cost_redshift,  
			  T2.Total_cost_sales			  
			  from fin_insights.stage_Total_cost_sales_red T1
left Join fin_insights.total_cost_sales T2
on T1.Month_year = T2.Month_year
and T1.region_id = T2.Region) ;

drop table if exists fin_insights.stage_Total_cost_sales_red3 ;

create table fin_insights.stage_Total_cost_sales_red3 as 
select *, (other_cost/Count_subid) as other_cost1 
from ( 
      with T1 as (
	              select count(subscription_id) as Count_subid, 
				         region_id, 
				         year_month
                  from fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue_emea
                  group by year_month, region_id
                  order by  region_id, year_month 
				 )
      select T1.Count_subid,
	         T1.region_id,
		     T1.year_month,
        	 T2.other_cost 
	  from  T1
      left Join fin_insights.stage_Total_cost_sales_red2 T2
      on T1.year_month = T2.year_month
      and T1.region_id = T2.region_id
	  ) ;

drop table if exists fin_insights.stage_other_cost ;

create table fin_insights.stage_other_cost as 
select *, 
        other_cost1 as other_cost 
from (
       select t1.subscription_id,
              t1.year_month,
              t1.region_id,
              t1.country_id,
              t1.printer_model,  
              t1.Year_Month_bcst, 
              t1.replacement_index, 
              t1.billing_cycle_pages_printed_number,  
              t1.billing_cycle_free_pages_number, 
              t1.billing_cycle_rollover_pages_number, 
              t1.billing_cycle_overage_pages, 
              t1.billing_cycle_free_month_type,  
              t1.billing_cycle_plan_description,  
              t1.plan_price_in_cents, 
              t1.billing_cycle_plan_pages, 
              t1.current_billing_cycle_overage_amount,  
              t1.days_in_billing_cycle,
	          T1.cost_ptb , 
              T1.cost_warranty,						
	          T1.cost_esc,
	          T1.cost_esc_ink_shipment,			
	          T1.cost_esc_welcome_kit_shipment,
	          T1.esc_count,
	          T1.cost_logistics,
	          T1.inkshipment,
	          T1.welcomekitshipment,
	          T1.cost_logistics_ink_shipment,
	          T1.cost_logistics_welcome_kit_shipment,
	          T1.shipping_speed_standard,
              T1.shipping_speed_express,
	          T1.cost_kit, 
	          T1.cost_kit_ink_shipment, 
	          T1.cost_kit_welcome_kit_shipment,
	          T1.cost_customer_support, 
              T1.cust_support_count,
              T1.cost_freemonths,
              T1.freemonths_count,
              t1.year_month_subscription,
              t1.enrolled_on_date, 
              t1.year_month_enroll,
              t1.full_enrollment_date,
              t1.year_month_full_enroll,
              t1.partial_enrollment_flag,              
              t1.hp_plus_activated_printer,
              t1.hp_plus_eligible_printer,
              t1.bounty_rate, 
              t1.currency,  
              t1.currency_conversion, 
              t1.bounty_rate_usd,   
	          t1.enrollment_type, 
	          t1.revshare_rate,
	          t1.year_month_subscription_1,
	          t1.year_month_enroll_1,
              t1.year_month_full_enroll_1,	          
	          t1.enrollment_id, 
	          t1.subscription_date,  
	          t1.printer_serial_number, 
	          t1.platform, 
	          t1.enrollment_plan, 
	          t1.current_plan,  
	          t1.p2_enrollment, 
	          t1.p2_category,  
--	          t1.printer_retailer_name,
	          t1.country_id_m,  
	          t1.printer_model_m,
	          t1.fiscal_year_quarter_code,
	          t1.country_id1,
	          t1.printer_model1,
	          t1.hp_plus_activated_printer1,
	          t1.age_month,
	          t1.Pages_printed,
	          t1.plan_page_utilization_percent,
	          t1.billing_cycle_plan_description1,
	          t1.plan_price_in_cents1,
	          t1.billing_cycle_plan_pages1,
	          t1.age_group,
	          t1.freemonths_count_subid,
	          t1.Kits_count_subid,
	          t1.total_pages_printed_ltd,
	          t1.enrolled_not_subscribed,
              t1.full_enrolled_not_subscribed,	 	          
	          t1.free_page_plan,
	          t1.lag_billing_cycle_plan_pages,
	          t1.lag_billing_cycle_plan_pages_null,
	          t1.flag,
	          t1.rank_replacementindex,
	          t1.billing_cycle_tax_amount_cents, 
	          t1.billing_cycle_pretax_amount_cents, 
	          t1.billing_cycle_pretax_amount_us_cents,
	          t1.billing_cycle_id,
	          t1.EMEA_pretax_amount_us_cents,
	          T2.other_cost1
	    from  fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue_emea T1
		left Join  fin_insights.stage_Total_cost_sales_red3 T2
		on T1.year_month = T2.year_month
		and T1.region_id = T2.region_id
	 )	; 
	
drop table if exists fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue1 ;

create table fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue1 as 
select subscription_id,
       year_month,
       region_id,
       country_id,
       printer_model,  
       Year_Month_bcst, 
       replacement_index, 
       billing_cycle_pages_printed_number,  
       billing_cycle_free_pages_number, 
       billing_cycle_rollover_pages_number, 
       billing_cycle_overage_pages, 
       billing_cycle_free_month_type,  
       billing_cycle_plan_description,  
       plan_price_in_cents, 
       billing_cycle_plan_pages, 
       current_billing_cycle_overage_amount,  
       days_in_billing_cycle,
	   cost_ptb , 
       cost_warranty,						
	   cost_esc,
	   cost_esc_ink_shipment,			
	   cost_esc_welcome_kit_shipment,
	   esc_count,
	   cost_logistics,
	   inkshipment,
	   welcomekitshipment,
	   cost_logistics_ink_shipment,
	   cost_logistics_welcome_kit_shipment,
	   shipping_speed_standard,
       shipping_speed_express,
	   cost_kit, 
	   cost_kit_ink_shipment, 
	   cost_kit_welcome_kit_shipment,
	   cost_customer_support, 
       cust_support_count,
       cost_freemonths,
       freemonths_count,
       year_month_subscription,
       enrolled_on_date, 
       year_month_enroll,
       full_enrollment_date,
       year_month_full_enroll,
       partial_enrollment_flag,
       hp_plus_activated_printer,
       hp_plus_eligible_printer,
       bounty_rate, 
       currency,  
       currency_conversion, 
       bounty_rate_usd,   
	   enrollment_type, 
	   revshare_rate,
	   year_month_subscription_1,
	   year_month_enroll_1,
	   year_month_full_enroll_1,
	   enrollment_id, 
	   subscription_date,  
	   printer_serial_number, 
	   platform, 
	   enrollment_plan, 
	   current_plan,  
	   p2_enrollment, 
	   p2_category,  
--	   printer_retailer_name,
	   country_id_m,  
	   printer_model_m,
	   fiscal_year_quarter_code,
	   country_id1,
	   printer_model1,
	   hp_plus_activated_printer1,
	   age_month,
	   Pages_printed,
	   plan_page_utilization_percent,
	   billing_cycle_plan_description1,
	   plan_price_in_cents1,
	   billing_cycle_plan_pages1,
	   age_group,
	   freemonths_count_subid,
	   Kits_count_subid,
	   total_pages_printed_ltd,
	   enrolled_not_subscribed,
	   full_enrolled_not_subscribed,
	   free_page_plan,
	   lag_billing_cycle_plan_pages,
	   lag_billing_cycle_plan_pages_null,
	   flag,
	   rank_replacementindex,
	   billing_cycle_tax_amount_cents, 
	   billing_cycle_pretax_amount_cents, 
	   billing_cycle_pretax_amount_us_cents,
	   billing_cycle_id,
	   EMEA_pretax_amount_us_cents,
	   other_cost, 
       case when billing_cycle_plan_pages1 > lag_billing_cycle_plan_pages_null then 'upgraded_plan'
            when billing_cycle_plan_pages1 < lag_billing_cycle_plan_pages_null then 'downgraded_plan' 
            when billing_cycle_plan_pages1 = lag_billing_cycle_plan_pages_null then 'same_plan' end as plan_change_status,
       sum(flag) over (PARTITION by subscription_id order by year_month rows unbounded preceding) as plan_change_count, 
       case when region_id = 'NA' then plan_price_in_cents1 
            when region_id = 'EMEA' then EMEA_pretax_amount_us_cents end as plan_price_in_us_cents,
       case when  billing_cycle_pretax_amount_us_cents > 0 then 1 else 0 end as paid_subscriber,
       case when  billing_cycle_pretax_amount_us_cents > 0 or freemonths_count > 0 or free_page_plan = '15_page_plan' then 1 else 0 end as active_subscribers,
       case when  revshare_rate > 0 then (ISNULL(revshare_rate,0)/100)* ISNULL(plan_price_in_us_cents,0)  end as revshare_cost1,
       case when  revshare_cost1 >0 then ISNULL((cast(revshare_cost1 as float)/100), 0) end as revshare_cost,
       case when  revshare_cost > 0 then 1 else 0 end as paid_revshare,
       case when  subscription_date is not null then 1 else 0 end  as new_subscribers,
       case when  enrolled_on_date is not null then 1 else 0 end  as new_enroll,
       case when  full_enrollment_date is not null then 1 else 0 end  as new_full_enroll, -- newly added field
       case when  billing_cycle_free_month_type is null then  'unknown' else  billing_cycle_free_month_type end as billing_cycle_free_month_type1,
       case when  billing_cycle_free_month_type1 in ('RemainingTrialMonth') and cost_freemonths > 0 and  year_month_subscription_1 >= '2016-11-01' then ISNULL((cast(cost_freemonths as float)/100), 0) end  as customer_acqisition_freemonths_cost,
       ISNULL(cost_esc,0)+ ISNULL(cost_logistics, 0)+ ISNULL(cost_kit, 0)  as customer_maintenance_supplies_cost,
       case when  billing_cycle_free_month_type1 not in  ('RemainingTrialMonth')  and cost_freemonths > 0  then ISNULL((cast(cost_freemonths as float)/100), 0) end  as customer_maintenance_freemonths_cost,
       case when year_month_subscription_1 >= '2016-11-01' then  ISNULL(customer_acqisition_freemonths_cost, 0) +  ISNULL(bounty_rate_usd,0)  end as customer_acqisition_cost1,
       ISNULL(customer_maintenance_supplies_cost,0) + ISNULL(customer_maintenance_freemonths_cost, 0) + ISNULL(cost_ptb,0) + ISNULL(cost_warranty,0) +  ISNULL(cost_customer_support,0) + ISNULL(revshare_cost, 0) as customer_maintenance_cost,
       ISNULL(billing_cycle_pretax_amount_us_cents,0)/100::numeric(38,2) AS Gross_Revenue, -- NEWLY ADDED 
	   ISNULL(bounty_rate_usd,0) + ISNULL(revshare_cost, 0) AS Contra, -- NEWLY ADDED 
	   ISNULL(Gross_Revenue,0) - ISNULL(Contra,0) AS Net_Revenue, -- NEWLY ADDED 
	   ISNULL(cost_ptb,0)+ ISNULL(cost_warranty,0) + ISNULL(cost_esc,0) + ISNULL(cost_logistics,0) + ISNULL(cost_kit,0) + ISNULL(cost_customer_support,0) + ISNULL(other_cost,0) as cost_sales,
       ISNULL(Net_Revenue,0) - ISNULL(Cost_Sales,0) as Gross_Margin, -- NEWLY ADDED 
	   ISNULL(market_name,'other') as market,
       case when plan_page_utilization_percent = '<20' then 1
            when plan_page_utilization_percent = '20-40' then 2
            when plan_page_utilization_percent = '40-60' then 3
            when plan_page_utilization_percent = '60-80' then 4
            when plan_page_utilization_percent = '80-100' then 5
            when plan_page_utilization_percent = '100-150' then 6
            when plan_page_utilization_percent = '150-200' then 7
            when plan_page_utilization_percent = '>200' then 8
            when plan_page_utilization_percent = 'no_print' then 9 end as plan_page_utilization_percent_num,         
       case when age_group  = 'Upto_12' then  1
            when age_group = '13-24' then 2
            when age_group = '25-36' then 3
            when age_group = '37-48' then 4 
            when age_group = '49-60' then 5
            when age_group = '>60' then 6
            when age_group = 'enrolled_notsubscribed' then 7 end as age_group_num,
            region_code as Region_2,
       case when p2_enrollment = 'P1 - std'  then 'P1'
            when  p2_enrollment = 'P1 - sys'  then 'P1'
            when  p2_enrollment = 'P1 - sys - replacement'  then 'P1'
            when  p2_enrollment = 'P1 - std - replacement'  then 'P1'
            when  p2_enrollment = 'P1 - no regn - replacement'  then 'P1'
            when  p2_enrollment = 'P1 - no regn'  then 'P1'
            when  p2_enrollment = 'P2 - std'  then 'P2'
            when  p2_enrollment = 'P2 - no regn'  then 'P2'
            when  p2_enrollment = 'P2 - sys'  then 'P2' end as enrollment_category,     
       case when billing_cycle_free_month_type1 =  'RemainingOthersMonth' then 'Others'
            when billing_cycle_free_month_type1 =  'RemainingUpgradePlanFreeMonth' then 'UpgradePlanFree'
            when billing_cycle_free_month_type1 = 'RemainingCustomerSatMonth' then 'CustomerSat'
            when billing_cycle_free_month_type1 = 'RemainingLupsConversionMonth' then 'LupsConversion'
            when billing_cycle_free_month_type1 = 'RemainingOfflineAssetsMonth' then 'OfflineAssets'
            when billing_cycle_free_month_type1 = 'RemainingLupsUpgradePromoMonth' then 'LupsUpgradePromo'
            when billing_cycle_free_month_type1 = 'RemainingReferAFriendMonth' then 'ReferAFriend'
            when billing_cycle_free_month_type1 = 'RemainingDigitalAssetsMonth' then 'DigitalAssets'
            when billing_cycle_free_month_type1 = 'RemainingAffiliateMonth' then 'Affiliate'
            when billing_cycle_free_month_type1 = 'RemainingPromoMonth' then 'Promo'
            when billing_cycle_free_month_type1 = 'RemainingTrialMonth' then 'Trial'
            when billing_cycle_free_month_type1 = 'RemainingPrinterReplacementMonth' then 'PrinterReplacement'
            when billing_cycle_free_month_type1 = 'RemainingPrepaidMonth' then 'Prepaid'
            when billing_cycle_free_month_type1 = 'RemainingCoBrandedMonth' then 'CoBranded'  else billing_cycle_free_month_type1 end as billing_cycle_free_month_type_short,
       case when freemonths_count = 1 then  isnull(billing_cycle_pages_printed_number,0)  - isnull(billing_cycle_overage_pages,0) else 0 end as freemonths_pages_printed,
       isnull(billing_cycle_pages_printed_number,0) - (isnull(freemonths_pages_printed,0) + isnull(billing_cycle_overage_pages,0)) as plan_pages_printed
from fin_insights.stage_other_cost f
left join fin_insights.vw_dim_country d on
f.country_id1 = d.country_code2 ;
	 
EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate) 
     VALUES (
             'Other_Cost',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
             );

END;
$$


call fin_insights.sp_load_other_cost();
select count(1) from fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue1 --380794307 --381330890 --381700575 --383779717 --390011319 --404591474 --414470764--428933108 --438585747 --449520588 --459888343 --472013534 --483082852 --483342043 --494343754 --495975717 --521191755 --532389512 --547362570 --559986488 --573948220 --589784223 --601170649 --612442440 --624043983 --651450802
select count(1) from fin_insights.total_cost_sales --140 --142 --144 --146 --148--150--152--154--156 --158 --160 --162 --164
select year_month,
       region_id,
       FLOOR(sum(customer_acqisition_freemonths_cost)) as customer_acqisition_freemonths_cost,
       FLOOR(sum(customer_maintenance_supplies_cost)) as customer_maintenance_supplies_cost,
       FLOOR(sum(customer_maintenance_freemonths_cost)) as customer_maintenance_freemonths_cost,
       FLOOR(sum(customer_acqisition_cost1)) as customer_acqisition_cost1,
       FLOOR(sum(revshare_cost)) as revshare_cost,
       FLOOR(sum(customer_maintenance_cost)) as customer_maintenance_cost,
       FLOOR(sum(other_cost)) as other_cost
       from fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue1
       where region_id <> 'APJ' and year_month <= '2023-11-01'
       group by year_month,region_id
       order by year_month desc, region_id asc
       
===================================================================================================================================================================
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_marketing_cost()
 LANGUAGE plpgsql
AS $$ 
BEGIN 
      drop table if exists fin_insights.stage_printer_family ;

create table fin_insights.stage_printer_family
(
   printer_model VARCHAR(22) NOT NULL,
   printer_model_short VARCHAR(22) NOT NULL,
   printer_family VARCHAR(22) NOT NULL 
);

copy fin_insights.stage_printer_family
from 's3://instant-ink-finance/team-iifin/profitability/printer_family_mapping.csv'
iam_role 'arn:aws:iam::828361281741:role/team-iifin'
ignoreheader as 1
maxerror 10
emptyasnull
blanksasnull
delimiter as ',' ;

--  /*Marketing*/

drop table if exists fin_insights.stage_Marketing_cost ;

create table fin_insights.stage_Marketing_cost 
(
   fiscal_year_quarter_code VARCHAR(22) NOT NULL,
   marketing_cost decimal(10,3) NOT NULL,
   New_enrol decimal(10,0) NOT NULL 
);

copy fin_insights.stage_Marketing_cost  
from 's3://instant-ink-finance/team-iifin/profitability/marketing_cost.csv'
iam_role 'arn:aws:iam::828361281741:role/team-iifin'
ignoreheader as 1
maxerror 10
emptyasnull
blanksasnull
delimiter as ',';

delete  from fin_insights.marketing_cost
where (fiscal_year_quarter_code)  
in (select s.fiscal_year_quarter_code
from  fin_insights.marketing_cost f, fin_insights.stage_Marketing_cost s
where f.fiscal_year_quarter_code = s.fiscal_year_quarter_code) ;

insert into fin_insights.marketing_cost
(fiscal_year_quarter_code,marketing_cost, New_enrol)
select fiscal_year_quarter_code, 
       marketing_cost,
       New_enrol
from fin_insights.stage_Marketing_cost s
where not exists (	select top 1 * 
						from fin_insights.marketing_cost f 
						where f.fiscal_year_quarter_code = s.fiscal_year_quarter_code
					) ;

--  combining year of subscription for the enrollee + printer family + Marketing Cost
drop table if exists fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue_year ;

create table fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue_year as 
select t1.subscription_id,
       t1.year_month,
       t1.region_id,
       t1.country_id,
       t1.printer_model,  
       t1.Year_Month_bcst, 
       t1.replacement_index, 
       t1.billing_cycle_pages_printed_number,  
       t1.billing_cycle_free_pages_number, 
       t1.billing_cycle_rollover_pages_number, 
       t1.billing_cycle_overage_pages, 
       t1.billing_cycle_free_month_type,  
       t1.billing_cycle_plan_description,  
       t1.plan_price_in_cents, 
       t1.billing_cycle_plan_pages, 
       t1.current_billing_cycle_overage_amount,  
       t1.days_in_billing_cycle,
	   t1.cost_ptb , 
       t1.cost_warranty,						
	   t1.cost_esc,
	   t1.cost_esc_ink_shipment,			
	   t1.cost_esc_welcome_kit_shipment,
	   t1.esc_count,
	   t1.cost_logistics,
	   t1.inkshipment,
	   t1.welcomekitshipment,
	   t1.cost_logistics_ink_shipment,
	   t1.cost_logistics_welcome_kit_shipment,
	   t1.shipping_speed_standard,
       t1.shipping_speed_express,
	   t1.cost_kit, 
	   t1.cost_kit_ink_shipment, 
	   t1.cost_kit_welcome_kit_shipment,
	   t1.cost_customer_support, 
       t1.cust_support_count,
       t1.cost_freemonths,
       t1.freemonths_count,
       t1.year_month_subscription,
       t1.enrolled_on_date, 
       t1.year_month_enroll,
       t1.full_enrollment_date,
       t1.year_month_full_enroll,
       t1.partial_enrollment_flag,       
       t1.hp_plus_activated_printer,
       t1.hp_plus_eligible_printer,
       t1.bounty_rate, 
       t1.currency,  
       t1.currency_conversion, 
       t1.bounty_rate_usd,   
	   t1.enrollment_type, 
	   t1.revshare_rate,
	   t1.year_month_subscription_1,
	   t1.year_month_enroll_1,
       t1.year_month_full_enroll_1,	   
	   t1.enrollment_id, 
	   t1.subscription_date,  
	   t1.printer_serial_number, 
	   t1.platform, 
	   t1.enrollment_plan, 
	   t1.current_plan,  
	   t1.p2_enrollment, 
	   t1.p2_category,  
--	   t1.printer_retailer_name,
	   t1.country_id_m,  
	   t1.printer_model_m,
	   t1.fiscal_year_quarter_code,
	   t1.country_id1,
	   t1.printer_model1,
	   t1.hp_plus_activated_printer1,
	   t1.age_month,
	   t1.Pages_printed,
	   t1.plan_page_utilization_percent,
	   t1.billing_cycle_plan_description1,
	   t1.plan_price_in_cents1,
	   t1.billing_cycle_plan_pages1,
	   t1.age_group,
	   t1.freemonths_count_subid,
	   t1.Kits_count_subid,
	   t1.total_pages_printed_ltd,
	   t1.enrolled_not_subscribed,
       t1.full_enrolled_not_subscribed,	   
	   t1.free_page_plan,
	   t1.lag_billing_cycle_plan_pages,
	   t1.lag_billing_cycle_plan_pages_null,
	   t1.flag,
	   t1.rank_replacementindex,
	   t1.billing_cycle_tax_amount_cents, 
	   t1.billing_cycle_pretax_amount_cents, 
	   t1.billing_cycle_pretax_amount_us_cents,
	   t1.billing_cycle_id,
	   t1.EMEA_pretax_amount_us_cents,
	   t1.other_cost, 
	   t1.plan_change_status,
	   t1.plan_change_count,
	   t1.plan_price_in_us_cents,
	   t1.paid_subscriber,
	   t1.active_subscribers,
	   t1.revshare_cost1,
	   t1.revshare_cost,
	   t1.paid_revshare,
	   t1.new_subscribers,
	   t1.new_enroll,
       t1.new_full_enroll,	   
	   t1.billing_cycle_free_month_type1,
	   t1.customer_acqisition_freemonths_cost,
	   t1.customer_maintenance_supplies_cost,
	   t1.customer_maintenance_freemonths_cost,
	   t1.customer_acqisition_cost1,
	   t1.customer_maintenance_cost,
	   t1.cost_sales,
	   t1.net_revenue,
	   t1.gross_margin,
	   t1.market,
	   t1.plan_page_utilization_percent_num,
	   t1.age_group_num,
       t1.Region_2,
	   t1.enrollment_category,
	   t1.billing_cycle_free_month_type_short,
	   t1.freemonths_pages_printed,
	   t1.plan_pages_printed,
       t2.fiscal_year_name as enroll_year,
       t6.fiscal_year_name as full_enroll_year,                                                        -- newly added field
	   case when t3.printer_family is null then 'other' else printer_family end as printer_family1,  
       case when t3.printer_model_short is null then 'other' else printer_model_short end as printer_sub_brand,
	   t4.marketing_cost,
	   t5.marketing_cost as marketing_cost_1                                             -- newly added field                       
from fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue1 t1
left join app_instant_ink_bi_dim.dim_date_time t2
on T1.year_month_enroll = T2.calendar_date
left Join fin_insights.stage_printer_family t3
on T1.printer_model1 = T3.printer_model
left Join fin_insights.marketing_cost t4
on T1.fiscal_year_quarter_code = T4.fiscal_year_quarter_code 
and T1.new_enroll = T4.New_enrol
left Join fin_insights.marketing_cost t5
on T1.fiscal_year_quarter_code = T5.fiscal_year_quarter_code 
and T1.new_full_enroll = T5.New_enrol
left join app_instant_ink_bi_dim.dim_date_time t6
on t1.year_month_full_enroll = t6.calendar_date;

drop table if exists fin_insights.fact_Profitability ;

create table fin_insights.fact_Profitability as 
select a.subscription_id,
       year_month, 
	   a.fiscal_year_quarter_code, 
	   country_id1 as country_id, 
	   Region_2 as region_id,
	   billing_cycle_id,
	   market,  
	   year_month_bcst, 
	   subscription_date, 
	   enrolled_on_date,
	   full_enrollment_date,
	   year_month_subscription, 
       case when a.enroll_year is null then c.fiscal_year_name else a.enroll_year end as enroll_year,
       case when a.full_enroll_year is null then e.fiscal_year_name else a.full_enroll_year end as full_enroll_year,
       case when a.year_month_enroll is null then b.year_month_enroll else a.year_month_enroll end as year_month_enroll,
       case when a.year_month_full_enroll is null then d.year_month_full_enroll else a.year_month_full_enroll end as year_month_full_enroll,
       partial_enrollment_flag,
       hp_plus_activated_printer1 as hp_plus_activated_printer,
       hp_plus_eligible_printer,
	   printer_model1 as printer_model,
	   printer_sub_brand, 
	   printer_family1 as printer_family, 
	   billing_cycle_plan_description1 as billing_cycle_plan_description,
	   plan_price_in_cents1 as plan_price_in_cents, 
	   plan_price_in_us_cents,  
	   billing_cycle_plan_pages1 as billing_cycle_plan_pages, 
	   billing_cycle_pages_printed_number,  
	   freemonths_pages_printed, 
	   billing_cycle_overage_pages as overage_pages_printed, 
	   plan_pages_printed, 
	   pages_printed,
	   plan_page_utilization_percent , 
	   plan_page_utilization_percent_num, 
	   billing_cycle_free_pages_number,  
	   age_month, 
	   age_group, 
	   age_group_num,
	   enrollment_plan, 
	   current_plan,  
	   replacement_index, 
	   enrollment_category, 
	   enrollment_type, 
	   p2_enrollment, 
	   p2_category,  
--	   printer_retailer_name as kit_retailer_name , 
	   enrollment_id, 
	   printer_serial_number,  
	   platform,
	   enrolled_not_subscribed,
	   full_enrolled_not_subscribed,
	   free_page_plan, 
	   billing_cycle_free_month_type1 as billing_cycle_free_month_type, 
	   billing_cycle_free_month_type_short, 
	   kits_count_subid, 
	   total_pages_printed_ltd,  
	   plan_change_status,  
	   plan_change_count, 
	   lag_billing_cycle_plan_pages_null as previous_billing_cycle_plan_pages, 
	   paid_subscriber, 
	   active_subscribers, 
	   new_enroll,
	   new_full_enroll,
	   new_subscribers, 
	 --  covid,
	 --  covid_subscription, 
	   freemonths_count_subid, 
	   freemonths_count,  
	   cost_freemonths as freemonths_cost,  
	   bounty_rate, 
	   bounty_rate_usd,
	   revshare_rate, 
	   revshare_cost,
	   paid_revshare,
	   cost_ptb as ptb_cost_subid, 
	   cost_warranty as warranty_cost_subid, 
	   shipping_speed_standard, 
	   shipping_speed_express, 
	   inkshipment, 
	   welcomekitshipment, 
	   esc_count, 
	   cost_esc as esc_cost_subid, 
	   cost_esc_ink_shipment as esc_inkshipment_cost, 
	   cost_esc_welcome_kit_shipment as esc_welcomekitshipment_cost, 
	   cost_logistics as logistic_cost_subid,
	   cost_logistics_ink_shipment as logistic_inkshipment_cost, 
	   cost_logistics_welcome_kit_shipment as logistic_welcomekitshipment_cost, 
	   cost_kit as kitting_cost_subid, 
	   cost_kit_ink_shipment as kitting_inkshipment_cost, 
	   cost_kit_welcome_kit_shipment as kitting_welcomekitshipment_cost, 
	   cost_customer_support as cust_support_cost_subid, 
	   marketing_cost,
	   marketing_cost_1,
	   other_cost, 
	   cust_support_count, 
	   customer_acqisition_freemonths_cost, 
	   customer_maintenance_supplies_cost, 
	   customer_maintenance_freemonths_cost, 
	   customer_acqisition_cost1+ ISNULL(marketing_cost,0) as customer_acqisition_cost ,
	   customer_acqisition_cost1+ ISNULL(marketing_cost_1,0) as customer_acqisition_cost_1, -- newly added field
	   customer_maintenance_cost,
	   cost_sales,
	   net_revenue,
	   gross_margin,
	   billing_cycle_tax_amount_cents,
	   current_billing_cycle_overage_amount, 
	   billing_cycle_pretax_amount_cents, 
	   billing_cycle_pretax_amount_us_cents	   
from fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue_year a
left join (select subscription_id,year_month_enroll from (
           select subscription_id, year_month_enroll,
           row_number() over (partition by subscription_id order by year_month_enroll) as rnk 
           from fin_insights.stage_customer_sub_base  
           ) where rnk = 1) B 
           on A.subscription_id = B.subscription_id
left join app_instant_ink_bi_dim.dim_date_time c
on b.year_month_enroll = c.calendar_date
left join (select subscription_id,year_month_full_enroll from (
           select subscription_id, year_month_full_enroll,
           row_number() over (partition by subscription_id order by year_month_full_enroll) as rnk 
           from fin_insights.stage_customer_sub_base  
           ) where rnk = 1) d 
           on A.subscription_id = d.subscription_id
left join app_instant_ink_bi_dim.dim_date_time e
on d.year_month_full_enroll = e.calendar_date
where a.enrollment_category is not null ;

GRANT SELECT ON fin_insights.fact_profitability to auto_prdii;

DROP TABLE IF EXISTS fin_insights.stage_cost_customer_support ;
DROP TABLE IF EXISTS fin_insights.stage_cost_esc ;
DROP TABLE IF EXISTS fin_insights.stage_cost_kit ;
DROP TABLE IF EXISTS fin_insights.stage_cost_logistics ;
DROP TABLE IF EXISTS fin_insights.stage_cost_ptb_warranty ;
DROP TABLE IF EXISTS fin_insights.stage_cost ;
DROP TABLE IF EXISTS fin_insights.stage_cust_newenroll ;
DROP TABLE IF EXISTS fin_insights.stage_bounty ;
DROP TABLE IF EXISTS fin_insights.stage_ptb_supl_cs_freemonths_Bounty ;
DROP TABLE IF EXISTS fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev ;
DROP TABLE IF EXISTS fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev_cust2 ;
DROP TABLE IF EXISTS fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev_cust3 ;
DROP TABLE IF EXISTS fin_insights.stage_ptb_supl_cs_freemonths_Bounty_rev_cust4 ;
DROP TABLE IF EXISTS fin_insights.stage_ptb_supl_cs_freemonths_Bounty4 ;
DROP TABLE IF EXISTS fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue ;
DROP TABLE IF EXISTS fin_insights.stage_revenue_emea ;
DROP TABLE IF EXISTS fin_insights.stage_revenue_emea1 ;
DROP TABLE IF EXISTS fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue_emea ;
DROP TABLE IF EXISTS fin_insights.stage_Total_cost_sales_red ;
DROP TABLE IF EXISTS fin_insights.stage_Total_cost_sales_red2 ;
DROP TABLE IF EXISTS fin_insights.stage_Total_cost_sales_red3 ;
DROP TABLE IF EXISTS fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue1 ;
DROP TABLE IF EXISTS fin_insights.stage_ptb_supl_cs_freemonths_Bounty4_revenue_year ;
DROP TABLE IF EXISTS fin_insights.stage_billing_cycle_base ;
DROP TABLE IF EXISTS fin_insights.stage_customer_sub_base ;
	
EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate) 
     VALUES (
             'Marketing_Cost',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
             );

END;
$$


call fin_insights.sp_load_marketing_cost();
select count(1) from fin_insights.fact_Profitability --380794307 --381330890 --381700575 --383779717 --384063144 --389949110 --404523210 --414398550 --428933003 --449520483 --459888238 --472013429 --482573958 --482815089 --493679132 --495264990 --519612871 --530340033 --544561457 --556555901 --569944554 --585224212 --596303103 --607333086 --618766514 --646008550
elect count(1) from fin_insights.Marketing_cost  --18 --19 --19 --20 --20 --21 --22--22 --22 --23 --23--23--24--24--24
select count(1) from fin_insights.stage_printer_family -- 274 --274

-------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_profitability_db()
 LANGUAGE plpgsql
AS $$ 
BEGIN 
      drop table if exists fin_insights.Profitability_db ;
      drop table if exists GetYearMonth_Profitability_db ;

create temp table GetYearMonth_Profitability_db as
select TRUNC(date_trunc('MONTH', ADD_MONTHS(current_date ,-1)))::date as year_month ;
     
create table fin_insights.Profitability_db as
select subscription_id,
       year_month, 
	   fiscal_year_quarter_code, 
	   country_id, 
	   region_id,
	   billing_cycle_id,
	   market,  
	   year_month_bcst, 
	   subscription_date, 
	   enrolled_on_date,
	   full_enrollment_date,
	   year_month_subscription, 
	   enroll_year,
	   full_enroll_year,
       year_month_enroll,
       year_month_full_enroll,
       partial_enrollment_flag,
       hp_plus_activated_printer,
       hp_plus_eligible_printer,
	   printer_model,  
	   printer_sub_brand, 
	   printer_family, 
	   billing_cycle_plan_description,
	   plan_price_in_cents, 
	   plan_price_in_us_cents,  
	   billing_cycle_plan_pages, 
	   billing_cycle_pages_printed_number,  
	   freemonths_pages_printed, 
	   overage_pages_printed, 
	   plan_pages_printed, 
	   pages_printed,
	   plan_page_utilization_percent , 
	   plan_page_utilization_percent_num, 
	   billing_cycle_free_pages_number,  
	   age_month, 
	   age_group, 
	   age_group_num,
	   enrollment_plan, 
	   current_plan,  
	   replacement_index, 
	   enrollment_category, 
	   enrollment_type, 
	   p2_enrollment, 
	   p2_category,  
--	   kit_retailer_name , 
	   enrollment_id, 
	   printer_serial_number,  
	   platform,
	   enrolled_not_subscribed,
	   full_enrolled_not_subscribed,
	   free_page_plan, 
	   billing_cycle_free_month_type, 
	   billing_cycle_free_month_type_short, 
	   kits_count_subid, 
	   total_pages_printed_ltd,  
	   plan_change_status,  
	   plan_change_count, 
	   previous_billing_cycle_plan_pages, 
	   paid_subscriber, 
	   active_subscribers, 
	   new_enroll,
	   new_full_enroll,
	   new_subscribers, 
	   freemonths_count_subid, 
	   freemonths_count,  
	   freemonths_cost,  
	   bounty_rate, 
	   bounty_rate_usd,
	   revshare_rate, 
	   revshare_cost,
	   paid_revshare,
	   ptb_cost_subid, 
	   warranty_cost_subid, 
	   shipping_speed_standard, 
	   shipping_speed_express, 
	   inkshipment, 
	   welcomekitshipment, 
	   esc_count, 
	   esc_cost_subid, 
	   esc_inkshipment_cost, 
	   esc_welcomekitshipment_cost, 
	   logistic_cost_subid,
	   logistic_inkshipment_cost, 
	   logistic_welcomekitshipment_cost, 
	   kitting_cost_subid, 
	   kitting_inkshipment_cost, 
	   kitting_welcomekitshipment_cost, 
	   cust_support_cost_subid, 
	   marketing_cost,
	   marketing_cost_1,
	   other_cost, 
	   cust_support_count, 
	   customer_acqisition_freemonths_cost, 
	   customer_maintenance_supplies_cost, 
	   customer_maintenance_freemonths_cost, 
	   customer_acqisition_cost,
	   customer_acqisition_cost_1,
	   customer_maintenance_cost,
	   cost_sales,
	   net_revenue,
	   gross_margin,
	   billing_cycle_tax_amount_cents,
	   current_billing_cycle_overage_amount, 
	   billing_cycle_pretax_amount_cents, 
	   billing_cycle_pretax_amount_us_cents
FROM fin_insights.fact_Profitability
where year_month >= '2018-10-01' 
and year_month  <= (select year_month from GetYearMonth_Profitability_db) ;

GRANT SELECT ON TABLE fin_insights.Profitability_db TO ramk;
GRANT ALL ON TABLE fin_insights.Profitability_db TO ramdassu;
GRANT SELECT ON TABLE fin_insights.Profitability_db TO tskav;
GRANT SELECT ON TABLE fin_insights.Profitability_db TO auto_prdii;
GRANT SELECT ON TABLE fin_insights.Profitability_db TO srv_power_bi_fin;

EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate) 
     VALUES (
             'Profitability_db',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
             );

END;
$$

call fin_insights.sp_load_profitability_db();

select count(1) from fin_insights.Profitability_db --272855692 --272858488 --272858507 --294208073 --294208045 --305189048 --316324760 --327540822 -- 361866460 --373578278 --385385352 --396924349 --432819234 --445015247 --457311886 --469638491 --482167130 --494609043 --507114382 --519752172 --532364225 --557754841
-------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_cumenroll_grossmargin_prof_db()
 LANGUAGE plpgsql
AS $$ 
BEGIN 
      drop table if exists GetYearMonth_CumEnroll ;

create temp table GetYearMonth_CumEnroll as
select TRUNC(date_trunc('MONTH', ADD_MONTHS(current_date ,-1)))::date as year_month ;

drop table if exists fin_insights.prof_summ_agg ;

create table fin_insights.prof_summ_agg as 
select year_month,
       fiscal_year_quarter_code,
       country_id,
       market,
       region_id,
       printer_family,
       billing_cycle_plan_pages,
 --    kit_retailer_name,
       sum(billing_cycle_pages_printed_number) as billing_cycle_pages_printed_number,
       sum(freemonths_pages_printed) as  freemonths_pages_printed,
       sum(overage_pages_printed) as overage_pages_printed,
       sum(plan_pages_printed) as plan_pages_printed,
       sum(new_enroll) as new_enroll,
       sum(new_full_enroll) as new_full_enroll, --newly added field
       sum(freemonths_cost) as freemonths_cost,
       sum(bounty_rate_usd) as bounty_rate_usd,
       sum(revshare_cost) as revshare_cost,
       sum(ptb_cost_subid) as ptb_cost_subid,
       sum(warranty_cost_subid) as warranty_cost_subid,
       sum(esc_cost_subid) as esc_cost_subid,
       sum(logistic_cost_subid) as logistic_cost_subid,
       sum(kitting_cost_subid) as kitting_cost_subid,
       sum(cust_support_cost_subid) as cust_support_cost_subid,
       sum(marketing_cost) as marketing_cost,
       sum(marketing_cost_1) as marketing_cost_1, -- newly added field
       sum(other_cost) as other_cost,
       sum(customer_acqisition_freemonths_cost) as customer_acqisition_freemonths_cost,
       sum(customer_maintenance_supplies_cost) as customer_maintenance_supplies_cost,
       sum(customer_maintenance_freemonths_cost) as customer_maintenance_freemonths_cost,
       sum(customer_acqisition_cost) as customer_acqisition_cost,
       sum(customer_acqisition_cost_1) as customer_acqisition_cost_1,
       sum(customer_maintenance_cost) as customer_maintenance_cost,
       sum(current_billing_cycle_overage_amount) as current_billing_cycle_overage_amount,
       sum(billing_cycle_pretax_amount_us_cents) as billing_cycle_pretax_amount_us_cents 
from fin_insights.fact_Profitability
group by year_month,fiscal_year_quarter_code, country_id, market, region_id,  printer_family, billing_cycle_plan_pages --, kit_retailer_name
order by year_month, fiscal_year_quarter_code, country_id, market, region_id,  printer_family, billing_cycle_plan_pages ; --, kit_retailer_name ;

drop table if exists fin_insights.cumenrolles ;		
			
create table fin_insights.cumenrolles as 
select calendar_month_date,
       country,
       printer_sub_brand,
       plan_pages_per_month,
 --    retailer,
       partial_enrollment_flag,
	   partial_cancellation_flag,
       all_enrollments_gross,
       cancellation_gross,
       case when country = 'UK' then 'GB' else country end as country_id,
       case when printer_sub_brand = 'LaserJet' then 	'LaserJet'
            when printer_sub_brand = 'DJ' then 'DeskJet'
            when printer_sub_brand = 'Envy' then 'Envy'
            when printer_sub_brand = 'DeskJet Plus' then 'DeskJet'
	        when printer_sub_brand = 'OJP' then 'OfficeJet Pro'
	        when printer_sub_brand = 'Envy Photo' then 'Envy'
	        when printer_sub_brand = 'OfficeJet' then 'OfficeJet'
	        when printer_sub_brand = 'AMP' then 'AMP'
	        when printer_sub_brand = 'TANGO' then 'TANGO'
	        when printer_sub_brand = 'OJP-X' then 'OfficeJet Pro'
	        when printer_sub_brand = 'OJ' then 'OfficeJet'
	        when printer_sub_brand = 'Tango' then 'TANGO'
	        when printer_sub_brand = 'LaserJet MFP' then 'LaserJet'
	        when printer_sub_brand = 'Envy Pro' then 'Envy' else 'other' end as printer_family,	   
            (isnull(all_enrollments_gross,0) - isnull(cancellation_gross, 0)) as active_enrollments,
            sum(active_enrollments) over (partition by country, printer_sub_brand, plan_pages_per_month,-- retailer, 
            partial_enrollment_flag order by calendar_month_date,country, printer_sub_brand, plan_pages_per_month--, retailer 
            rows unbounded preceding) as cum_enrollees 
from (	
			   select calendar_month_date,
					  country, 
					  printer_sub_brand, 
					  plan_pages_per_month,  
			--		  retailer,
					  partial_enrollment_flag,
					  partial_cancellation_flag,
					  all_enrollments_gross,
					  cancellation_gross 
			   from (
		              select calendar_month_date,   
			                 country, 
			                 printer_sub_brand, 
			                 plan_pages_per_month,  
			  --             retailer,
			                 partial_enrollment_flag,
			                 partial_cancellation_flag,
			                 sum(all_enrollments_gross) over (partition by  country, printer_sub_brand, plan_pages_per_month,partial_enrollment_flag order by calendar_month_date, country, printer_sub_brand, plan_pages_per_month  rows unbounded preceding ) as all_enrollments_gross,
			                 sum(cancellation_gross) over (partition by  country, printer_sub_brand, plan_pages_per_month, partial_cancellation_flag order by calendar_month_date, country, printer_sub_brand, plan_pages_per_month rows unbounded preceding ) as cancellation_gross

		              from (
			                 select  calendar_month_date, 
					                 country, 
					                 printer_sub_brand,  
					                 plan_pages_per_month,  
					   --            retailer,
					                 partial_enrollment_flag,
					                 partial_cancellation_flag,
					                 sum(all_enrollments_gross) as all_enrollments_gross,
					                 sum(cancellation_gross) as cancellation_gross  
			                 from bi_fact.fact_reporting_summary
			                 where program_type = 'Instant Ink' and calendar_month_date <= '2018-10-01'
			                 group by country, calendar_month_date, printer_sub_brand, plan_pages_per_month, partial_enrollment_flag, partial_cancellation_flag 
			   
			 
			               )
			        )
		       where calendar_month_date = '2018-10-01' 
        	
        Union 
        
		select calendar_month_date, 
		   country, 
		   printer_sub_brand, 
		   plan_pages_per_month,  
	--	   retailer,
		   partial_enrollment_flag,
		   partial_cancellation_flag,
		   sum(all_enrollments_gross) as all_enrollments_gross, 
		   sum(cancellation_gross) as cancellation_gross
		from  bi_fact.fact_reporting_summary
		where program_type = 'Instant Ink' and calendar_month_date > '2018-10-01'
		group by country, calendar_month_date, printer_sub_brand, plan_pages_per_month, partial_enrollment_flag, partial_cancellation_flag 
	 )
	order by country, calendar_month_date, printer_sub_brand, plan_pages_per_month, partial_enrollment_flag, partial_cancellation_flag ;


drop table if exists fin_insights.prof_cum_enrolles_agg ;

create table fin_insights.prof_cum_enrolles_agg as 
select COALESCE (T1.year_month, T2.calendar_month_date) as year_month, 
       COALESCE (T1.country_id, T2.country_id) as country_id, 
       COALESCE (T1.printer_family, T2.printer_family) as printer_family, 
       COALESCE (T1.billing_cycle_plan_pages, T2.plan_pages_per_month) as billing_cycle_plan_pages, 
--     COALESCE (T1.kit_retailer_name, T2.retailer) as kit_retailer_name, 
       t1.fiscal_year_quarter_code,
       t1.region_id,
       t1.market,
       t1.billing_cycle_pages_printed_number,
       t1.freemonths_pages_printed,
       t1.overage_pages_printed,
       t1.plan_pages_printed,
       t1.new_enroll,
       t1.new_full_enroll,       
       t1.freemonths_cost,
       t1.bounty_rate_usd,
       t1.revshare_cost,
       t1.ptb_cost_subid,
       t1.warranty_cost_subid,
       t1.esc_cost_subid,
       t1.logistic_cost_subid,
       t1.kitting_cost_subid,
       t1.cust_support_cost_subid,
       t1.marketing_cost,
       t1.marketing_cost_1,
       t1.other_cost,
       t1.customer_acqisition_freemonths_cost,
       t1.customer_maintenance_supplies_cost,
       t1.customer_maintenance_freemonths_cost,
       t1.customer_acqisition_cost,
       t1.customer_acqisition_cost_1,
       t1.customer_maintenance_cost,
       t1.current_billing_cycle_overage_amount,
       t1.billing_cycle_pretax_amount_us_cents,
       t2.all_enrollments_gross, 
       t2.cancellation_gross, 
       t2.active_enrollments, 
       t2.cum_enrollees,
       t2.partial_enrollment_flag,
       t2.partial_cancellation_flag
from fin_insights.prof_summ_agg t1
FULL OUTER JOIN fin_insights.cumenrolles t2
on t1.year_month = t2.calendar_month_date
and t1.country_id = t2.country_id
and t1.printer_family = t2.printer_family
and t1.billing_cycle_plan_pages = t2.plan_pages_per_month ;
--and t1.kit_retailer_name = t2.retailer ;


drop table if exists fin_insights.prof_cum_enrolles_db ;

create table fin_insights.prof_cum_enrolles_db as 
select year_month, 
       country_id, 
       printer_family, 
       billing_cycle_plan_pages, 
   --  kit_retailer_name, 
       fiscal_year_quarter_code,
       region_id,
       market,
       billing_cycle_pages_printed_number,
       freemonths_pages_printed,
       overage_pages_printed,
       plan_pages_printed,
       new_enroll,
       new_full_enroll,
       freemonths_cost,
       bounty_rate_usd,
       revshare_cost,
       ptb_cost_subid,
       warranty_cost_subid,
       esc_cost_subid,
       logistic_cost_subid,
       kitting_cost_subid,
       cust_support_cost_subid,
       marketing_cost,
       marketing_cost_1,
       other_cost,
       customer_acqisition_freemonths_cost,
       customer_maintenance_supplies_cost,
       customer_maintenance_freemonths_cost,
       customer_acqisition_cost,
       customer_acqisition_cost_1,
       customer_maintenance_cost,
       current_billing_cycle_overage_amount,
       billing_cycle_pretax_amount_us_cents,
       all_enrollments_gross, 
	   cancellation_gross, 
	   active_enrollments, 
	   cum_enrollees,
       partial_enrollment_flag,
       partial_cancellation_flag	   
from fin_insights.prof_cum_enrolles_agg 
where year_month >= '2018-10-01' and year_month <= (select year_month from GetYearMonth_CumEnroll) ;

-- calculate gross margin bucket 

drop table if exists fin_insights.prof_gross_margin_by_subid ;

create table fin_insights.prof_gross_margin_by_subid as 
select subscription_id,
       billable_months_SubId,
       Cost_Sales_SubId,
       Net_Revenue_SubId,
       Gross_Margin_SubId,
       Gross_Margin_ByPercentage,
       Gross_Margin_Bucket
	   from (
              select subscription_id, 
			         billable_months_SubId,
			         Cost_Sales_SubId,
					 Net_Revenue_SubId, 
					 Gross_Margin_SubId,
                     case when Net_Revenue_SubId < 0 then cast(ISNULL(Gross_Margin_SubId,0)/NULLIF(Net_Revenue_SubId,0)*-100 as float) else
                     cast(ISNULL(Gross_Margin_SubId,0)/NULLIF(Net_Revenue_SubId,0)*100 as float) end as Gross_Margin_ByPercentage,
                     case when Gross_Margin_SubId < 0 then '<0%'
                          when Gross_Margin_ByPercentage  between 0 and 5  then  '0-5%'
                          when Gross_Margin_ByPercentage  between 5 and 10 then '6-10%'
                          when Gross_Margin_ByPercentage  between 10 and 15 then '11-15%'
                          when Gross_Margin_ByPercentage  between 15 and 20 then '16-20%'
                          when Gross_Margin_ByPercentage  between 20 and 25 then '21-25%'
                          when Gross_Margin_ByPercentage  between 25 and 30 then '26-30%'
                          when Gross_Margin_ByPercentage  between 30 and 35 then '31-35%'
                          when Gross_Margin_ByPercentage  between 35 and 40 then '36-40%'
                          when Gross_Margin_ByPercentage  between 40 and 45 then '41-45%'
                          when Gross_Margin_ByPercentage  between 45 and 50 then '46-50%'
                          when Gross_Margin_ByPercentage  between 50 and 55 then '51-55%'
                          when Gross_Margin_ByPercentage  between 55 and 60 then '56-60%'
                          when Gross_Margin_ByPercentage  between 60 and 65 then '61-65%'
                          when Gross_Margin_ByPercentage  between 65 and 70 then '66-70%'
                          when Gross_Margin_ByPercentage  between 70 and 75 then '71-75%'
                          when Gross_Margin_ByPercentage  between 75 and 80 then '76-80%'
                          when Gross_Margin_ByPercentage  between 80 and 85 then '81-85%'
                          when Gross_Margin_ByPercentage  between 85 and 90 then '86-90%'
                          when Gross_Margin_ByPercentage  between 90 and 95 then '91-95%'
                          when Gross_Margin_ByPercentage  >95 then '96-100%' end as Gross_Margin_Bucket
            
              from (
                     select subscription_id,
                            count(distinct billing_cycle_id) as billable_months_SubId,
                            sum(cost_sales)::numeric(38,2) as Cost_Sales_SubId,
                            sum(Net_Revenue)::numeric(38,2) as Net_Revenue_SubId,
                            sum(ISNULL(Gross_Margin,0))::numeric(38,2) as Gross_Margin_SubId      
                     from fin_insights.fact_Profitability
                     group by subscription_id 

                   ) 

            );
           
    UPDATE A
    SET prof_db_dt_load_status = 1 FROM fin_insights.db_cos_fcst_config A  
    WHERE run_id = (SELECT max(run_id) FROM fin_insights.db_cos_fcst_config) ;           
           
GRANT SELECT ON TABLE fin_insights.prof_cum_enrolles_db TO ramk;
GRANT ALL ON TABLE fin_insights.prof_cum_enrolles_db TO ramdassu;
GRANT SELECT ON TABLE fin_insights.prof_cum_enrolles_db TO tskav;
GRANT SELECT ON TABLE fin_insights.prof_cum_enrolles_db TO auto_prdii;
GRANT SELECT ON TABLE fin_insights.prof_cum_enrolles_db TO srv_power_bi_fin;
GRANT SELECT ON TABLE fin_insights.prof_gross_margin_by_subid TO ramk;
GRANT ALL ON TABLE fin_insights.prof_gross_margin_by_subid TO ramdassu;
GRANT SELECT ON TABLE fin_insights.prof_gross_margin_by_subid TO tskav;
GRANT SELECT ON TABLE fin_insights.prof_gross_margin_by_subid TO auto_prdii;
GRANT SELECT ON TABLE fin_insights.prof_gross_margin_by_subid TO srv_power_bi_fin;

    EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'An exception occurred.';
    INSERT INTO fin_insights.profitability_error_log (metricsname,errordescription,errordate) 
    VALUES (
             'Enrollee_Margin',
             'Error message: ' || sqlerrm,
              cast(current_timestamp as datetime)
           );

END;
$$


call fin_insights.sp_load_cumenroll_grossmargin_prof_db();

select count(1) from fin_insights.prof_cum_enrolles_db; --914746 --915101 --964346 --1014896 --1063157 --1113367 --1162751 --1205904 --1268767 --1313926 --1366129 --1390206 --1390397 --1440947 --1488793 --1547815 --87022 --89316 --91870 --126393 --129284 --140877 --113865
select count(1) from fin_insights.prof_gross_margin_by_subid; --15447730 --15453955 --15559048 --15852956 --16024539 --16265641 --16447495 --16682383 --16952244 --17246726 --17371259 --17607555 --17638170 --17900920--18200171 --18405098 --18676013 --18888084 --19097193 --19591268 --19830494 --20057052 --20614778

---------------------------------------------------master stored procedure to load profitability data------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE fin_insights.sp_load_profitability()	
 LANGUAGE plpgsql	
AS $$	
DECLARE is_load_enabled INT;	
        get_monthly_load_flag VARCHAR(MAX) := '';	
BEGIN	
	  get_monthly_load_flag := 'SELECT is_load_enabled FROM fin_insights.prof_monthly_load_flag';
      EXECUTE get_monthly_load_flag INTO is_load_enabled ;	
      IF is_load_enabled = 1 THEN	
         call fin_insights.sp_load_kit_partno_mapping();	
         call fin_insights.sp_load_cust_support_cost();	
         call fin_insights.sp_load_esc_cost();	
         call fin_insights.sp_load_kit_cost();	
         call fin_insights.sp_load_logistics_cost();	
         call fin_insights.sp_load_ptb_warranty_cost();	
         call fin_insights.sp_load_costofsales();	
         call fin_insights.sp_load_new_enrollee();	
         call fin_insights.sp_load_bounty();	
         call fin_insights.sp_load_revshare();	
         call fin_insights.sp_load_P1_P2_Qrt();	
         call fin_insights.sp_load_Revenue();	
         call fin_insights.sp_load_other_cost();	
         call fin_insights.sp_load_marketing_cost();	
         call fin_insights.sp_load_profitability_db();	
         call fin_insights.sp_load_cumenroll_grossmargin_prof_db();	
    ELSE	
         RAISE INFO 'monthly load flag is disabled';	
    END IF;    	
END;	
$$	
-----------------------------------------------------------------------------------------------------------------
