 {% set colsQuery %}
    select CUSTOM_HEADER from {{ source('TRIPLEPEAK_SB','RF_SPINS_CUSTOMATRRIBUTES') }}
    where CLIENT='TRIPLEPEAK' and DATASOURCE='SPINS_LP'
{% endset %}
{% set results = run_query(colsQuery) %}
{% if execute %}
	{% set results_list = results.columns[0].values() %}
{% else %}
	{% set results_list = [] %}
{% endif %}
with basic_calculations as (

select   	"Product Level" as PRODUCT_LEVEL,"Category" as CATEGORY,"Subcategory" SUBCATEGORY,"Channel/Outlet" CHANNEL_OUTLET,"Retail Account Level" RETAIL_ACCOUNT_LEVEL,"Brand" BRAND, "Product Universe" PRODUCT_UNIVERSE, "UPC" UPC,"Description" as PRODUCT_DESCRIPTION
			, case when "Time Period"='4 Weeks' then '04 Weeks' else "Time Period" end TIME_PERIOD
			, CASE when "Time Period"='4 Weeks' then '04W' 
		 	   when "Time Period"='12 Weeks' then '12W' 
			   when "Time Period"='24 Weeks' then '24W' 
			   when "Time Period"='52 Weeks' then '52W' 
			   else "Time Period" end as TIME_PERIOD_ABR
			,"Time Period End Date" TIME_PERIOD_END_DATE, "Geography" GEOGRAPHY,"Geography Level" GEOGRAPHY_LEVEL, "POSITIONING GROUP" POSITIONING_GROUP 
			, "PRODUCT TYPE" PRODUCT_TYPE, "Department" DEPARTMENT,"STORAGE" STORAGE,"PLANT BASED" PLANT_BASED,"UNIT OF MEASURE" UNIT_OF_MEASURE, 
			{% for item in results_list %}
    			"{{item}}" as ATTRIBUTE{{loop.index}}{%if not loop.last%},{% endif %}
			{% endfor %}
			, sum("Units"						) as UNIT_SALES
			, sum("Units, Yago"					) as UNIT_SALES_YA
			, sum(cast(case when "Dollars" is null then 0 else "Dollars" end  as float)) as DOLLAR_SALES
			, sum(cast(case when "Dollars, Yago" is null then 0 else "Dollars, Yago" end as float)) as DOLLAR_SALES_YA
			, sum("Dollars, Promo"						) as DOLLAR_SALES_PROMO
			, sum("Dollars, Promo, Yago"					) as DOLLAR_SALES_PROMO_YA
			, sum("Units, Promo"							) as UNIT_SALES_PROMO
			, sum("Units, Promo, Yago"					) as UNIT_SALES_PROMO_YA
			, sum("Base Dollars"						) as BASE_DOLLAR_SALES
			, sum("Base Dollars, Yago"					) as BASE_DOLLAR_SALES_YA
			, sum("Dollars SPM"						) as DOLLAR_SALES_SPM
			, sum("Dollars SPM, Yago"					) as DOLLAR_SALES_SPM_YA
			, sum("Units SPM"						) as UNIT_SALES_SPM
			, sum("Units SPM, Yago"					) as UNIT_SALES_SPM_YA
			, sum(cast("Dollars" 					as float))-		sum(cast("Base Dollars" 			as float)) as INCREMENTAL_SALES
			, sum(cast("Dollars, Yago" 				 as float))-	sum(cast("Base Dollars, Yago" 	as float)) as INCREMENTAL_SALES_YA
			, sum("Base Units"								) as BASE_UNIT_SALES
			, sum("Base Units, Yago"						) as BASE_UNIT_SALES_YA
			, sum("Base Units, Promo"					) as BASE_UNIT_SALES_PROMO
			, sum("Base Units, Promo, Yago"				) as BASE_UNIT_SALES_PROMO_YA
			, sum("Base Dollars, Promo"					) as BASE_DOLLAR_SALES_PROMO
			, sum("Base Dollars, Promo, Yago"					) as BASE_DOLLAR_SALES_PROMO_YA
			, sum("TDP, Any Promo"									) as TDP_ANY_PROMO
			, sum("TDP, Any Promo, Yago"							) as TDP_ANY_PROMO_YA
			, sum("TDP"									) as TDP
			, sum("TDP, Yago"							) as TDP_YA
			, max("Max % ACV"						)     as MAX_ACV 
			, max("Max % ACV, Yago"				) as MAX_ACV_YA
			, max("Avg % ACV"						)     as AVG_ACV 
			, max("Avg % ACV, Yago"				) as AVG_ACV_YA
			, max("Max % ACV"	) - max("Max % ACV, Yago"	) as MAX_ACV_PT_CHG
			, max("Time Period End Date"				) as LAST_UPDATE_DATE
			, max("# of Stores Selling"				) as NO_OF_STORES_SELLING
			, max("# of Stores Selling, Yago"			) as NO_OF_STORES_SELLING_YA
			, max("Max % ACV, Any Promo") AS MAX_ACV_ANY_PROMO
			, max("Max % ACV, Any Promo, Yago") AS MAX_ACV_ANY_PROMO_YA
			, max("Weight Weeks, Any Promo") AS WEIGHT_WEEKS_ANY_PROMO
			, max("Weight Weeks, Any Promo, Yago") AS WEIGHT_WEEKS_ANY_PROMO_YA
			, case when max("Number of Weeks Selling") is null then 0 else max("Number of Weeks Selling") end  as NUMBER_OF_WEEKS_SELLING
			, case when max("Number of Weeks Selling, Yago") is null then 0 else max("Number of Weeks Selling, Yago") end as NUMBER_OF_WEEKS_SELLING_YA
			, avg((case when "SIZE"  is null then null else cast("SIZE"  as float) end) ) as _SIZE
	from {{ source('TRIPLEPEAK_SB', 'TRIPLEPEAK_SPINS_WXW') }} msly--public.miltons_spins_lp_2y msly 
	group by "Product Level","Category","Subcategory","Channel/Outlet","Retail Account Level","Brand" , "Product Universe" , "UPC" ,"Description"
			, case when "Time Period"='4 Weeks' then '04 Weeks' else "Time Period" end
			, CASE when "Time Period"='4 Weeks' then '04W' 
		 	   when "Time Period"='12 Weeks' then '12W' 
			   when "Time Period"='24 Weeks' then '24W' 
			   when "Time Period"='52 Weeks' then '52W' 
			   else "Time Period" end 
			,"Time Period End Date", "Geography" ,"Geography Level", "POSITIONING GROUP" 
			, "PRODUCT TYPE", "Department" ,"STORAGE","PLANT BASED","UNIT OF MEASURE", 
			{% for item in results_list %}
    			"{{item}}"{%if not loop.last%},{% endif %}
			{% endfor %}
	--limit 1000
), level_2 as (
select * 
, sum(DOLLAR_SALES) 			over(partition by GEOGRAPHY,TIME_PERIOD,TIME_PERIOD_END_DATE) TOTAL_CATEGORY_SALES
	, sum(DOLLAR_SALES_YA) 		over(partition by GEOGRAPHY,TIME_PERIOD,TIME_PERIOD_END_DATE) TOTAL_CATEGORY_SALES_YA
	, sum(UNIT_SALES) 		over(partition by GEOGRAPHY,TIME_PERIOD,TIME_PERIOD_END_DATE) TOTAL_CATEGORY_UNIT_SALES
	, sum(UNIT_SALES_YA) 		over(partition by GEOGRAPHY,TIME_PERIOD,TIME_PERIOD_END_DATE) TOTAL_CATEGORY_UNIT_SALES_YA
	, sum(TDP) 					over(partition by GEOGRAPHY,TIME_PERIOD,TIME_PERIOD_END_DATE) TOTAL_CATEGORY_TDP
	, DOLLAR_SALES -	DOLLAR_SALES_YA		 DOLLAR_SALES_CHANGE_YA
	, cast(DOLLAR_SALES as float) - 								cast(DOLLAR_SALES_PROMO 				as float)		DOLLAR_SALES_NON_PROMO
	, cast(DOLLAR_SALES_YA as float) - 							cast(DOLLAR_SALES_PROMO_YA 			as float)		DOLLAR_SALES_NON_PROMO_YA
	, cast(UNIT_SALES_YA as float) - 						cast(BASE_UNIT_SALES_YA 		as float)		INCREMENTAL_UNIT_SALES_YA
	, cast(TDP as float)- 									cast(TDP_YA  					as float)		as TDP_CHANGE_YA
	, cast(UNIT_SALES as float)-							cast(UNIT_SALES_YA  			as float)		as UNIT_SALES_CHANGE_YA
	, cast(UNIT_SALES_YA as float)-						cast(UNIT_SALES_PROMO_YA  	as float)			as UNIT_SALES_NON_PROMO_YA	
	, INCREMENTAL_SALES-INCREMENTAL_SALES_YA as CHANGE_DUE_TO_PROMOTION
	
	from basic_calculations
)
select * from level_2