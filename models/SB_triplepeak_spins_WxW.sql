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
			, sum("Units"						) as UNITS	
			, sum("Units, Yago"					) as UNITS_YAGO
			, sum(cast(case when "Dollars" is null then 0 else "Dollars" end  as float)) as DOLLARS
			, sum(cast(case when "Dollars, Yago" is null then 0 else "Dollars, Yago" end as float)) as DOLLARS_YAGO
			, sum("Dollars, Promo"						) as DOLLARS_PROMO
			, sum("Dollars, Promo, Yago"					) as DOLLARS_PROMO_YAGO
			, sum("Units, Promo"							) as UNITS_PROMO
			, sum("Units, Promo, Yago"					) as UNITS_PROMO_YAGO
			, sum("Base Dollars"						) as BASE_DOLLARS
			, sum("Base Dollars, Yago"					) as BASE_DOLLARS_YAGO
			, sum("Dollars SPM"						) as DOLLARS_SPM
			, sum("Dollars SPM, Yago"					) as DOLLARS_SPM_YAGO
			, sum("Units SPM"						) as UNITS_SPM
			, sum("Units SPM, Yago"					) as UNITS_SPM_YAGO
			, sum(cast("Dollars" 					as float))-		sum(cast("Base Dollars" 			as float)) as INCREMENTAL_SALES
			, sum(cast("Dollars, Yago" 				 as float))-	sum(cast("Base Dollars, Yago" 	as float)) as INCREMENTAL_SALES_YAGO
			, sum("Base Units"								) as BASE_UNITS
			, sum("Base Units, Yago"						) as BASE_UNITS_YAGO
			, sum("Base Units, Promo"					) as BASE_UNITS_PROMO
			, sum("Base Units, Promo, Yago"				) as BASE_UNITS_PROMO_YAGO
			, sum("Base Dollars, Promo"					) as BASE_DOLLARS_PROMO
			, sum("Base Dollars, Promo, Yago"					) as BASE_DOLLARS_PROMO_YAGO
			, sum("TDP, Any Promo"									) as TDP_ANY_PROMO
			, sum("TDP, Any Promo, Yago"							) as TDP_ANY_PROMO_YAGO
			, sum("TDP"									) as TDP
			, sum("TDP, Yago"							) as TDP_YAGO
			, max("Max % ACV"						)     as MAX_ACV 
			, max("Max % ACV, Yago"				) as MAX_ACV_YAGO
			, max("Avg % ACV"						)     as AVG_ACV 
			, max("Avg % ACV, Yago"				) as AVG_ACV_YAGO
			, max("Max % ACV"	) - max("Max % ACV, Yago"	) as MAX_ACV_PT_CHG
			, max("Time Period End Date"				) as LAST_UPDATE_DATE
			, max("# of Stores"				) as NO_OF_STORES_SELLING
			, max("# of Stores, Yago"			) as NO_OF_STORES_SELLING_YAGO
			, max("Max % ACV, Any Promo") AS MAX_ACV_ANY_PROMO
			, max("Max % ACV, Any Promo, Yago") AS MAX_ACV_ANY_PROMO_YAGO
			, max("Weight Weeks, Any Promo") AS WEIGHT_WEEKS_ANY_PROMO
			, max("Weight Weeks, Any Promo, Yago") AS WEIGHT_WEEKS_ANY_PROMO_YAGO
			, case when max("Number of Weeks Selling") is null then 0 else max("Number of Weeks Selling") end  as NUMBER_OF_WEEKS_SELLING
			, case when max("Number of Weeks Selling, Yago") is null then 0 else max("Number of Weeks Selling, Yago") end as NUMBER_OF_WEEKS_SELLING_YAGO
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
	, sum(DOLLARS) 			over(partition by GEOGRAPHY,TIME_PERIOD,TIME_PERIOD_END_DATE) TOTAL_CATEGORY_SALES
	, sum(DOLLARS_YAGO) 		over(partition by GEOGRAPHY,TIME_PERIOD,TIME_PERIOD_END_DATE) TOTAL_CATEGORY_SALES_YAGO
	, sum(UNITS) 		over(partition by GEOGRAPHY,TIME_PERIOD,TIME_PERIOD_END_DATE) TOTAL_CATEGORY_UNIT_SALES
	, sum(UNITS_YAGO) 		over(partition by GEOGRAPHY,TIME_PERIOD,TIME_PERIOD_END_DATE) TOTAL_CATEGORY_UNIT_SALES_YAGO
	, sum(TDP) 					over(partition by GEOGRAPHY,TIME_PERIOD,TIME_PERIOD_END_DATE) TOTAL_CATEGORY_TDP
	, DOLLARS -	DOLLARS_YAGO		 DOLLARS_CHANGE_YAGO
	, cast(DOLLARS as float) - 								cast(DOLLARS_PROMO 				as float)		DOLLARS_NON_PROMO
	, cast(DOLLARS_YAGO as float) - 							cast(DOLLARS_PROMO_YAGO			as float)		DOLLARS_NON_PROMO_YAGO
	, cast(UNITS_YAGO as float) - 						cast(BASE_UNITS_YAGO 		as float)		INCREMENTAL_UNITS_YAGO
	, cast(TDP as float)- 									cast(TDP_YAGO  					as float)		as TDP_CHANGE_YAGO
	, cast(UNITS as float)-							cast(UNITS_YAGO  			as float)		as UNITS_CHANGE_YAGO
	, cast(UNITS_YAGO as float)-						cast(UNITS_PROMO_YAGO  	as float)			as UNITS_NON_PROMO_YAGO	
	, INCREMENTAL_SALES-INCREMENTAL_SALES_YAGO as CHANGE_DUE_TO_PROMOTION
	
	from basic_calculations
)
select * from level_2