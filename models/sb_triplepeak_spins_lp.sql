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

select   	"Product Level" as "PRODUCT LEVEL","Category" as CATEGORY,"Subcategory" "SUBCATEGORY","Channel/Outlet" "CHANNEL/OUTLET","Brand" "BRAND", "Product Universe" "PRODUCT UNIVERSE", "UPC" ,"Description" as "PRODUCT DESCRIPTION"
			, case when "Time Period"='4 Weeks' then '04 Weeks' else "Time Period" end "TIME PERIOD"
			, CASE when "Time Period"='4 Weeks' then '04W' 
		 	   when "Time Period"='12 Weeks' then '12W' 
			   when "Time Period"='24 Weeks' then '24W' 
			   when "Time Period"='52 Weeks' then '52W' 
			   else "Time Period" end as "TIME PERIOD ABR"
			,"Time Period End Date" "TIME PERIOD END DATE", "Geography" "GEOGRAPHY", "POSITIONING GROUP" "POSITIONING GROUP" 
			, "PRODUCT TYPE" "PRODUCT TYPE", "Department" "DEPARTMENT","STORAGE" "STORAGE","PLANT BASED" "PLANT BASED","UNIT OF MEASURE" "UNIT OF MEASURE", 
			{% for item in results_list %}
    			"{{item}}" as ATTRIBUTE{{loop.index}}{%if not loop.last%},{% endif %}
			{% endfor %}
			, sum("Units"						) as "UNIT SALES"
			, sum("Units, Yago"					) as "UNIT SALES YA"
			, sum(cast(case when "Dollars" is null then 0 else "Dollars" end  as float)) as "$ SALES"
			, sum(cast(case when "Dollars, Yago" is null then 0 else "Dollars, Yago" end as float)) as "$ SALES YA"
			, sum("Dollars, Promo"						) as "SALES PROMO"
			, sum("Dollars, Promo, Yago"					) as "SALES PROMO YA"
			, sum("Units, Promo"							) as "UNIT SALES PROMO"
			, sum("Units, Promo, Yago"					) as "UNIT SALES PROMO YA"
			, sum("Base Dollars"						) as "BASE SALES"
			, sum("Base Dollars, Yago"					) as "BASE SALES YA"
			, sum(cast("Dollars" 					as float))-		sum(cast("Base Dollars" 			as float)) as "INCREMENTAL SALES"
			, sum(cast("Dollars, Yago" 				 as float))-	sum(cast("Base Dollars, Yago" 	as float)) as "INCREMENTAL SALES YA"
			, sum("Base Units"								) as "BASE UNIT SALES"
			, sum("Base Units, Yago"						) as "BASE UNIT SALES YA"
			, sum("Base Units, Promo"					) as "BASE UNIT SALES PROMO"
			, sum("Base Units, Promo, Yago"				) as "BASE UNIT SALES PROMO YA"
			, sum("Base Dollars, Promo"					) as "BASE DOLLARS PROMO"
			, sum("Base Dollars, Promo, Yago"					) as "BASE DOLLARS PROMO YA"
			, sum("TDP"									) as TDP
			, sum("TDP, Yago"							) as "TDP YA"
			, max("Max % ACV"						)     as "MAX % acv" 
			, max("Max % ACV, Yago"				) as "MAX $ ACV YA"
			, max("Max % ACV"	) - max("Max % ACV, Yago"	) as "MAX ACV PT CHG YA"
			, max("Time Period End Date"				) as "LAST UPDATE DATE"
			, max("# of Stores"				) as "NO OF STORES SELLING"
			, max("# of Stores, Yago"			) as "NO OF STORES SELLING YA"
			, max("Max % ACV, Any Promo") AS "MAX % ACV, Any PROMO"
			, max("Max % ACV, Any Promo, Yago") AS "MAX % ACV, Any PROMO, YA"
			, case when max("Number of Weeks Selling") is null then 0 else max("Number of Weeks Selling") end  as "NUMBER OF WEEKS SELLING"
			, case when max("Number of Weeks Selling, Yago") is null then 0 else max("Number of Weeks Selling, Yago") end as "NUMBER OF WEEKS SELLING YA"
			, avg((case when "SIZE"  is null then null else cast("SIZE"  as float) end) ) as AVG_SIZE
	from {{ source('TRIPLEPEAK_SB', 'TRIPLEPEAK_SPINS_LP') }} msly--public.miltons_spins_lp_2y msly 
	group by "Product Level","Category","Subcategory","Channel/Outlet" ,"Brand" , "Product Universe" , "UPC" ,"Description"
			, case when "Time Period"='4 Weeks' then '04 Weeks' else "Time Period" end
			, CASE when "Time Period"='4 Weeks' then '04W' 
		 	   when "Time Period"='12 Weeks' then '12W' 
			   when "Time Period"='24 Weeks' then '24W' 
			   when "Time Period"='52 Weeks' then '52W' 
			   else "Time Period" end 
			,"Time Period End Date", "Geography" , "POSITIONING GROUP" 
			, "PRODUCT TYPE", "Department" ,"STORAGE","PLANT BASED","UNIT OF MEASURE", 
			{% for item in results_list %}
    			"{{item}}"{%if not loop.last%},{% endif %}
			{% endfor %}
	--limit 1000
), level_2 as (
select * 
	, sum("$ SALES") 			over(partition by "GEOGRAPHY","TIME PERIOD","TIME PERIOD END DATE") "TOTAL CATEGORY SALES"
	, sum("$ SALES YA") 		over(partition by "GEOGRAPHY","TIME PERIOD","TIME PERIOD END DATE") "TOTAL CATEGORY SALES YA"
	, sum("UNIT SALES") 		over(partition by "GEOGRAPHY","TIME PERIOD","TIME PERIOD END DATE") "TOTAL CATEGORY UNIT SALES"
	, sum("UNIT SALES YA") 		over(partition by "GEOGRAPHY","TIME PERIOD","TIME PERIOD END DATE") "TOTAL CATEGORY UNIT SALES YA"
	, sum(TDP) 					over(partition by "GEOGRAPHY","TIME PERIOD","TIME PERIOD END DATE") "TOTAL CATEGORY TDP"
	, "$ SALES" -	"$ SALES YA"		 "$ SALES CHANGE YA"
	, cast("$ SALES" as float) - 								cast("SALES PROMO" 				as float)		"SALES NON PROMO"
	, cast("$ SALES YA" as float) - 							cast("SALES PROMO YA" 			as float)		"SALES NON PROMO YA"
	, cast("UNIT SALES YA" as float) - 						cast("BASE UNIT SALES YA" 		as float)		"INCREMENTAL UNIT SALES YA"
	, cast(tdp as float)- 									cast("TDP YA"  					as float)		as "TDP CHANGE YA"
	, cast("UNIT SALES" as float)-							cast("UNIT SALES YA"  			as float)		as "UNIT SALES CHANGE YA"
	, cast("UNIT SALES YA" as float)-						cast("UNIT SALES PROMO YA"  	as float)			as "UNIT SALES NON PROMO YA"	
	, "INCREMENTAL SALES"-"INCREMENTAL SALES YA" as "CHANGE DUE TO PROMOTION"
	
	from basic_calculations
)
select * from level_2