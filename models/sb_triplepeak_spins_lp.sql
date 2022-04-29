with basic_calculations as (

select  "Product Level","Category","Subcategory","Channel/Outlet" ,"Brand" , "Product Universe" , "UPC" ,"Description", "Time Period" ,"Time Period End Date", "Geography" , "POSITIONING GROUP" 
			, "PRODUCT TYPE", "Department" ,"STORAGE", "DIET - KETO DIET" , "DIET - PALEO DIET" , "FLAVOR" ,"LABELED NON-GMO", 
			 "LABELED GRASS FED","LABELED ORGANIC" ,"LABELED NITRATE FREE" ,"PLANT BASED","ANIMAL TYPE","PALEO", "UNIT OF MEASURE"
			, sum("Units"						) as "unit sales"
			, sum("Units, Yago"					) as "unit sales ya"
			, sum(cast(case when "Dollars" is null then 0 else "Dollars" end  as float)) as "$ sales"
			, sum(cast(case when "Dollars, Yago" is null then 0 else "Dollars, Yago" end as float)) as "$ sales ya"
			, sum("Dollars, Promo"						) as "sales promo"
			, sum("Dollars, Promo, Yago"					) as "sales promo ya"
			, sum("Units, Promo"							) as "unit sales promo"
			, sum("Units, Promo, Yago"					) as "unit sales promo ya"
			, sum("Base Dollars"						) as "base sales"
			, sum("Base Dollars, Yago"					) as "base sales ya"
			, sum(cast("Dollars" 					as float))-		sum(cast("Base Dollars" 			as float)) as "incremental sales"
			, sum(cast("Dollars, Yago" 				 as float))-	sum(cast("Base Dollars, Yago" 	as float)) as "incremental sales ya"
			, sum("Base Units"								) as "base unit sales"
			, sum("Base Units, Yago"						) as "base unit sales ya"
			, sum("Base Units, Promo"					) as "base unit sales promo"
			, sum("Base Units, Promo, Yago"				) as "base unit sales promo ya"
			, sum("Base Dollars, Promo"					) as "base dollars promo"
			, sum("Base Dollars, Promo, Yago"					) as "base dollars promo ya"
			, sum("TDP"									) as tdp
			, sum("TDP, Yago"							) as "tdp ya"
			, max("Max % ACV"						)     as "max % acv" 
			, max("Max % ACV, Yago"				) as "max $ acv ya"
			, max("Max % ACV"	) - max("Max % ACV, Yago"	) as "max acv pt chg ya"
			, max("Time Period End Date"				) as "last update date"
			, max("# of Stores"				) as "no of stores selling"
			, max("# of Stores, Yago"			) as "no of stores selling ya"
			, max("Max % ACV, Any Promo") AS "Max % ACV, Any Promo"
			, max("Max % ACV, Any Promo, Yago") AS "Max % ACV, Any Promo, Yago"
			, max("Number of Weeks Selling"				) as "number of weeks selling"
			, max("Number of Weeks Selling, Yago"		) as "number of weeks selling ya"
			, avg((case when "SIZE"  is null then null else cast("SIZE"  as float) end) ) as avg_size
	from {{ source('TRIPLEPEAK_SB', 'TRIPLEPEAK_SPINS_LP') }} msly--public.miltons_spins_lp_2y msly 
	group by "Product Level","Category","Subcategory","Channel/Outlet" ,"Brand" , "Product Universe" , "UPC" ,"Description", "Time Period" ,"Time Period End Date", "Geography" , "POSITIONING GROUP" 
			, "PRODUCT TYPE", "Department" ,"STORAGE", "DIET - KETO DIET" , "DIET - PALEO DIET" , "FLAVOR" ,"LABELED NON-GMO", 
			 "LABELED GRASS FED","LABELED ORGANIC" ,"LABELED NITRATE FREE" ,"PLANT BASED","ANIMAL TYPE","PALEO", "UNIT OF MEASURE"
	--limit 1000
), level_2 as (
select * 
	, sum("$ sales") 			over(partition by "Geography","Time Period","Time Period End Date") "total category sales"
	, sum("$ sales ya") 		over(partition by "Geography","Time Period","Time Period End Date") "total category sales ya"
	, sum("unit sales") 		over(partition by "Geography","Time Period","Time Period End Date") "total category unit sales"
	, sum("unit sales ya") 		over(partition by "Geography","Time Period","Time Period End Date") "total category unit sales ya"
	, sum(tdp) 					over(partition by "Geography","Time Period","Time Period End Date") "total category tdp"
	, "$ sales" -	"$ sales ya"		 "$ sales change ya"
	, cast("$ sales" as float) - 								cast("sales promo" 				as float)		"sales non promo"
	, cast("$ sales ya" as float) - 							cast("sales promo ya" 			as float)		"sales non promo ya"
	, cast("unit sales ya" as float) - 						cast("base unit sales ya" 		as float)		"incremental unit sales ya"
	, cast(tdp as float)- 									cast("tdp ya"  					as float)		as "tdp change ya"
	, cast("unit sales" as float)-							cast("unit sales ya"  			as float)		as "unit sales change ya"
	, cast("unit sales ya" as float)-						cast("unit sales promo ya"  	as float)			as "unit sales non promo ya"	
	, "incremental sales"-"incremental sales ya" as "change due to promotion"
	
	from basic_calculations
)
select * from level_2