with fields as 
(

    Select 'column1' as columName
    union all  
    Select 'column2' as columName
    union all  
    Select 'column4' as columName
    union all  
    Select 'column4' as columName
)
select * from fields