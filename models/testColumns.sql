 {% set colsQuery %}
    Select 'column1' as columName
    union all
    Select 'column2' as columName
    union all
    Select 'column3' as columName
{% endset %}
{% set results = run_query(colsQuery) %}
{% if execute %}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}
select 
{% for item in results_list %}
    {{item}} as ATTRIBUTE_{{loop.index}}{%if not loop.last%},{% endif %}
{% endfor %}
from {{ ref('sb_triplepeak_spins_lp') }} limit 5
