 {%- set my_list = adapter.get_columns_in_relation(ref('columns')) -%}


select
    {% for item in my_list %}
        {{ item.name }} as {{ item.name | replace('_', '') }}

        {% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}


from {{ ref('columns') }}