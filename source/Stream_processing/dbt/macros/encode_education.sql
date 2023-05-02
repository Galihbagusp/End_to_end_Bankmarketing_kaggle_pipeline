{# This macro encode the education column to numeric #}

{% macro encode_education(column_name) -%}
    
    case {{ column_name }}
        when 'basic.4y' then 1
        when 'basic.6y' then 2
        when 'basic.9y' then 3
        when 'high.school' then 4
        when 'professional.course' then 5
        when 'unknown' then 6
        when 'university.degree' then 7
        when 'illiterate' then 8
    end

{%- endmacro %}    