{# This macro encode the credit loan column to numeric #}

{% macro encode_credit_loan(column_name) -%}
    
    case {{ column_name }}
        when 'yes' then 1
        when 'no' then 2
        when 'unknown' then 3
    end

{%- endmacro %}        