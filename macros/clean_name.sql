-- State Farm Agent is one of some specific cases we process one by one

{% macro clean_name(column_name) %} trim(
   
   case when {{ clean_company_name_from_country(column_name) }} ilike '%- State Farm Agent%' then trim(splitByChar('-', {{ clean_company_name_from_country(column_name) }})[1]) else {{ clean_company_name_from_country(column_name) }} end
    
) {% endmacro %}