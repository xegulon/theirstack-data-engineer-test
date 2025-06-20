{% macro clean_company_name_from_country(column_name) %} trim(
    replaceRegexpAll(
        {{ clean_string(column_name) }},
        '(?i)(malaysia|usa|belgië|suisse|switzerland|polska|españa|deutschland|italia|sverige|nederland|österreich|россия)',
        ''
    )
) {% endmacro %}