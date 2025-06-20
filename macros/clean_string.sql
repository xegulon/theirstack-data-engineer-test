{% macro clean_string(column_name) %}
    replaceRegexpAll(trim({{ normalize_case(clean_double_spaces(clean_characters(column_name))) }}), '\|', '')
{% endmacro %} 

{% macro clean_characters(column_name) %}
    replaceRegexpAll(trim({{ column_name }}), '[\(\)\[\]\{\}\*\^\%\$#\@\!\\\|\"\'\-_]', ' ')
{% endmacro %}

{% macro clean_double_spaces(column_name) %}
    trim(replaceRegexpAll({{ column_name }}, '\\s{2,}', ' '))
{% endmacro %}

{% macro normalize_case(column_name) %}
    lower({{ column_name }})
{% endmacro %}