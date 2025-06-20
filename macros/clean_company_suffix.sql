{% macro clean_company_suffix(column_name) %}
    trim(replaceRegexpAll({{ clean_string(column_name) }}, 
    '(?i)(,\\s*)?(Pvt\\. Ltd\\.?|CO\.\s?LTD\.|Limited Partnership|Sdn\.?\s?Bhd\.?|Inc\\.?|LP|Ltd\\.?|LLC\\.?|LLP\\.?|gmbh|Pvt\\.|S\\.A\\.|L\.P\.|\\be\\.?v\\b|P\.C\.|L\.L\.C|S/A|S\.p\.A\.)', ''))
{% endmacro %} 