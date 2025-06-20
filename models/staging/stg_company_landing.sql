{{
  config(
    materialized='view'
  )
}}

WITH enriched_company_landing AS (
SELECT
    *,
    {{ clean_company_name_from_country('name') }} as name_cleaned,
    row_number() over () as unique_id,
    case when domain ilike '%linkedin.com%' then null else firstSignificantSubdomain(coalesce(domain, url, host)) end as domain_without_tld,
    arraySort(arrayFilter(x -> lengthUTF8(x) >= 3, splitByChar(' ', name_cleaned))) AS words
FROM company_landing
)

select *, 

    arrayElement(arrayReverseSort(x -> lengthUTF8(x), words), 1) as longest_word,
    
    arrayElement(arraySort(x -> lengthUTF8(x), words), 1) as shortest_word

  from enriched_company_landing

-- TODO: convert empty strings to null for all columns