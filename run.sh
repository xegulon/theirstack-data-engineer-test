# make dbt-install # once
# make dbt-install-500k # once
dbt run --select stg_company_landing
python entity_resolution_script.py
