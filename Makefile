ch:
	docker compose exec clickhouse clickhouse-client --host localhost --port 9000 --user default --password "" --database default

init-db:
	docker compose cp init_database.sql clickhouse:/tmp/init_database.sql
	docker compose exec clickhouse clickhouse-client --host localhost --port 9000 --user default --password "" --database default -f /tmp/init_database.sql

insert-5k:
	docker compose exec clickhouse clickhouse-client --host localhost --port 9000 --user default --password "" --database mydb -q "INSERT INTO company_landing SELECT * FROM s3('https://media.theirstack.com/ts-data-engineer-test-2025/company_landing_5k.csv', 'CSV')"

insert-50k:
	docker compose exec clickhouse clickhouse-client --host localhost --port 9000 --user default --password "" --database mydb -q "INSERT INTO company_landing SELECT * FROM s3('https://media.theirstack.com/ts-data-engineer-test-2025/company_landing_50k.csv', 'CSV')"

insert-500k:
	docker compose exec clickhouse clickhouse-client --host localhost --port 9000 --user default --password "" --database mydb -q "INSERT INTO company_landing SELECT * FROM s3('https://media.theirstack.com/ts-data-engineer-test-2025/company_landing_500k.csv', 'CSV')"

truncate-landing:
	docker compose exec clickhouse clickhouse-client --host localhost --port 9000 --user default --password "" --database mydb -q "TRUNCATE TABLE company_landing"

truncate-dbt:
	docker compose exec clickhouse clickhouse-client --host localhost --port 9000 --user default --password "" --database mydb -q "DROP VIEW IF EXISTS stg_company_landing"

clean-all:
	docker compose exec clickhouse clickhouse-client --host localhost --port 9000 --user default --password "" --database mydb -q "TRUNCATE TABLE company_landing; TRUNCATE TABLE stg_company_landing; TRUNCATE TABLE int_company_similarity; TRUNCATE TABLE int_company_clusters; TRUNCATE TABLE company_final"

show-tables:
	docker compose exec clickhouse clickhouse-client --host localhost --port 9000 --user default --password "" --database mydb -q "SHOW TABLES"

count-landing:
	docker compose exec clickhouse clickhouse-client --host localhost --port 9000 --user default --password "" --database mydb -q "SELECT count(*) FROM company_landing"

dbt-install:
	pip install -r requirements.txt

dbt-debug:
	dbt debug

dbt-run:
	dbt run

dbt-test:
	dbt test

dbt-docs:
	dbt docs generate
	dbt docs serve

dbt-fresh:
	dbt run --full-refresh

dbt-seed:
	dbt seed

dbt-clean:
	dbt clean