import clickhouse_connect
import splink.comparison_library as cl  # type: ignore
from splink import Linker, SettingsCreator, block_on
from splinkclickhouse import ClickhouseServerAPI  # type: ignore
import pandas as pd

client = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username="default",
    password="",
    database="mydb",
)

db_api = ClickhouseServerAPI(client)

query = """
SELECT 
    unique_id,
    data_provider_origin_id,
    data_provider_company_id,
    name,
    name_cleaned,
    domain,
    domain_without_tld,
    linkedin_slug,
    host,
    url,
    info,
    created_at,
    updated_at,
    longest_word,
    words
FROM stg_company_landing
"""

df = client.query_df(query)

print(f"Loaded {len(df)} records from stg_company_landing table")

settings = SettingsCreator(
    link_type="dedupe_only",
    unique_id_column_name="unique_id",
    comparisons=[
        cl.NameComparison("url"),
        cl.NameComparison("host"),
        cl.NameComparison("linkedin_slug"),
        cl.NameComparison(
            "name_cleaned", jaro_winkler_thresholds=[0.85, 0.9, 0.95, 0.98]
        ),
        cl.NameComparison("domain"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("domain"),
        block_on("url"),
        block_on("host"),
        block_on("linkedin_slug"),
        block_on("data_provider_origin_id", "data_provider_company_id"),
        block_on("domain_without_tld"),
    ],
)

linker = Linker(df, settings, db_api)

print("Training the model...")

deterministic_rules = [
    "l.domain = r.domain and l.domain != '' and l.domain is not null",
    "l.url = r.url and l.url != '' and l.url is not null",
    "l.host = r.host and l.host != '' and l.host is not null",
    "l.linkedin_slug = r.linkedin_slug and l.linkedin_slug != '' and l.linkedin_slug is not null",
    "l.data_provider_origin_id = r.data_provider_origin_id and l.data_provider_company_id = r.data_provider_company_id and l.data_provider_origin_id is not null and l.data_provider_company_id is not null",
    "l.name_cleaned = r.name_cleaned and (l.url is null and l.host is null and l.linkedin_slug is null)  and (r.url is not null or r.host is not null or r.linkedin_slug is not null)",
]

linker.training.estimate_probability_two_random_records_match(
    deterministic_rules,
    recall=0.8,
)

linker.training.estimate_u_using_random_sampling(max_pairs=1e5)

linker.training.estimate_parameters_using_expectation_maximisation(block_on("domain"))
linker.training.estimate_parameters_using_expectation_maximisation(block_on("name"))

print("Generating predictions...")

pairwise_predictions = linker.inference.predict(threshold_match_weight=-3)


clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    pairwise_predictions, 0.85
)

df_clusters = clusters.as_pandas_dataframe()

print(f"Generating clusters table...")

cluster_table_name = "company_clusters"

drop_table_query = f"""
DROP TABLE IF EXISTS {cluster_table_name}
"""

client.command(drop_table_query)

create_table_query = f"""
CREATE TABLE IF NOT EXISTS {cluster_table_name} (
    cluster_id UInt32,
    unique_id UInt32,
    data_provider_origin_id String,
    data_provider_company_id String,
    name String,
    name_cleaned String,
    domain String,
    domain_without_tld String,
    linkedin_slug String,
    host String,
    url String,
    info String,
    created_at DateTime,
    updated_at DateTime,
    longest_word String,
) ENGINE = MergeTree()
ORDER BY (cluster_id, data_provider_origin_id, data_provider_company_id)
"""

client.command(create_table_query)

if not df_clusters.empty:
    cluster_data = []
    for _, row in df_clusters.iterrows():
        cluster_data.append(
            [
                int(row["cluster_id"]),
                int(row["unique_id"]),
                str(row["data_provider_origin_id"]),
                str(row["data_provider_company_id"]),
                str(row["name"]),
                str(row["name_cleaned"]),
                str(row["domain"]) if pd.notna(row["domain"]) else "",
                (
                    str(row["domain_without_tld"])
                    if pd.notna(row["domain_without_tld"])
                    else ""
                ),
                str(row["linkedin_slug"]) if pd.notna(row["linkedin_slug"]) else "",
                str(row["host"]) if pd.notna(row["host"]) else "",
                str(row["url"]) if pd.notna(row["url"]) else "",
                str(row["info"]) if pd.notna(row["info"]) else "",
                row["created_at"],
                row["updated_at"],
                str(row["longest_word"]) if pd.notna(row["longest_word"]) else "",
            ]
        )

    client.insert(
        cluster_table_name,
        cluster_data,
        column_names=[
            "cluster_id",
            "unique_id",
            "data_provider_origin_id",
            "data_provider_company_id",
            "name",
            "name_cleaned",
            "domain",
            "domain_without_tld",
            "linkedin_slug",
            "host",
            "url",
            "info",
            "created_at",
            "updated_at",
            "longest_word",
        ],
    )
    print(f"Inserted {len(cluster_data)} records into {cluster_table_name}")

print("\n=== CLUSTERING STATISTICS ===")
print(f"Total records processed: {len(df)}")
print(f"Total clusters created: {df_clusters['cluster_id'].nunique()}")
print(
    f"Average cluster size: {len(df_clusters) / df_clusters['cluster_id'].nunique():.2f}"
)

# Show some example clusters
print("\n=== EXAMPLE CLUSTERS ===")
cluster_sizes = df_clusters.groupby("cluster_id").size().sort_values(ascending=False)
print("Top 100 largest clusters:")

for i, (cluster_id, size) in enumerate(cluster_sizes.head(100).items()):
    cluster_records = df_clusters[df_clusters["cluster_id"] == cluster_id]
    print(f"\n{i+1}. Cluster {cluster_id} (size: {size}):")
    for _, row in cluster_records.head(5).iterrows():
        print(
            f"  - {row['name']} — domain/host/url/linkedin_slug: {row['domain']}|{row['host']}|{row['url']}|{row['linkedin_slug']}"
        )

print(f"\nResults saved to ClickHouse table: {cluster_table_name}")

# Create company_final table

drop_final_table_query = "DROP TABLE IF EXISTS company_final"
client.command(drop_final_table_query)

create_table_query = """
CREATE TABLE IF NOT EXISTS company_final (
    cluster_id UInt32,
    name String,
    possible_names Array(String),
    domain String,
    possible_domains Array(String),
    linkedin_slug String,
    possible_linkedin_slugs Array(String),
    possible_hostnames Array(String)
) ENGINE = MergeTree()
ORDER BY (cluster_id)
"""

client.command(create_table_query)

final_table_query = """
INSERT INTO company_final
WITH aggregated_data AS (
    SELECT 
        cluster_id,
        groupArrayDistinct(name) as possible_names,
        groupArrayDistinct(domain) as possible_domains,
        groupArrayDistinct(host) as possible_hostnames,
        groupArrayDistinct(linkedin_slug) as possible_linkedin_slugs
    FROM company_clusters
    WHERE cluster_id IS NOT NULL
    GROUP BY cluster_id
)
SELECT
    cluster_id,
    possible_names[1] as name,
    possible_names,
    possible_domains[1] as domain,
    possible_domains,
    possible_linkedin_slugs[1] as linkedin_slug,
    possible_linkedin_slugs,
    possible_hostnames
FROM aggregated_data
"""

client.command(final_table_query)

print("Final table created successfully!")
print("Querying final results...")

# Query and display some results
final_results = client.query_df("SELECT * FROM company_final LIMIT 10")
print(f"\n=== FINAL TABLE RESULTS (first 10 rows) ===")
print(final_results.to_string(index=False))



