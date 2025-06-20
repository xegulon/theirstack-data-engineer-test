# Senior Data Engineer test - TheirStack, 2025

This is the test for the [Senior Data Engineer (up to 80k€)](https://theirstack.notion.site/Data-Engineer-at-TheirStack-com-1d0885e5e97b8085b4f0c9d22733464b) position at [TheirStack](https://theirstack.com).  

The goal of this test is to build an **entity resolution system** for a table with **company data**.
In the table, companies may appear multiple times, with information coming from multiple sources, and the same company may have multiple names.  
Also, multiple companies may have the same name.  
Jump to the ["some considerations about the data"](#some-considerations-about-the-data) part to learn more.

## Input

This repo contains a Docker Compose file to run a ClickHouse database. Run `docker compose up -d` to run it. Connect to it running `make ch`.

The data to populate it is at <https://media.theirstack.com/ts-data-engineer-test-2025/company_landing.csv>

The table where all the company into is has this schema:

```sql
CREATE TABLE company_landing
(
    data_provider_origin_id UInt32,
    data_provider_company_id String,
    name String,
    domain Nullable(String),
    linkedin_slug Nullable(String),
    info String, -- JSON stored as String in ClickHouse
    created_at DateTime64(3) DEFAULT now(),
    updated_at DateTime64(3) DEFAULT now(),
    host Nullable(String),
    url Nullable(String)
)
ENGINE = ReplacingMergeTree()
ORDER BY (data_provider_origin_id, data_provider_company_id)
```

We provide a dataset with 5k records, another one with 50k records and another one with 500k records. You can populate the table with any of them with any of these commands:

```sql
-- Inserts the 5k records dataset
INSERT INTO company_landing 
SELECT * FROM s3('https://media.theirstack.com/ts-data-engineer-test-2025/company_landing_5k.csv', 'CSV');

-- Inserts the 50k records dataset
INSERT INTO company_landing 
SELECT * FROM s3('https://media.theirstack.com/ts-data-engineer-test-2025/company_landing_50k.csv', 'CSV');

-- Inserts the 500k records dataset
INSERT INTO company_landing 
SELECT * FROM s3('https://media.theirstack.com/ts-data-engineer-test-2025/company_landing_500k.csv', 'CSV');
```

### Some considerations about the data

There is not a common key to identify the same company across the different sources, but there are common attributes that can be used to identify the same company.  

For example, there may be 5 records about the same company, and:

- they all have different company names
- some records may have domain and host information
- some records may have the LinkedIn slug or URL of the company
- some records may have the company industry
- some records may have the same or similar company logos

The company name alone is not enough to identify the same company across the different sources. Using the company name alone, we may end up merging information from different companies that have the same name. And we'd fail to merge multiple records with different names that refer to the same company.  

Your job is to use common attributes to build these "clusters" of rows from the `company_landing` table that belong to the same company, as if they were the connected components of an undirected graph.

![connected components](img/connected%20components.gif)

## Output

Build an entity resolution system that will be able to merge records from multiple records from the company_landing table into a single record.  

You can create as many intermediate tables and migartions on the original table as you need.

This is the proposed schema for the output table:

```sql
CREATE TABLE company_final
(
    name String,
    possible_names Array(String),
    domain Nullable(String),
    possible_domains Array(Nullable(String)),
    linkedin_slug Nullable(String),
    possible_hostnames Array(Nullable(String)),
    ... (info here extracted from the `info` JSON column)
) ENGINE = MergeTree()
ORDER BY (name)
```

It is **not** necessary to extract information from the `info` JSON column such as employee count, industry, etc. - that's out of the scope of this test. But you can also use it if you want more attributes to find common patterns between companies.

Complete this README.md file explaining:

- the approach you took
- the assumptions you made
- the trade-offs you made
- the performance of the solution
- the limitations of the solution
- the alternatives you considered
- how you used Cursor, Windsurf, Claude, ChatGPT or other AI tools to help you
- the possible improvements you would make if you had more time

### To test my work

- Create a python venv

Run the commands:

```bash
bash run.sh
```

Now you have the final table.

### The approach I took

I first cleaned the data (the names, removing terms like ', Inc.', 'gmbh', etc.) and did some normalization.'

Then I used splink with a clickhouse connector, which is a state of the art deduplication library. This help me generate a table of clusters.

Then I generated the final table company_final, containing one row for each cluster.

Then I analyzed the final dataset generated by splink to look for inconsistencies : companies with similar names but that are different should not be in the same cluster

Then I iterated to find better rules, by displaying detailed info about the 50 biggest clusters.

For some rules, I had to enrich the initial data (see stg_company_landing.sql), and I think we can add even more rules of this kind to have an even better clusterization.

The problem is hard and needs continuous refinement.

splink is used because it is battle-tested, our job now is to provide it with the best features / settings so that it can sort out the clusters.

### The assumptions you made

If it is the same domain, or url, or host, or linkedin_slug or at the same time data_provider_origin_id/data_provider_company_id match, then it is the same company

As a consequence of this, sub-entities of companies are considered the same company (even if they have a completely different organization / autonomy in terms of recruitement, which is a sensitive topic in TheirStack's context)

This also applies to entities of a government.

```plain
39. Cluster 6773 (size: 96):
  - Off of the State Comptrollers | domain/url/linkedin_slug: mass.gov
  - Off of State Treas & Rec Genrl | domain/url/linkedin_slug: mass.gov
  - Chelsea Soldier's Home | domain/url/linkedin_slug: mass.gov
  - Department of Youth Services | domain/url/linkedin_slug: mass.gov
  - Supreme Judicial Court | domain/url/linkedin_slug: mass.gov
  - Office of the State Auditor | domain/url/linkedin_slug: mass.gov
  - Dept of Environmental Protect | domain/url/linkedin_slug: mass.gov
  - State Lottery Commission | domain/url/linkedin_slug: mass.gov
  - Center For Health Information And Analysis | domain/url/linkedin_slug: mass.gov
  - Office for Admin and Finance (ANF1000) | domain/url/linkedin_slug: mass.gov
```

I could choose to distinguish these entities by checking if one of namedomain/host/url contains `gov` and treat it differently in this case.

### The performance of the solution

The data cleaning part takes less than 10s.

splink is very performant, in 1min30, 213k rows are processed.

### The limitations of the solution

splink clustering is not deterministic, it uses a probabilistic algorithm.

Sometimes we have things like this:

```plain
34. Cluster 24453 (size: 100):
  - Pratt & Whitney Canada | domain/url/linkedin_slug: pwc.ca
  - PwC Australia | domain/url/linkedin_slug: pwc.com.au
  - PwC | domain/url/linkedin_slug: pwc.com
  - PWC Australia | domain/url/linkedin_slug: pwc.com
  - PwC Thailand | domain/url/linkedin_slug: pwc_thailand
  - PwC Luxembourg | domain/url/linkedin_slug: pwc.com
  - PwC Argentina | domain/url/linkedin_slug: pwc.com
  - Pratt & Whitney Canada | domain/url/linkedin_slug: pratt-&-whitney-canada ⇒ BAD !
  - PwC Deutschland | domain/url/linkedin_slug: pwc.de
  - Öhrlings PricewaterhouseCoopers AB | domain/url/linkedin_slug: pwc.co.uk
```

### How you used Cursor, Windsurf, Claude, ChatGPT or other AI tools to help you

I did eveything with Cursor.

Cursor was bad for generating ClickHouse queries, ChatGPT was best.

I used ChatGPT to show me an overview of the tools generally used in record linkage / deduplication. This is how I found splink (I knew what to look for, I have a bit of background on record linkage). I obviously used web search for fresh results.

I also read the official docs. Same for dbt, I asked ChatGPT to present it to me (never used it before).

I used Cursor to bootstrap a dbt project based on our specific task, then I used it to explain it to step by step.

Splink provides LLM documentation, I used it : <https://moj-analytical-services.github.io/splink/topic_guides/llms/prompting_llms.html> to feed Cursor

### The possible improvements you would make if you had more time

1. ML model for better performance

   I think it is relevant to generate a dataset of a few thousands entries, of the form [company_name_1|company_name_2|is_same] and then train a transformers classifier model on it (it would be cheap).

   I estimate this task to take 1-2 days.

   The data would come from the dataset, and also synthetically generated examples would complete the classe is_same=false if needed (generated by LLM).

   Then we would have a model that classifies whether (similar) company names represent the same company, and it could be used for scoring in the record linkage task.

2. Use a dbt python model instead of a sql model for the splink code

   This dbt feature is not yet available with ClickHouse, but soon it will be.

3. Add a table storing the information that domains/hosts belong to the same company

4. Clean country names from company names (in their original languages)

5. For some specific cases, we might want a custom processing, for example for this one:

   ```plain
   16. Cluster 13468 (size: 186):
   - Rockingham County Schools | domain/url/linkedin_slug: rock.k12.nc.us
   - Las Cruces Public Schools | domain/url/linkedin_slug: las-cruces-public-schools
   - HAYWARD UNIFIED SCHOOL DISTRICT | domain/url/linkedin_slug: husd.k12.ca.us
   - Winston-Salem/Forsyth County Schools | domain/url/linkedin_slug: wsfcs.k12.nc.us
   - Michigan City Area Schools | domain/url/linkedin_slug: mcas.k12.in.us
   - Paterson Public Schools | domain/url/linkedin_slug: paterson.k12.nj.us
   - PATERSON PUBLIC SCHOOLS | domain/url/linkedin_slug: paterson.k12.nj.us
   - Appleton Area School District | domain/url/linkedin_slug: aasd.k12.wi.us
   - Davidson County Schools | domain/url/linkedin_slug: davidson.k12.nc.us
   - Winston Salem/Forsyth County Schools | domain/url/linkedin_slug: wsfcs.k12.nc.us
   ```

   We should try to isolate each school in its own cluster, and this pattern is easy to detect.

6. Some tricky cases must be treated individually. E.g. : State Farm Agent, we should remove all the names ('Chad Sittig - State Farm Agent' -> 'State Farm Agent').

7. Look at domain name registrars to get info about the owners (takes time, but I think it's free-ish). This can help determine if two domains belong to the same company.

8. I've just discovered splink thanks to this test, I would go deeper and finetune the parameters.

9. Embeddings should be useful too.

## FAQ

### How will we evaluate the test?

These are things that we will value positively in your solution:

1. Maintainability:
   1. It's easy to build on the solution and extend it (using more fields to cluster companies by them, such as the logo for example)
   2. It's easy to maintain it - if we add new sources for companies, no or minimal changes have to be made
2. Performance:
   1. Which datasets did you do this test with: 5k, 50k, or 500k records?
   2. Have you tested your solution with the other records? How long did it take?
   3. Does time scale linearly with the number of records? Or what is the time complexity of the solution?
   4. Would your solution work with 10x more data? 100x more data? 1000x more data?
3. Reproducibility:
   1. The solution is broken down into multiple steps, and each step is easy to understand and debug
   2. It's easy to test that each step does what it's supposed to do
4. Simplicity: If you can solve 100% or 90% of the problem with a single tool, that's better than building a system with a lot of moving pieces.
5. Industrialization:
   1. Using frameworks like dbt (or similar tools) to orchestrate and organize your solution is valued positively, as it can improve maintainability, reproducibility, and clarity.
   2. If you made migrations to the original table, or added new tables, you ran them in a way that is easy to reproduce and understand, rather than as one-off commands on the terminal.

### Should I use ClickHouse?

Yes, solve as much as you can of the test in ClickHouse. We're betting on ClickHouse and on solving as much with it as possible. Systems with just a few moving pieces are easier to maintain and extend, and let us keep being a small, lean team.

### Useful links

These may be useful depending on the approach you take:

- [Finding connected components of a graph in ClickHouse](https://fiddle.clickhouse.com/b66efe27-439f-4315-878b-ee190b41cd7c) and [in Python](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.components.connected_components.html)
- [Recursive CTEs in ClickHouse](https://clickhouse.com/blog/clickhouse-release-24-04#recursive-ctes)
- [Running Python code in ClickHouse](https://www.youtube.com/watch?v=Fi6umysVP5w)
# theirstack-data-engineer-test
