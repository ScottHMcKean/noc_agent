# Databricks notebook source
# MAGIC %md
# MAGIC # Vector Search Preparation

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC %restart_python

# COMMAND ----------

"""
We pull the configuration from the agent configuration. The most important part of the vector index setup is the columns we are going to sync - in order to do filtering and advanced retrieval, we need to make sure those columns are available.
"""

from mlflow.models import ModelConfig

config = ModelConfig(development_config="../agent/config.yaml")

# data
data_config = config.get("data")
catalog = data_config.get("uc_catalog")
schema = data_config.get("uc_schema")
raw_chunk_table = "noc_chunks"

# retriever
vs_config = config.get("retriever")
endpoint = vs_config.get("endpoint_name")
index_name = vs_config.get("index_name")
index_source = vs_config.get("source_table")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's clean up our NOC chunks a bit before writing as an index. This code generates an augmented `text_w_headings` table, as well as extracts the heading codes. That should give our agent a lot of information to work with.

# COMMAND ----------

spark.sql(
    f"""
  CREATE OR REPLACE TABLE {catalog}.{schema}.{index_source} AS
  SELECT
    monotonically_increasing_id() AS id,
    filename AS document_uri,
    pages,
    doc_refs,
    text,
    h1,
    h2,
    h3,
    h4,
    h5,
    CONCAT(
      'Broad Category:', COALESCE(h1, ''),  
      '\n Major Group:', COALESCE(h2, 'None'), 
      '\n Sub-major Group:', COALESCE(h3, 'None'), 
      '\n Minor Group:', COALESCE(h4, 'None'), 
      '\n Unit Group:', COALESCE(h5, 'None'), 
      '\n Description:', COALESCE(text, '')
    ) AS text_w_headings,
    REGEXP_EXTRACT(h1, '(\\\d+)') AS broad_category,
    REGEXP_EXTRACT(h2, '(\\\d+)') AS major_group,
    REGEXP_EXTRACT(h3, '(\\\d+)') AS submajor_group,
    REGEXP_EXTRACT(h4, '(\\\d+)') AS minor_group,
    REGEXP_EXTRACT(h5, '(\\\d+)') AS unit_group
  FROM {catalog}.{schema}.{raw_chunk_table}
"""
)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.{schema}.{index_source} LIMIT 20"))

# COMMAND ----------

spark.sql(
    f"""
    ALTER TABLE {catalog}.{schema}.{index_source}
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
"""
)

# COMMAND ----------

"""
If the index exists already, we will run a sync on the table. If not, we will use the SDK to create the index
"""

from databricks.vector_search.client import VectorSearchClient

client = VectorSearchClient()
try:
    index = client.get_index(endpoint, f"{catalog}.{schema}.{index_name}")
    index.sync()
except:
    index = client.create_delta_sync_index(
        endpoint_name=endpoint,
        source_table_name=f"{catalog}.{schema}.{index_source}",
        index_name=f"{catalog}.{schema}.{index_name}",
        pipeline_type="TRIGGERED",
        primary_key="id",
        embedding_source_column="text_w_headings",
        embedding_model_endpoint_name="databricks-gte-large-en",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## NOC Titles
# MAGIC We can also do a dead simple vector search for semantic matching on titles. We have a
