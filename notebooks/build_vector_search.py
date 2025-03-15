# Databricks notebook source

"""
We pull the configuration from the agent configuration. The most important part of the vector index setup is the columns we are going to sync - in order to do filtering and advanced retrieval, we need to make sure those columns are available.
"""

from mlflow.models import ModelConfig

config = ModelConfig(development_config="config.yaml")
vs_config = config.get("retriever")
vs_endpoint = vs_config.get("endpoint_name")
vs_index_name = vs_config.get("index_name")
vs_source_table = vs_config.get("source_table")
vs_cols_to_sync = [
    "filename",
    "input_hash",
    "pages",
    "chunk_type",
    "image_path",
    "text",
    "enriched_text",
    "id",
]


# COMMAND ----------
spark.sql(
    f"""
    ALTER TABLE {vs_index_name}
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
    index = client.get_index(vs_endpoint, vs_index_name)
    index.sync()
except:
    index = client.create_delta_sync_index(
        endpoint_name=vs_endpoint,
        source_table_name=vs_source_table,
        index_name=vs_index_name,
        pipeline_type="TRIGGERED",
        primary_key="id",
        embedding_source_column="text",
        embedding_model_endpoint_name="databricks-gte-large-en",
        columns_to_sync=vs_cols_to_sync,
    )
