# Databricks notebook source
# MAGIC %pip install mlflow --upgrade
# MAGIC %restart_python

# COMMAND ----------

from mlflow.models import ModelConfig

from src.data import download_statcan_table, download_pdf

# COMMAND ----------

config = ModelConfig(development_config="../config.yaml").get('data')
catalog = config.get('catalog')
schema = config.get('schema')
raw_data_volume = config.get('raw_data_volume')
pdfs = config.get('pdfs')
product_tables = config.get('product_tables')
output_dir = f"/Volumes/{catalog}/{schema}/{raw_data_volume}/"

# COMMAND ----------

for product_id, description in product_tables.items():
  print(product_id, description)
  try:
      df = download_statcan_table(product_id, output_dir=output_dir)
      print(f"Successfully processed table {product_id}")
      print(f"Shape: {df.shape}")
      print(f"Columns: {', '.join(df.columns[:5])}...")
      print("-" * 50)
  except Exception as e:
      print(f"Error processing table {product_id}: {str(e)}")

# COMMAND ----------

for product_id, description in product_tables.items():
  print(product_id, description)
  table_path = f"{catalog}.{schema}.{description}"
  file_path = f"{output_dir}{product_id}.parquet"
  spark.read.parquet(file_path).write.saveAsTable(table_path)
