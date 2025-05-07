# Databricks notebook source
# MAGIC %md
# MAGIC # NOC Agent - Download Data

# COMMAND ----------

# MAGIC %pip install mlflow --upgrade
# MAGIC %restart_python

# COMMAND ----------

import sys
sys.path.append('..')

# COMMAND ----------

from mlflow.models import ModelConfig
from src.data import download_statcan_table

# COMMAND ----------

config = ModelConfig(development_config="../agent/config.yaml").get("data")
catalog = config.get("uc_catalog")
schema = config.get("uc_schema")
raw_data_volume = config.get("raw_data_volume")
pdfs = config.get("pdfs")
product_tables = config.get("product_tables")
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

# COMMAND ----------

from pdf2image import convert_from_path

# Convert PDF to images
images = convert_from_path(pdf_path, dpi=300)

# Save images
for i, image in enumerate(images):
    image.save(f'../data/images/{i + 1}.webp', 'WEBP')
