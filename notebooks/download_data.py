# Databricks notebook source

# COMMAND ----------
from src.data import download_statcan_table

# COMMAND ----------
product_ids = [
    "14100320",  # Average usual hours and wages by selected characteristics, monthly, unadjusted for seasonality
    "14100310",  # Employment by occupation, monthly, seasonally adjusted
    "14100421",  # Labour force characteristics by occupation, monthly, unadjusted for seasonality
    "14100425",  # Job tenure by occupation, monthly, unadjusted for seasonality
]

for product_id in product_ids:
    try:
        df = download_statcan_table(product_id)
        print(f"Successfully processed table {product_id}")
        print(f"Shape: {df.shape}")
        print(f"Columns: {', '.join(df.columns[:5])}...")
        print("-" * 50)
    except Exception as e:
        print(f"Error processing table {product_id}: {str(e)}")
