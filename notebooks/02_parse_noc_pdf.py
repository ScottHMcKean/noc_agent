# Databricks notebook source
# MAGIC %md
# MAGIC # Parsing the NOC PDF

# COMMAND ----------

# MAGIC %md
# MAGIC TODO: This section needs to be fixed. There was a change in Docling that made the custom extraction function breakdown. Using cached results for the downstream process.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt --quiet
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Docling Conversions

# COMMAND ----------

from pathlib import Path

from docling.datamodel.base_models import InputFormat
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.pipeline.standard_pdf_pipeline import PdfPipelineOptions
from docling.chunking import HybridChunker
import pandas as pd

from src.parse import make_chunk_dict, clean_up_hierarchy
from src.noc import noc_classes

# COMMAND ----------

from mlflow.models import ModelConfig

config = ModelConfig(development_config="../agent/config.yaml").get("data")
catalog = config.get("uc_catalog")
schema = config.get("uc_schema")
raw_data_volume = config.get("raw_data_volume")
raw_data_dir = f"/Volumes/{catalog}/{schema}/{raw_data_volume}/"
silver_data_dir = f"/Volumes/{catalog}/{schema}/silver/"

# COMMAND ----------

pdf_path = next(Path(raw_data_dir).glob("12-583*.pdf"))

# COMMAND ----------

# Create the DocumentConverter
pipeline_options = PdfPipelineOptions()
pipeline_options.do_ocr = False
pipeline_options.do_table_structure = False
pipeline_options.table_structure_options.do_cell_matching = False

doc_converter = DocumentConverter(
    format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
)

result = doc_converter.convert(pdf_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up the document hierarchy

# COMMAND ----------

# Clean up the hierarchy
result.document = clean_up_hierarchy(result.document)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chunk the Text

# COMMAND ----------

# Chunk the document and make a dataframe
chunker = HybridChunker(chunk_size=1000, chunk_overlap=100, merge_list_items=True)
chunks = list(chunker.chunk(result.document))
df = pd.DataFrame([make_chunk_dict(x) for x in chunks])
len(chunks)

# COMMAND ----------

display(df)

# COMMAND ----------

# Filter the dataframe to only include the NOC classes
df_filt = df[df["h1"].isin(noc_classes)]

# COMMAND ----------

display(df_filt)

# COMMAND ----------

# Convert the dataframe to a flat file and a Unity Catalog table
df_filt.to_excel("../data/clean_noc_chunks.xlsx")

# COMMAND ----------

spark.createDataFrame(df_filt).write.mode("overwrite").saveAsTable(
    "shm.noc_agent.noc_chunks"
)
