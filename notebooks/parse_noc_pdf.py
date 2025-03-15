# Databricks notebook source
from pathlib import Path

from docling.datamodel.base_models import InputFormat
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.pipeline.standard_pdf_pipeline import PdfPipelineOptions
from docling.chunking import HybridChunker
import pandas as pd

from src.parse import make_chunk_dict, clean_up_hierarchy
from src.noc import noc_classes

# COMMAND ----------

test_pdf_path = Path("../data/files/12-583-x2021001-eng.pdf")

# COMMAND ----------

# Create the DocumentConverter

pipeline_options = PdfPipelineOptions()
pipeline_options.do_ocr = False
pipeline_options.do_table_structure = True
pipeline_options.table_structure_options.do_cell_matching = False

doc_converter = DocumentConverter(
    format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
)

result = doc_converter.convert(test_pdf_path)

# COMMAND ----------

# Clean up the hierarchy
result.document = clean_up_hierarchy(result.document)

# COMMAND ----------

# Chunk the document and make a dataframe
chunker = HybridChunker(chunk_size=1000, chunk_overlap=100, merge_list_items=True)
chunks = list(chunker.chunk(result.document))
df = pd.DataFrame([make_chunk_dict(x) for x in chunks])
len(chunks)


# COMMAND ----------

# Filter the dataframe to only include the NOC classes
df_filt = df[df["h1"].isin(noc_classes)]


# COMMAND ----------

# Convert the dataframe to a flat file and a Unity Catalog table
df_filt.to_excel("../data/clean_noc_chunks.xlsx")


# COMMAND ----------
spark.createDataFrame(df_filt).write.mode("overwrite").saveAsTable(
    "shm.noc_agent.noc_chunks"
)
