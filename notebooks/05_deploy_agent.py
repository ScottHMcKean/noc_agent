# Databricks notebook source
# MAGIC %md
# MAGIC # NOC Agent - Deploy Agent

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow databricks-langchain databricks-agents uv langgraph==0.3.4
# MAGIC %restart_python

# COMMAND ----------

import mlflow
from src.config import parse_config

# COMMAND ----------

input_example = {
    "messages": [
        {
            "role": "human",
            "content": "I am a haul truck driver working in the oil and gas industry, can you prepare an occupation report for the past 12 months?"
        }
    ]
}

# COMMAND ----------

username = (
  dbutils.notebook.entry_point
  .getDbutils().notebook().getContext()
  .userName().get()
)

# COMMAND ----------

mlflow_config = mlflow.models.ModelConfig(
  development_config="../agent/config.yaml"
  )
config = parse_config(mlflow_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log
# MAGIC Log the agent using mlflow

# COMMAND ----------

from agent.react import tools

# COMMAND ----------

# Setup tracking and registry
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

# Setup experiment
mlflow.set_experiment(f"/Users/{username}/{config.agent.name}")

# Setup retriever schema
mlflow.models.set_retriever_schema(
    primary_key='id',
    text_column='text_w_headings',
    doc_uri='document_uri',
)

# Signature
from mlflow.models import ModelSignature
from mlflow.types.llm import CHAT_MODEL_INPUT_SCHEMA, CHAT_MODEL_OUTPUT_SCHEMA

signature = ModelSignature(
    inputs=CHAT_MODEL_INPUT_SCHEMA, 
    outputs=CHAT_MODEL_OUTPUT_SCHEMA
)

# Setup passthrough resources
from databricks_langchain import VectorSearchRetrieverTool
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint, DatabricksVectorSearchIndex, DatabricksTable
from unitycatalog.ai.langchain.toolkit import UnityCatalogTool

catalog = config.data.uc_catalog
schema = config.data.uc_schema

index_full_name = f"{catalog}.{schema}.{config.retriever.index_name}"

resources = [
    DatabricksServingEndpoint(endpoint_name=config.model.endpoint_name),
    DatabricksVectorSearchIndex(index_name=index_full_name),
    DatabricksTable(table_name='shm.noc_agent.monthly_adjusted_employment_by_occupation'),
    DatabricksTable(table_name='shm.noc_agent.monthly_hours_and_wages')
]

for tool in tools:
    if isinstance(tool, VectorSearchRetrieverTool):
        resources.extend(tool.resources)
    elif isinstance(tool, UnityCatalogTool):
        resources.append(DatabricksFunction(function_name=tool.uc_function_name))

# COMMAND ----------

# Log the model
model_full_name = f"{catalog}.{schema}.{config.agent.name}"

with mlflow.start_run():
    logged_agent_info = mlflow.langchain.log_model(
        lc_model="../agent/react.py",
        model_config="../agent/config.yaml",
        pip_requirements=[
            "mlflow",
            "databricks_langchain",
            "langgraph==0.3.4",
        ],
        artifact_path="agent",
        code_paths=["../src"],
        registered_model_name=model_full_name,
        input_example=input_example,
        signature=signature,
        resources=resources,
    )

    print(f"Model logged and registered with URI: {logged_agent_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC Test the reloaded model before deploying

# COMMAND ----------

reloaded = mlflow.langchain.load_model(
    f"models:/{catalog}.{schema}.{config.agent.name}/{logged_agent_info.registered_model_version}"
)
result = reloaded.invoke(input_example)

# COMMAND ----------

mlflow.models.predict(
  model_uri = logged_agent_info.model_uri,
  input_data=input_example,
  env_manager="uv"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy
# MAGIC Now we deploy the model

# COMMAND ----------

from mlflow.deployments import get_deploy_client
from databricks import agents

client = get_deploy_client("databricks")

deployment_info = agents.deploy(
    f"{catalog}.{schema}.{config.agent.name}",
    logged_agent_info.registered_model_version,
    scale_to_zero=True,
)
