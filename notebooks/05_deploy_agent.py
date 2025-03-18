# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Our React Agent

# COMMAND ----------


# COMMAND ----------

input_example = {
    "messages": [
        {
            "role": "human",
            "content": "What is the regulation around building temporary encampments?",
        }
    ]
}

with mlflow.start_run():
    mlflow.langchain.autolog()
    result = chain.invoke(input_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log
# MAGIC Log the agent using mlflow

# COMMAND ----------

# Setup tracking and registry
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

# Setup experiment
mlflow.set_experiment(f"/Users/{USERNAME}/elaws")

# Setup retriever schema
mlflow.models.set_retriever_schema(
    primary_key=sls_config.retriever.mapping.primary_key,
    text_column=sls_config.retriever.mapping.chunk_text,
    doc_uri=sls_config.retriever.mapping.document_uri,
)

# Signature
from mlflow.models import ModelSignature
from mlflow.types.llm import CHAT_MODEL_INPUT_SCHEMA, CHAT_MODEL_OUTPUT_SCHEMA

signature = ModelSignature(
    inputs=CHAT_MODEL_INPUT_SCHEMA, outputs=CHAT_MODEL_OUTPUT_SCHEMA
)

# Setup passthrough resources
from mlflow.models.resources import (
    DatabricksVectorSearchIndex,
    DatabricksServingEndpoint,
)

databricks_resources = [
    DatabricksServingEndpoint(endpoint_name=sls_config.model.endpoint_name),
    DatabricksVectorSearchIndex(index_name=sls_config.retriever.index_name),
]

# Get packages from requirements to set standard environments
with open("requirements.txt", "r") as file:
    packages = file.readlines()
    package_list = [pkg.strip() for pkg in packages]

# COMMAND ----------

# Log the model
with mlflow.start_run():
    logged_agent_info = mlflow.langchain.log_model(
        lc_model=str(implementation_path / "agent.py"),
        model_config=str(implementation_path / "config.yaml"),
        pip_requirements=packages,
        artifact_path="agent",
        code_paths=["src"],
        registered_model_name=sls_config.agent.uc_model_name,
        input_example=input_example,
        signature=signature,
        resources=databricks_resources,
    )

    print(f"Model logged and registered with URI: {logged_agent_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC Test the reloaded model before deploying

# COMMAND ----------

reloaded = mlflow.langchain.load_model(
    f"models:/{sls_config.agent.uc_model_name}/{logged_agent_info.registered_model_version}"
)
result = reloaded.invoke(input_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy
# MAGIC Now we deploy the model

# COMMAND ----------

from mlflow.deployments import get_deploy_client
from databricks import agents

client = get_deploy_client("databricks")

deployment_info = agents.deploy(
    sls_config.agent.uc_model_name,
    logged_agent_info.registered_model_version,
    scale_to_zero=True,
)
