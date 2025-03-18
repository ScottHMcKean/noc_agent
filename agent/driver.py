# Config
import mlflow
from databricks_langchain import ChatDatabricks
from databricks_langchain.uc_ai import (
    DatabricksFunctionClient,
    UCFunctionToolkit,
    set_uc_function_client,
)

from langgraph.prebuilt import create_react_agent
from langchain_core.runnables import RunnableLambda

from src.config import parse_config
from src.retriever import get_vector_retriever
from src.utils import react_agent_to_chat_response

mlflow_config = mlflow.models.ModelConfig(development_config="./config.yaml")
config = parse_config(mlflow_config)

retriever = get_vector_retriever(config)
model = ChatDatabricks(endpoint=config.model.endpoint_name)

client = DatabricksFunctionClient()
set_uc_function_client(client)

tools = UCFunctionToolkit(
    function_names=[
        "shm.noc_agent.vector_search",
        "shm.noc_agent.get_occupation_employment",
        "shm.noc_agent.get_occupation_wages",
    ]
).tools

agent = create_react_agent(model, tools, prompt=config.agent.prompt)
chain = agent | RunnableLambda(react_agent_to_chat_response)

mlflow.models.set_model(chain)
