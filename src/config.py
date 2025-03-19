from pydantic import BaseModel, ConfigDict
from typing import Dict, Optional
import mlflow
from pathlib import Path


class ConfigModel(BaseModel):
    """
    We use pydantic to help standardize config files.
    We use the ConfigDict to allow extra fields in the config files.
    This also provides extensibility.
    https://docs.pydantic.dev/latest/api/config/
    """

    model_config = ConfigDict(extra="allow")


class ModelParameters(ConfigModel):
    temperature: float
    max_tokens: int


class DataConfig(ConfigModel):
    uc_catalog: str
    uc_schema: str
    raw_data_volume: str
    product_tables: Dict[str, str]
    pdfs: Dict[str, str]


class ModelConfig(ConfigModel):
    endpoint_name: str
    parameters: ModelParameters


class RetrieverParameters(ConfigModel):
    k: int = 5
    query_type: str = "ann"
    score_threshold: float = 0


class RetrieverConfig(ConfigModel):
    tool_name: Optional[str] = None
    tool_description: Optional[str] = None
    endpoint_name: str
    index_name: str
    embedding_model: str
    parameters: RetrieverParameters
    primary_key: str
    text_column: str
    doc_uri: str
    chunk_template: str


class AgentConfig(ConfigModel):
    prompt: str
    streaming: bool = False
    name: str


class Config(ConfigModel):
    data: DataConfig
    agent: AgentConfig
    model: ModelConfig
    retriever: RetrieverConfig


def parse_config(mlflow_config: mlflow.models.ModelConfig) -> Config:
    """
    Parse an mlflow config into a pydantic configuration.
    """
    return Config(**mlflow_config.to_dict())
