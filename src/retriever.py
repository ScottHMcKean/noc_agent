from src.config import Config
from databricks_langchain.vectorstores import DatabricksVectorSearch
from langchain_core.vectorstores import VectorStoreRetriever


def get_vector_retriever(config: Config) -> VectorStoreRetriever:
    catalog = config.data.uc_catalog
    schema = config.data.uc_schema
    index_name = config.retriever.index_name

    vector_search = DatabricksVectorSearch(
        endpoint=config.retriever.endpoint_name,
        index_name=f"{catalog}.{schema}.{index_name}",
    )

    retriever = vector_search.as_retriever(
        search_kwargs={
            "k": config.retriever.parameters.k,
            "score_threshold": config.retriever.parameters.score_threshold,
            "query_type": config.retriever.parameters.query_type,
        }
    )

    return retriever


def format_documents(config: Config, docs):
    chunk_template = config.retriever.chunk_template
    chunk_contents = [
        chunk_template.format(
            chunk_text=d.page_content,
            document_uri=d.metadata[config.retriever.mapping.document_uri],
        )
        for d in docs
    ]
    return "".join(chunk_contents)


def index_exists(client, vs_endpoint, index_name):
    try:
        client.get_index(vs_endpoint, index_name)
        return True
    except Exception as e:
        if "IndexNotFoundException" in str(e):
            return False
        else:
            raise e
