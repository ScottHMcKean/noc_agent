from functools import partial
from typing import List, Dict, Union
from dataclasses import asdict

from langchain_core.messages import AIMessage, HumanMessage

from mlflow.types.llm import (
    ChatMessage,
    ChatCompletionResponse,
    ChatChoice,
    ChatChoiceDelta,
    ChatChunkChoice,
    ChatCompletionChunk,
)


def format_generation(role: str, generation) -> Dict[str, str]:
    """
    Reformat Chat model response to a list of dictionaries. This function
    is called within the graph's nodes to ensure a consistent chat
    format is saved in the graph's state
    """
    return [{"role": role, "content": generation.content}]


format_generation_user = partial(format_generation, "user")
format_generation_assistant = partial(format_generation, "assistant")


def get_pages_from_messages(messages: List[Union[AIMessage, HumanMessage]]):
    """
    Extract all page numbers from the response of the vector search tool
    """
    # Find the index of the message with the vector search tool call
    idx = next(
        i
        for i, msg in enumerate(messages)
        if isinstance(msg, AIMessage)
        and any(
            tool_call["function"]["name"] == "shm__noc_agent__vector_search"
            for tool_call in msg.additional_kwargs.get("tool_calls", [])
        )
    )

    # Extract all page numbers from the response
    pages_str = messages[idx + 1].content
    pages_to_download = []

    # Use string manipulation to find all page numbers
    import re

    page_matches = re.findall(r"Pages:\[(\d+),\s*(\d+)\]", pages_str)
    for match in page_matches:
        pages_to_download.extend([int(match[0]), int(match[1])])

    # Remove duplicates and sort
    pages_to_download = sorted(list(set(pages_to_download)))
    return pages_to_download


def react_agent_to_chat_response(response: Dict[str, str]) -> Dict:
    """
    Reformat the applications responses to conform to the ChatCompletionResponse
    required by Databricks Mosaic AI Agent Framework
    """

    answer = response["messages"][-1].content
    history = response["messages"][:-1]
    pages = get_pages_from_messages(response["messages"])

    return asdict(
        ChatCompletionResponse(
            choices=[ChatChoice(message=ChatMessage(role="assistant", content=answer))],
            custom_outputs={
                "history": history,
                "pages": pages,
            },
        )
    )


def format_chat_response_for_mlflow(answer, history=None, documents=None, stream=False):
    """
    Reformat the LangGraph dictionary output into mlflow chat model types.
    Streaming output requires the ChatCompletionChunk type; batch (invoke)
    output requires the ChatCompletionResponse type.

    The models answer to the users question is returned. The messages history,
    if it exists, is returned as a custom output within the chat message type.
    """
    if stream:
        chat_completion_response = ChatCompletionChunk(
            choices=[
                ChatChunkChoice(delta=ChatChoiceDelta(role="assistant", content=answer))
            ]
        )

    else:
        chat_completion_response = ChatCompletionResponse(
            choices=[ChatChoice(message=ChatMessage(role="assistant", content=answer))]
        )

    if history:
        chat_completion_response.custom_outputs = {
            "message_history": history,
            "documents": documents,
        }

    return chat_completion_response
