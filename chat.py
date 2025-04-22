import requests
import logging
from pathlib import Path
import json
import io
import os

import streamlit as st
from databricks.sdk import WorkspaceClient
from PIL import Image

from src.interface import load_interface_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

workspace_client = WorkspaceClient()
workspace_url = workspace_client.config.host
token = workspace_client.config.token

from mlflow.deployments import get_deploy_client


assert os.getenv('SERVING_ENDPOINT'), "SERVING_ENDPOINT must be set"

def get_user_info():
    headers = st.context.headers
    return dict(
        user_name=headers.get("X-Forwarded-Preferred-Username"),
        user_email=headers.get("X-Forwarded-Email"),
        user_id=headers.get("X-Forwarded-User"),
    )

def load_image(volume_path: str) -> Image.Image:
    """
    Load an image from a Unity Catalog Volume using full path.

    Args:
        volume_path: Full path to image (e.g., '/Volumes/catalog/schema/volume/path/to/image.jpg')

    Returns:
        PIL.Image.Image: The loaded image if successful
        None: If there was an error loading the image

    Raises:
        FileNotFoundError: If image doesn't exist in volume
        ValueError: If image format is invalid or corrupted
        IOError: If there are permission issues or other I/O errors
    """
    try:
        response = workspace_client.files.download(volume_path)
        image_bytes = response.contents.read()
        image = Image.open(io.BytesIO(image_bytes))
        return image
    except Exception as e:
        logging.warning(f"Error loading image: {str(e)}")
        return None

#Streamlit app
user_info = get_user_info()
config = load_interface_config(str(Path(__file__).parent / "config.yaml"))

st.markdown(
    """
    <style>
    .stApp {
        background-color: white;
    }
    /* Title text styling */
    .stApp h1 {
        color: #1E1E1E !important; /* Dark color for title */
        font-weight: 600 !important;
    }
    /* Subtitle/header styling */
    .stApp h2, .stApp h3 {
        color: #2D3B45 !important;
    }
    .stTitleContainer {
        background-color: white !important;
    }
    .stMarkdown {
        color: #333333;
    }
    .stButton button {
        background-color: #ff3621;
        color: white;
    }
    .stButton button:hover {
        background-color: #db2b1d;
    }
    /* Image caption styling */
    .stImage img + div, .stImage figcaption, small {
        color: black !important;
        font-weight: 500 !important;
    }
    </style>
""",
    unsafe_allow_html=True,
)

logo = load_image(
    "/Volumes/shm/noc_agent/noc_page_images/databricks-horizontal.webp"
)
st.image(logo, width=400)
st.title(config.title)
st.write(config.description)

# Query input
query = st.text_input(
    label="Enter your query:",
    placeholder=config.example,
)


if query:
    # Make direct request to the endpoint
    with st.spinner("Processing query..."):
        response = get_deploy_client('databricks').predict(
            endpoint=os.getenv("SERVING_ENDPOINT"),
            inputs={
                "messages": [
            {"role": "user", "content": query},
        ]},
        )

        st.markdown(f"Hi {user_info['user_name']}!")

        ai_response = response["choices"][0]["message"]["content"]
        st.markdown(ai_response, unsafe_allow_html=True)

        # Retrieval Markdown Table

        data = json.loads(response["custom_outputs"]["history"][2]["content"])[
            "value"
        ].replace("noc_result\n", "")

        table_rows = data.split('"\n"')
        markdown_table = "| |\n|-------------------|\n"
        for row in table_rows:
            if row.strip():  # Skip empty rows
                # Replace category and group headings with bolded text
                formatted_row = row.replace('"', "").replace("\n", ", ")

                # Bold the category/group headings by adding ** around them
                for prefix in [
                    "Broad Category:",
                    "Major Group:",
                    "Sub Group:",
                    "Minor Group:",
                    "Unit Group:",
                ]:
                    if prefix in formatted_row:
                        parts = formatted_row.split(prefix, 1)
                        formatted_row = parts[0] + f"**{prefix}**" + parts[1]

                markdown_table += f"| {formatted_row} |\n"

        st.write("## Matching NOC Classes")
        st.markdown(markdown_table)

        # Pages
        st.write("## Retrieved Pages")
        pages = response["custom_outputs"]["pages"]

        cols = st.columns(3)
        for i, page in enumerate(pages):
            img = load_image(f"/Volumes/shm/noc_agent/noc_page_images/{page}.webp")
            if img:
                with cols[i % 3]:
                    st.image(img, use_container_width=True, caption=f"Page {page}")
