import requests
import io
import zipfile
import os
import re
from pathlib import Path
from urllib.parse import urlparse, unquote

import pandas as pd

def clean_column_name(name):
    """
    Clean column names to be lowercase, replace spaces with underscores,
    and remove special characters.
    """
    # Convert to lowercase
    name = name.lower()
    # Replace spaces with underscores
    name = name.replace(" ", "_")
    # Remove special characters except underscores
    name = re.sub(r"[^\w\s]", "", name)
    # Replace multiple underscores with a single one
    name = re.sub(r"_+", "_", name)
    return name

def download_pdf(url: str, output_dir: str = "data/files", filename: str = None) -> Path:
    """
    Download a PDF from a URL and save it to the specified directory.
    
    Args:
        url (str): The URL of the PDF to download
        output_dir (str): Directory to save the PDF to (default: "data/files")
        filename (str, optional): Name to save the file as. If None, extracts from URL
        
    Returns:
        Path: Path object pointing to the downloaded file
        
    Raises:
        Exception: If the download fails or if the response isn't a PDF
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # If no filename provided, extract it from the URL
    if filename is None:
        # Get filename from URL and decode any URL encoding
        filename = unquote(os.path.basename(urlparse(url).path))
        # If no extension or wrong extension, add .pdf
        if not filename.lower().endswith('.pdf'):
            filename += '.pdf'
    
    output_path = Path(output_dir) / filename
    
    # Download the file
    print(f"Downloading PDF from {url}...")
    response = requests.get(url, stream=True)
    
    # Check if the request was successful
    if response.status_code != 200:
        raise Exception(f"Failed to download PDF: HTTP {response.status_code}")
    
    # Check if the content is actually a PDF
    content_type = response.headers.get('content-type', '').lower()
    if 'application/pdf' not in content_type and not url.lower().endswith('.pdf'):
        raise Exception(f"URL does not point to a PDF file. Content-Type: {content_type}")
    
    # Save the file
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    
    print(f"PDF successfully downloaded to {output_path}")
    return output_path

def download_statcan_table(product_id, language="en", output_dir="data"):
    """
    Download a full table from Statistics Canada, clean column names,
    and save it as a parquet file.

    Args:
        product_id (str): The product ID of the table
        language (str): Language for the CSV ('en' for English, 'fr' for French)
        output_dir (str): Directory to save the parquet file

    Returns:
        pandas.DataFrame: The loaded data with cleaned column names
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Construct the URL
    url = f"https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/{product_id}/{language}"

    print(f"Downloading table {product_id}...")

    # Get the download link
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to get download link: {response.status_code}")

    download_link = response.json()["object"]
    print(f"Download link: {download_link}")

    # Download the zip file
    zip_response = requests.get(download_link)
    if zip_response.status_code != 200:
        raise Exception(f"Failed to download zip file: {zip_response.status_code}")

    # Extract the CSV file from the zip
    with zipfile.ZipFile(io.BytesIO(zip_response.content)) as z:
        # Find the CSV file in the zip (assuming there's only one)
        csv_files = [f for f in z.namelist() if f.endswith(".csv")]
        if not csv_files:
            raise Exception("No CSV file found in the zip archive")

        csv_file = csv_files[0]
        print(f"Extracting {csv_file}")

        # Read the CSV file into a pandas DataFrame
        with z.open(csv_file) as f:
            df = pd.read_csv(f)

    # Clean column names
    df.columns = [clean_column_name(col) for col in df.columns]

    # Save as parquet
    output_file = os.path.join(output_dir, f"{product_id}.parquet")
    df.to_parquet(output_file)
    print(f"Saved to {output_file}")

    return df
