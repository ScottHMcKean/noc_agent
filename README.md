# NOC Agent
This repository is an example accelerator to develop an agent to chat with National Occupational Classification.

# Running the NOC Agent

## Install Requirements

This project uses poetry to manage dependencies. Please refer to the [poetry project](https://python-poetry.org/docs/#installation) for more information.

```bash
poetry install
```

## Download the Data

See the [download_data.py](download_data.py) script for more information.
#TODO: Automate loading the data into Databricks via Databricks Connect

## Parse the Documents

See the [parse_noc_pdf.py](parse_noc_pdf.py) script for more information.

## Deploy the Vector Database

See the [build_vector_db.py](build_vector_db.py) script for more information.

## Setup the Genie Space

See the [build_genie_space.py](build_genie_space.py) script for more information.

## Build the Agent

See the [build_agent.py](build_agent.py) script for more information.

## Deploy the Agent

See the [deploy_agent.py](deploy_agent.py) script for more information.