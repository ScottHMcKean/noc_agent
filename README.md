# NOC Agent
This repository is an example accelerator to develop an agent to chat with National Occupational Classification.

## Running the NOC Agent

### Install Requirements

This project uses poetry to manage dependencies. Please refer to the [poetry project](https://python-poetry.org/docs/#installation) for more information.

```bash
poetry install
```

### Modify the Configuration File

The `config.yaml` file in the root of the project contains all the information for index location, paths, and agent setup in order to configure this example to your own project and Databricks instance.

### Download the Data

See  `01_download_data.py` for more information.
#TODO: Automate loading the data into Databricks via Databricks Connect

### Parse the Documents

See the `02_parse_noc_pdf.py` for more information.

### Deploy the Vector Database

See `03_build_vector_search.py` for more information.

### Prepare our Tables and Unity Functions

See `04_prep_sql_functions.py` for more information.

### Build the Agent

See `build_agent.py` for more information.
