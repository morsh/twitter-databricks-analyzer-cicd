# Introduction

The following is a Social Post Sentiment processing pipeline implemented within [Azure Databricks](https://azure.microsoft.com/en-au/services/databricks/). 

## Data Pipeline Architecture
![Pipelin Architecture](/docs/SocialPipeline.png)

# Deployment

Ensure you are in the root of the repository and logged in to the Azure cli by running `az login`.

## Requirements

- [Azure CLI 2.0](https://azure.github.io/projects/clis/)
- [Python virtualenv](http://docs.python-guide.org/en/latest/dev/virtualenvs/) 
- [jq tool](https://stedolan.github.io/jq/download/)
- Check the requirements.txt for list of necessary Python packages. (will be installed by `make requirements`)

## Deployment Machine
The deployment is done using [Python Virtual Environment](https://docs.python-guide.org/dev/virtualenvs/).

- The following works with [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10)
- `virtualenv .`  This creates a python virtual environment to work in.
- `source bin/activate`  This activates the virtual environment.
- TODO: Add _ext.env
- `make requirements`. This installs python dependencies in the virtual environment.
- WARNING: The line endings of the two shell scripts `deploy.sh` and `databricks/configure.sh` may cause errors in your interpreter. You can change the line endings by opening the files in VS Code, and changing in the botton right of the editor.

## Deploy Entire Solution

- To deploy the solution, simply run `make deploy` and fill in the prompts.
- When prompted for a Databricks Host, enter the full name of your databricks workspace host, e.g. `https://westeurope.azuredatabricks.net`  (Or change the zone to the one closest to you)
- When prompted for a token, you can [generate a new token](https://docs.databricks.com/api/latest/authentication.html) in the databricks workspace.
- To view additional make commands run `make`

# Potential Issues

> org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 145.0 failed 4 times, most recent failure: Lost task 0.3 in stage 145.0 (TID 1958, 10.139.64.4, executor 0): org.apache.spark.SparkException: Failed to execute user defined function($anonfun$9: (string) => string)

This issue may be `Caused by: org.apache.http.client.HttpResponseException: Too Many Requests` due to cognitive services throtteling limit on API requests.