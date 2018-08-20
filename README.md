# Social Posts Pipeline [![Build Status](https://travis-ci.org/morsh/social-posts-pipeline.svg?branch=master)](https://travis-ci.org/morsh/social-posts-pipeline)

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

# Make Options

- `make test_environment`: Test python environment is setup correctly
- `make requirements`: Install Python Dependencies
- `make deploy_resources`: Deploy infrastructure (Just ARM template)
- `make create_secrets`: Create secrets in Databricks
- `make configure_databricks`: Configure Databricks
- `make deploy`: Deploys entire solution
- `make clean`: Delete all compiled Python files
- `make lint`: Lint using flake8
- `make create_environment`: Set up python interpreter environment

# Integration Tests

Main Assumption: The current design of the integration test pipeline, enables only one test to run e-2-e at any given moment, becuase of shared resources.
That said, in case the integration tests are able to spin-up/down an entire environment, that would not be an issue, since each test runs on an encapsulated environment.

## Connect to Travis-CI
This project displays how to connect [Travis-CI](https://travis-ci.org) to enable continuous integration and e2e validation.
To achieve that you need to perform the following tasks:

- Make sure to deploy a test environment using the make script
- Create a new Service Principal on Azure using [azure cli](https://docs.microsoft.com/en-us/cli/azure/create-an-azure-service-principal-azure-cli?toc=%2Fen-us%2Fazure%2Fazure-resource-manager%2Ftoc.json&bc=%2Fen-us%2Fazure%2Fbread%2Ftoc.json&view=azure-cli-latest) or [azure portal](https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal?view=azure-cli-latest)
- Make sure to give the service principals permission on you azure subscription
- Set the following environment variables
  - `DATABRICKS_ACCESS_TOKEN` - Access Token you created on the databricks portal
  - `DATABRICKS_URL` - Regional address of databrick, i.e. https://westeurope.azuredatabricks.net
  - `SERVICE_PRINCIPAL_APP_ID` - Service Principal Application ID
  - `SERVICE_PRINCIPAL_SUBSCRIPTION_ID` - Service Principal Subscription ID
  - `SERVICE_PRINCIPAL_PASSWORD` - Service Principal Password/Key
  - `SERVICE_PRINCIPAL_TENANT_ID` - Tenant ID your resources exist
- Connect travis ci to your github repo

# Potential Issues

> org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 145.0 failed 4 times, most recent failure: Lost task 0.3 in stage 145.0 (TID 1958, 10.139.64.4, executor 0): org.apache.spark.SparkException: Failed to execute user defined function($anonfun$9: (string) => string)

This issue may be `Caused by: org.apache.http.client.HttpResponseException: Too Many Requests` due to cognitive services throtteling limit on API requests.

> java.util.NoSuchElementException: An error occurred while enumerating the result, check the original exception for details.

> ERROR PoolWatchThread: Error in trying to obtain a connection. Retrying in 7000ms 
> java.security.AccessControlException: access denied org.apache.derby.security.SystemPermission( 'engine', 'usederbyinternals' )

These issue may be cause by DBR 4+ versions. You get rid of those issues by using the initialization notebook to run the script:

```scala
// Fix derby permissions
dbutils.fs.put("/databricks/init/fix-derby-permissions.sh", s"""
#!/bin/bash
cat <<EOF > ${System.getProperty("user.home")}/.java.policy
grant {
     permission org.apache.derby.security.SystemPermission "engine", "usederbyinternals";
};
EOF
""", true)
```