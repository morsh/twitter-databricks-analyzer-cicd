az login --service-principal --username $SERVICE_PRINCIPAL_APP_ID --password $SERVICE_PRINCIPAL_PASSWORD --tenant $SERVICE_PRINCIPAL_TENANT_ID
az account list

echo "az login successfull"