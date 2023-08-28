import logging

import azure.functions as func
import snowflake.connector
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from snowflake.connector import connect

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Verkrijg toegang tot Azure Key Vault
        keyvault_name = "snowflakeauthentication"
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=f"https://{keyvault_name}.vault.azure.net", credential=credential)

        # Haal Snowflake-inloggegevens uit Key Vault
        snowflake_account_url = secret_client.get_secret("SnowflakeAccount").value
        snowflake_username = secret_client.get_secret("SnowflakeUsername").value
        snowflake_password = secret_client.get_secret("SnowflakePassword").value

        # Maak verbinding met Snowflake
        con = connect(
            user=snowflake_username,
            password=snowflake_password,
            account=snowflake_account_url,
            warehouse="WH_COMPUTE",
            database="TEST_API",
            schema="INVENTORY"
        )

        # Voer een Snowflake-query uit
        cursor = con.cursor()
        cursor.execute("SELECT * FROM PRODUCTS")
        results = cursor.fetchall()

        return func.HttpResponse(f"Query results: {results}")

    except Exception as e:
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)