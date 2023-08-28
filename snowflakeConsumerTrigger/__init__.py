import logging

import azure.functions as func
import snowflake.connector
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    # Check if the HTTP method is a POST request
    if req.method == 'POST':
        # Initialize SecretClient with the managed identity
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url='https://snowflakeauthentication.vault.azure.net/', credential=credential)
        
        # Snowflake connection parameters
        account = secret_client.get_secret('SnowflakeAccount').value
        user = secret_client.get_secret('SnowflakeUsername').value
        password = secret_client.get_secret('SnowflakePassword').value
        # TODO: Real life scenario required below
        warehouse = 'WH_COMPUTE'
        database = 'TEST_API'
        schema = 'INVENTORY'
        view = 'EXAMPLE_VIEW' 
        
        # Create a Snowflake connection
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        
        # Execute a sample query
        cursor = conn.cursor()
        query = f"SELECT * FROM {view}"
        cursor.execute(query)
        
        # Fetch and process query results
        results = cursor.fetchall()
        for row in results:
            # Process and send data to "work tray" service (you can add this logic here)
            print(row)
        
        # Close the connection
        cursor.close()
        conn.close()

        return func.HttpResponse("Data processed successfully.", status_code=200)
    else:
        return func.HttpResponse("Invalid HTTP method.", status_code=400)