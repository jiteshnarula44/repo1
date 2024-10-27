import os
import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient
from fastapi import FastAPI, HTTPException
from io import BytesIO  # Import BytesIO to handle in-memory binary streams

app = FastAPI()

# Environment Variables
blob_connection_string = "DefaultEndpointsProtocol=https;AccountName=narula12storage;AccountKey=s8rUHL11ngvXxzJMatsIPT1UKaQsXMw61lKTTb7xA4bM2AawsFIpuf0I4Ty5rwsPpqg4t6IDGe6c+AStCavGIg==;EndpointSuffix=core.windows.net"
server = 'jitesh-sql-server.database.windows.net'
database = 'kano.backup'
username = 'training_db_kano'
password = '234vb&Qx5#'
driver = '{ODBC Driver 17 for SQL Server}'

# Initialize Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
container_name = "posjitesh"
output_container_name = "intermediate"
blob_name = "External Rep Agency Distribution Emails.csv"  # The blob file to read

@app.post("/process_sales_data")
async def process_sales_data():
    try:
        # Step 1: Load the CSV directly from Blob Storage into pandas
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)

        # Read the blob directly into a DataFrame using BytesIO
        blob_data = blob_client.download_blob().readall()  # Read the blob content
        df_sales_rep = pd.read_csv(BytesIO(blob_data))  # Use BytesIO to read into pandas DataFrame

        # Step 2: Connect to Azure SQL Database
        conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(conn_str)

        # Step 3: Process each sales rep from the CSV file
        for sales_rep in df_sales_rep.iloc[:, 0]:  # Iterate over the first column
            print(f"Processing sales rep: {sales_rep}")

            # SQL query to get data for the specific sales rep
            sales_data_query = f"""
            SELECT 
                S.Company_External_Sales_Rep,
                S.Distribution_Group_Email,
                S.Internal_Sales_Rep,
                S.Internal_Sales_Rep_Email,
                F.Net_Amount,
                F.Date_Created_Date,
                CASE
                    WHEN F.Date_Created_Date >= DATEADD(MONTH, -3, GETDATE()) THEN 'Less than 3 months'
                    WHEN F.Date_Created_Date BETWEEN DATEADD(MONTH, -6, GETDATE()) AND DATEADD(MONTH, -3, GETDATE()) THEN '3 to 6 months'
                    WHEN F.Date_Created_Date BETWEEN DATEADD(MONTH, -9, GETDATE()) AND DATEADD(MONTH, -6, GETDATE()) THEN '6 to 9 months'
                    WHEN F.Date_Created_Date < DATEADD(MONTH, -12, GETDATE()) THEN 'Greater than 12 months'
                END AS TimeRange
            FROM 
                [stg].[sales_Rep] S 
            JOIN 
                [dwh].[Fact_Transaction_Line] F 
                ON S.Company_External_Sales_Rep = F.Sales_Rep
            WHERE 
                S.Company_External_Sales_Rep = '{sales_rep}'
            """
            
            sales_data_df = pd.read_sql(sales_data_query, conn)

            if sales_data_df.empty:
                print(f"No data found for sales rep: {sales_rep}. Skipping.")
                continue

            # Create an Excel file with different sheets for time ranges
            excel_file_path = f"{sales_rep}_sales_data.xlsx"
            with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
                for time_range, group in sales_data_df.groupby('TimeRange'):
                    group.to_excel(writer, sheet_name=time_range, index=False)
                    print(f"Added sheet for '{time_range}' in {sales_rep} Excel file.")

            # Upload to Azure Blob Storage
            blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=excel_file_path)
            with open(excel_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            print(f"Uploaded '{excel_file_path}' to Blob Storage in container '{output_container_name}'.")

        # Close the database connection
        conn.close()
        
        return {"status": "Success", "message": "Processing and upload completed."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
