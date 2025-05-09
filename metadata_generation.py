from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine, text
from datetime import datetime
import pandas as pd
import numpy as np
import json
import boto3
import uuid
import io


def create_metadata_json(df_top1000):
    blob_metadata = {"Meta Data": []}
    for column in df_top1000.columns:
        dtype = str(df_top1000[column].dtype)
        data_type = (
            'Text' if dtype == 'object' else
            'Decimal' if dtype in ('int64', 'float64') else
            'Date' if 'datetime' in dtype else
            'Unknown'
        )
        field_type = (
            "Dimension" if data_type == "Text" else
            "Measure" if data_type == "Decimal" else
            "Period" if data_type == "Date" else
            "Unknown"
        )
        blob_metadata["Meta Data"].append({
            "Field Name": column,
            "New Name": column,
            "Data Type": data_type,
            "Field Type": field_type
        })
    return blob_metadata


def connect_to_db(table, source_engine):
    try:
        with source_engine.connect() as connection: 
            # Find column names and dtypes
            query_col_dtype = f"""  
                                    SELECT 
                                        COLUMN_NAME, 
                                        DATA_TYPE
                                    FROM INFORMATION_SCHEMA.COLUMNS 
                                    WHERE TABLE_NAME = '{table}'
                                """
                                    
            result_col_dtype = connection.execute(text(query_col_dtype)).fetchall()

            col_dtype_dict = {
                            col: (
                                'Text' if dtype in ('nvarchar', 'varchar') else
                                'Decimal' if dtype in ('float', 'real', 'numeric', 'decimal', 'int', 'bigint', 'smallint', 'tinyint') else
                                'Date' if dtype in ('date', 'datetime', 'datetime2', 'smalldatetime', 'timestamp') 
                                else dtype
                            )
                            for col, dtype in result_col_dtype
                        }
            
            # Query top 1000 rows from the table
            query_top_1000_rows = f"select distinct top 1000* from [{table}]"
            result_top_1000_rows = pd.read_sql_query(query_top_1000_rows, source_engine)
            
            df_top1000 = result_top_1000_rows
            
            for column in df_top1000.columns:
                if df_top1000[column].dtype == 'object':
                    df_top1000[column] = df_top1000[column].apply(lambda x: x.strip() if isinstance(x, str) else x)
                
            json_output = {"Meta Data": []}
            for column in df_top1000.columns:
                data_type = col_dtype_dict.get(column, 'Unknown')
                field_type = (
                    "Dimension" if data_type == "Text" else 
                    "Measure" if data_type == "Decimal" else 
                    "Period" if data_type == "Date" else 
                    "Unknown"
                )
                json_output["Meta Data"].append({
                    "Field Name": column,
                    "New Name": column,
                    "Data Type": data_type,
                    "Field Type": field_type
                })

                # logger.info(f"Samples: {samples} ")
        return json_output
    except Exception as e:
        raise ValueError(f"Database connection error: {e}")


def get_secret(secret_name, region_name="us-east-1"):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = response['SecretString']
        return json.loads(secret)
    except Exception as e:
        raise Exception(f"Error retrieving secret {secret_name}: {str(e)}")


def process_csv_excel_from_azure(source_type, connection_string, container_name, blob_name):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_data = blob_client.download_blob().readall()

        if source_type == 'csv':
            df_blob = pd.read_csv(io.BytesIO(blob_data))
        elif source_type == 'xlsx':
            df_blob = pd.read_excel(io.BytesIO(blob_data))
        else:
            raise ValueError("Unsupported file type. Only CSV and XLSX are supported.")

        df_top1000 = df_blob.head(1000)
        blob_metadata = create_metadata_json(df_top1000)
    
        return blob_metadata

    except Exception as e:
        print(f"Error processing blob: {e}")
        raise


def process_csv_excel_from_s3(source_type, bucket_name, file_key, region):
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3', region_name=region)
        
        # Download file from S3 bucket
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_data = s3_object['Body'].read()

        # Read into pandas DataFrame based on source_type
        if source_type == 'csv':
            df_blob = pd.read_csv(io.BytesIO(file_data))
        elif source_type == 'xlsx':
            df_blob = pd.read_excel(io.BytesIO(file_data))
        else:
            raise ValueError("Unsupported file type. Only CSV and XLSX are supported.")

        # Get top 1000 rows
        df_top1000 = df_blob.head(1000)
        blob_metadata = create_metadata_json(df_top1000)
    
        return blob_metadata
        
    except Exception as e:
        print(f"Error processing file from S3: {e}")
        raise


def get_metadata_json(table, json_output_str):
    try:
        json_output_str = json_output_str.replace("'", '"')
        metadata = json.loads(json_output_str)
        # Create the initial DataFrame from the first metadata entry
        df_metadata = pd.DataFrame([metadata['Meta Data'][0]])
        # Create a list of DataFrames for the rest of the entries
        df_list = [pd.DataFrame([entry]) for entry in metadata['Meta Data'][1:]]
        # Concatenate all DataFrames together
        if df_list:
            df_metadata = pd.concat([df_metadata] + df_list, ignore_index=True)
            
        # Ensure 'New Name' column exists, then fill missing values with values from 'Field Name'
        if 'New Name' not in df_metadata.columns:
            df_metadata['New Name'] = np.nan   
        df_metadata['New Name'].fillna(df_metadata['Field Name'], inplace=True)
        
        return df_metadata
    except Exception as e:
        raise ValueError(f"Error while inserting metadata of {table} into m_datamart_metadata: {e}")


def update_metadata(datamart_id, table_id, df_metadata, engine):
    updated_date = datetime.now()
    field_id = 0
    for i in range(0,len(df_metadata['New Name'])):
        field_id+=1
        field_name = str(list(df_metadata['Field Name'])[i])
        data_type = str(list(df_metadata['Data Type'])[i])
        field_type = str(list(df_metadata['Field Type'])[i])
        display_new_name = str(list(df_metadata['New Name'])[i])

        metadata_id = str(uuid.uuid1())
        query = text("""
            INSERT INTO m_datamart_metadata ([MetaDataId],
                                             [DataMartId],
                                             [FieldId],
                                             [FieldName],
                                             [DisplayFieldName],
                                             [DataType],
                                             [FieldType],
                                             [MeasureType],
                                             [Significance],
                                             [UpdatedDatetime],
                                             [CreatedDate],
                                             [TableId])
            VALUES (:metadata_id, :datamart_id, :field_id, :field_name, :display_new_name,
                    :data_type, :field_type, :measure_type, :significance, :updated_date,
                    :created_date, :table_id)
        """)

        params = {
            "metadata_id": metadata_id,
            "datamart_id": datamart_id,
            "field_id": field_id,
            "field_name": field_name,
            "display_new_name": display_new_name,
            "data_type": data_type,
            "field_type": field_type,
            "measure_type": "",    # empty string for MeasureType
            "significance": "",    # empty string for Significance
            "updated_date": updated_date,
            "created_date": updated_date,
            "table_id": table_id,
        }
        
        with engine.connect() as cnxn:
            cnxn.execute(query, params)
            cnxn.commit()


def transform_metadata_to_json(retrieved_metadata):
    """
    Transforms a Pandas DataFrame containing metadata into the desired JSON format.

    Args:
        df (pd.DataFrame): DataFrame with columns 'FieldName', 'DisplayFieldName',
                           'DataType', and 'FieldType'.

    Returns:
        str: JSON string representing the transformed metadata.
    """
    metadata_list = []
    for index, row in retrieved_metadata.iterrows():
        metadata_item = {
            "MetaDataId": str(row['MetaDataId']),
            "Field Name": row['FieldName'],
            "New Name": row['DisplayFieldName'],
            "Data Type": row['DataType'],
            "Field Type": row['FieldType']
        }
        metadata_list.append(metadata_item)
    output_json = {"Meta Data": metadata_list}
    return json.dumps(output_json, indent=4)


def metadata_generator(event):
    datamart_id = event["datamart_id"]
    table_id = event["table_id"]
    refresh = event["refresh"]  # 'True' or 'False'

    # datamart_id = "68F4413C-FD9A-11EF-BA6C-2CEA7F154E8D"
    # table_id = "664493B1-FF38-11EF-BD32-2CEA7F154E8D"
    # refresh = 'True'

    try:
        username = "lsdbadmin"
        password = "logesys@1"
        server = "logesyssolutions.database.windows.net"
        database = "Insights_DB_Dev"
        logesys_engine = create_engine(f"mssql+pymssql://{username}:{password.replace('@', '%40')}@{server}/{database}")

        query_table_name = f"""
            SELECT TableName 
            FROM m_datamart_tables 
            WHERE DataMartId = '{datamart_id}'
            AND TableId = '{table_id}'
        """
        table_name = pd.read_sql_query(query_table_name, logesys_engine)['TableName'].iloc[0]

        query_source_creds = f"""
            SELECT
                SourceType,
                FilePath,
                UserName,
                Password
            FROM m_datamart_tables
            WHERE DataMartId = '{datamart_id}'
            AND TableId = '{table_id}'
        """
        source_creds = pd.read_sql_query(query_source_creds, logesys_engine)
        source_type = source_creds.iloc[0]['SourceType']
        file_path = source_creds.iloc[0]['FilePath']
        source_username = source_creds.iloc[0]['UserName']
        source_password = source_creds.iloc[0]['Password']

        if source_type != 'table':
            file_path_json = json.loads(file_path.replace("'", '"'))

        query_count_metadata_rows = f"""
                    SELECT COUNT(*)
                    FROM m_datamart_metadata
                    WHERE DataMartId = '{datamart_id}'
                    AND TableId = '{table_id}'
                """

        count_metadata_rows = pd.read_sql_query(query_count_metadata_rows, logesys_engine).iloc[0,0]


        if count_metadata_rows == 0 or refresh == 'True':
            if count_metadata_rows > 0 and refresh == 'True':
                delete_query = text(f"DELETE FROM m_datamart_metadata WHERE datamartid = '{datamart_id}' AND tableid = '{table_id}'")
                with logesys_engine.connect() as cnxn:
                    cnxn.execute(delete_query)
                    cnxn.commit()  
                print(f"Deleted existing metadata for datamartid: {datamart_id}, tableid: {table_id}")

            print(f'Creating metadata for the table:{table_name}')
            if source_type == 'csv' or source_type == 'xlsx':
                if 'connectionString' in file_path_json:
                    connection_string = file_path_json['connectionString']
                    container_name = file_path_json['containerName']
                    blob_name = file_path_json['fileName']
                    json_output = process_csv_excel_from_azure(source_type, connection_string, container_name, blob_name)
                    json_output_str = json.dumps(json_output)
                elif 'bucketName' in file_path_json:
                    bucket_name = file_path_json['bucketName']
                    file_key = file_path_json['fileKey']
                    region = file_path_json['region']
                    json_output = process_csv_excel_from_s3(source_type, bucket_name, file_key, region)
                    json_output_str = json.dumps(json_output)
                else:
                    raise ValueError("Invalid file path format: Azure connection string not found")
            elif source_type == 'table':
                source_server, source_database = file_path.split('//')
                source_engine = create_engine(f"mssql+pymssql://{source_username}:{source_password.replace('@', '%40')}@{source_server}/{source_database}")
                json_output = connect_to_db(table_name, source_engine)
                json_output_str = json.dumps(json_output)
 
            # Inserting metadata into m_datamart_metadata table
            df_metadata_initial = get_metadata_json(table_name, json_output_str)
            # Upload the df into m_datamart_metadata
            update_metadata(datamart_id, table_id, df_metadata_initial, logesys_engine)

            created_metadata_query = f"""
                                    SELECT  
                                        MetaDataId,
                                        FieldName,
                                        DisplayFieldName,
                                        DataType, 
                                        FieldType
                                    FROM m_datamart_metadata
                                    WHERE DataMartId = '{datamart_id}'
                                    AND TableId = '{table_id}'"""
            created_metadata = pd.read_sql_query(created_metadata_query, logesys_engine)
            created_metadata = pd.DataFrame(created_metadata)
            created_metadata_json = transform_metadata_to_json(created_metadata)
            return created_metadata_json
        
        elif count_metadata_rows > 0 and refresh == 'False':
            print(f"Retrieving metadata for the table:{table_name} ")

            retrieve_metadata_query = f"""
                                    SELECT MetaDataId,
                                        FieldName,
                                        DisplayFieldName, 
                                        DataType,
                                        FieldType
                                    FROM m_datamart_metadata
                                    WHERE DataMartId = '{datamart_id}'
                                        """
            retrieved_metadata = pd.read_sql_query(retrieve_metadata_query, logesys_engine)
            retrieved_metadata = pd.DataFrame(retrieved_metadata)
            retrieved_metadata_json = transform_metadata_to_json(retrieved_metadata)
            return retrieved_metadata_json

    except Exception as e:
        error_message = f"Error in metadata_generator: {e}"
        print(error_message)
        return {"status": "error", "message": error_message}

    finally:
        if cnxn:
            cnxn.close()