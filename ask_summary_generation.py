from FinalCommon import *
import json
import boto3
import datetime
from sqlalchemy import create_engine, text
import pandas as pd 
import numpy as np
import constants


def get_datamart_source_credentials(datamart_id, logesys_engine):
    """
    Get source credentials for all tables in a datamart.
    
    Parameters:
    datamart_id - ID of the datamart
    engine - SQLAlchemy engine for database connection
    
    Returns:
    Dictionary with table information and credentials structured by table name
    """
    query_source_creds = f"""
        SELECT
            TableName,
            TableId,
            SourceType,
            FilePath,
            UserName,
            Password
        FROM m_datamart_tables
        WHERE DataMartId = '{datamart_id}'
    """
    
    source_creds_df = pd.read_sql_query(query_source_creds, logesys_engine)
    
    # Create a dictionary to store credentials by table name
    tables_info = {}
    
    # Common credentials for the datamart (assuming they're the same)
    common_credentials = {
        'file_path': source_creds_df.iloc[0]['FilePath'] if not source_creds_df.empty else None,
        'username': source_creds_df.iloc[0]['UserName'] if not source_creds_df.empty else None,
        'password': source_creds_df.iloc[0]['Password'] if not source_creds_df.empty else None,
        'source_type': source_creds_df.iloc[0]['SourceType'] if not source_creds_df.empty else None
    }
    
    # Create entries for each table
    for _, row in source_creds_df.iterrows():
        table_id = row['TableId']
        if hasattr(table_id, 'hex'):  # Check if it has a hex attribute (UUID objects do)
            table_id = str(table_id)
        elif isinstance(table_id, str) and 'UUID(' in table_id and ')' in table_id:
            # For string representation of UUID, extract the value
            import re
            match = re.search(r'UUID\("(.+?)"\)', table_id)
            if match:
                table_id = match.group(1)
        tables_info[row['TableName']] = {
            'table_id': table_id,
            'file_path': row['FilePath'],
            'source_type': row['SourceType'],
            # Include common credentials as well
            'username': row['UserName'],
            'password': row['Password']
        }
    
    return tables_info, common_credentials


def connect_to_db(table, query, source_engine, col_description_dict):
    try:
        with source_engine.connect() as connection:
            # find column names and dtypes
            query_col_dtype = f"""  
                                    SELECT 
                                        COLUMN_NAME, 
                                        DATA_TYPE
                                    FROM INFORMATION_SCHEMA.COLUMNS 
                                    WHERE TABLE_NAME = '{table}'
                                """
            result_col_dtype = connection.execute(text(query_col_dtype)).fetchall()

            # Create a dictionary from the SQL result
            col_dtype_dict = {col: dtype for col, dtype in result_col_dtype}
            
            
            # Find unique value count for each column
            query_unq_cnt = ""
            count =0 
            for column_name, data_type in result_col_dtype:
                if count == 1: query_unq_cnt = query_unq_cnt + " UNION ALL"
                
                query_unq_cnt = query_unq_cnt + f"""
                                                    SELECT '{column_name}' AS COLUMN_NAME, 
                                                           COUNT(DISTINCT [{column_name}]) AS UNIQUE_COUNT 
                                                    FROM [{table}]
                                                """
                count = 1
            result_unq_cnt = connection.execute(text(query_unq_cnt)).fetchall()

            # Convert result to a dictionary
            unq_cnt_dict = {col: count for col, count in result_unq_cnt}
            
            query_top_1000_rows = f"select distinct top 1000* from [{table}]"
            result_top_1000_rows = pd.read_sql_query(query_top_1000_rows, source_engine)
            
            df = result_top_1000_rows
            
            for column in df.columns:
                if df[column].dtype == 'object':
                    df[column] = df[column].apply(lambda x: x.strip() if isinstance(x, str) else x)
                
            json_output = {}
            for column in df.columns:
                samples = df[column].dropna().unique()[:3]
                samples = [sample.item() if isinstance(sample, np.int64) else 
                        sample.strftime('%Y-%m-%d') if isinstance(sample, (pd.Timestamp, datetime.date)) else 
                        sample.isoformat() if isinstance(sample, (pd.Timedelta, pd.Period)) else 
                        sample.strftime('%Y-%m-%d') if hasattr(sample, 'strftime') else 
                        sample for sample in samples]
                          
                json_output[column] = {
                    "dtype": str(col_dtype_dict.get(column, 'Unknown')),
                    "samples": samples,
                    "unique_item_count":str(unq_cnt_dict.get(column, 'Unknown')),
                    "context": str(col_description_dict.get(column, "No context available"))
                }            
        return json_output
    except Exception as e:
        raise ValueError(f"Database connection error: {e}")


def build_summary_query(table_name):
    return f"""
    SELECT 
        COLUMN_NAME, 
        DATA_TYPE, 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = '{table_name}'
    """

def ask_summary_generator(event):
    datamart_id = event.get('datamart_id')
    s3_summary_bucket = "auto-insights-ask-19-march-2025"
    s3_client = boto3.client('s3')

    tables = {}
    summary_mapping = {}
    table_queries = {}
    output_files = {}

    cnxn, cursor, logesys_engine = sql_connect()

    query_table = f"""
            SELECT TableName 
            FROM m_datamart_tables 
            WHERE DataMartId = '{datamart_id}'
        """

    table_df = pd.read_sql_query(query_table, logesys_engine)

    for idx, row in table_df.iterrows():
        table_name = row['TableName']
        tables[f'table_{idx}'] = table_name

        # Build the table_queries dictionary
        table_queries[table_name] = build_summary_query(table_name)
            
        # Build the output_files dictionary
        output_files[table_name] = f"summary_{table_name}.json"

    try:
        for table, query in table_queries.items():
            query_col_desc = f"""
                            SELECT FieldName, Description 
                            FROM m_datamart_metadata 
                            WHERE TableId IN (
                                SELECT TableId FROM m_datamart_tables WHERE TableName = '{table}'
                            )
                        """
            result_col_desc = pd.read_sql_query(query_col_desc, logesys_engine)
            col_description_dict = {row[0]: row[1] for row in result_col_desc.itertuples(index=False, name=None)}

            ########## Get client's credentials from the database ##########
            tables_info, common_credentials = get_datamart_source_credentials(datamart_id, logesys_engine)

            file_path = common_credentials['file_path']
            source_server, source_database = file_path.split("//")
            source_username = common_credentials['username']
            source_password = common_credentials['password']
            source_type = common_credentials['source_type']
            source_engine = create_engine(f"mssql+pymssql://{source_username}:{source_password.replace('@', '%40')}@{source_server}/{source_database}")

            json_output = connect_to_db(table, query, source_engine, col_description_dict)
            json_output_str = json.dumps(json_output)

            s3_client.put_object(Bucket=s3_summary_bucket, Key=output_files[table], Body=json_output_str)
            constants.logger.info(f"Summary created for table: {table}")

        return {
            'statusCode': 200,
            'body': json.dumps("All files processed successfully")
        }

    except Exception as e:
        constants.logger.info(f"Error processing data: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing data: {str(e)}")
        }