from sqlalchemy import create_engine, text
from dateutil.relativedelta import relativedelta
from FinalCommon import *
import json
import pandas as pd


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


# def significant_fields(cursor, source_engine, datamart_id, table_id, 
#                        dimensions_dict=None, measures_dict=None, dates_dict=None,
#                        max_year_dict=None, max_month_dict=None, max_date_dict=None,
#                        ty_ly_periods_dict=None,  # Added for TY/LY periods
#                        start_month=4, end_month=3):  # Added start_month and end_month, default fiscal year.):
#     """ 
#     Get significant dimensions, measures, and date columns for a specific table ID.
#     Accumulates results into existing dictionaries if provided.
    
#     Parameters:
#     cursor - database cursor (for datamart)
#     source_engine - SQLAlchemy engine connected to the source/client's database
#     datamart_id - ID of the datamart
#     table_id - ID of the specific table
#     dimensions_dict - existing dictionary of dimensions to update (optional)
#     measures_dict - existing dictionary of measures to update (optional)
#     dates_dict - existing dictionary of date columns to update (optional)
#     max_year_dict - existing dictionary to store max years (optional)
#     max_month_dict - existing dictionary to store max months (optional)
#     max_date_dict - existing dictionary to store max dates (optional)
#     ty_ly_periods_dict - existing dictionary to store TY/LY periods (optional) # Added
#     start_month - int, starting month of fiscal year
#     end_month - int, ending month of fiscal year
    
#     Returns:
#     Tuple of three dictionaries (dimensions, measures, dates) with table name as key
#     """
    
#     # Initialize or use provided dictionaries
#     significant_dimensions_dict = dimensions_dict if dimensions_dict is not None else {}
#     significant_measures_dict = measures_dict if measures_dict is not None else {}
#     date_columns_dict = dates_dict if dates_dict is not None else {}
    
#     max_year_dict = max_year_dict if max_year_dict is not None else {}
#     max_month_dict = max_month_dict if max_month_dict is not None else {}
#     max_date_dict = max_date_dict if max_date_dict is not None else {}
#     ty_ly_periods_dict = ty_ly_periods_dict if ty_ly_periods_dict is not None else {}

#     # Get the table name for this table ID
#     cursor.execute(f"SELECT TableName FROM m_datamart_tables WHERE TableId = '{table_id}'")
#     result = cursor.fetchone()
    
#     if not result:
#         return significant_dimensions_dict, significant_measures_dict, date_columns_dict  # Table not found
        
#     table_name = result.TableName
#     print(f'table_name in sign fields function:{table_name}')
#     # Get significant fields for this table
#     cursor.execute(f"SELECT * FROM m_datamart_metadata WHERE TableId = '{table_id}' AND DataMartId = '{datamart_id}' AND Significance = 'Primary'")
#     records = cursor.fetchall()
    
#     # Initialize lists for this table
#     dimensions = []
#     measures = []
#     dates = []
    
#     # Categorize fields based on data type
#     for r in records:
#         if r.DataType == 'Text':
#             dimensions.append(r.FieldName)
#         elif r.DataType == 'Decimal':
#             measures.append(r.FieldName)
#         elif r.DataType == 'Date':
#             dates.append(r.FieldName)
    
#     # Add non-empty lists to respective dictionaries
#     if dimensions:
#         significant_dimensions_dict[table_name] = dimensions
#     if measures:
#         significant_measures_dict[table_name] = measures
#     if dates:
#         date_columns_dict[table_name] = dates[0] if dates else None
#         # Fetch max year, month, date from source DB
#         for date_column in dates:
#             try:
#                 max_year, max_month, max_date = create_datetime_columns('table', pd.DataFrame(), table_name, source_engine, [], date_column) #Pass empty dataframe
#                 max_year_dict[table_name] = max_year
#                 max_month_dict[table_name] = max_month
#                 max_date_dict[table_name] = max_date
#                 print(f'max_year_dict, max_month_dict, max_date_dict in sign fields in for loop:{max_year_dict, max_month_dict, max_date_dict}')
#                 # Calculate and store TY/LY periods
#                 (ty_start, ty_end, ly_start, ly_end, last_12_start, last_12_end,
#                  last_52_start, last_52_end) = get_ty_ly_start_end_period(start_month, end_month, max_year, max_month, max_date, date_column)
#                 ty_ly_periods_dict[table_name] = {  
#                     'ty_start': ty_start,
#                     'ty_end': ty_end,
#                     'ly_start': ly_start,
#                     'ly_end': ly_end,
#                     'last_12_start': last_12_start,
#                     'last_12_end': last_12_end,
#                     'last_52_start': last_52_start,
#                     'last_52_end': last_52_end,
#                 }
#             except Exception as e:
#                 print(f"Error fetching max date info for table {table_name}, column {date_column}: {e}")
#     print(f'max_year_dict, max_month_dict, max_date_dict in sign fields before return:{max_year_dict, max_month_dict, max_date_dict}')
#     return significant_dimensions_dict, significant_measures_dict, date_columns_dict, max_year_dict, max_month_dict, max_date_dict, ty_ly_periods_dict

# def collect_sig_fields_for_all_tables(cursor, datamart_id, logesys_engine, source_engine):
#     """
#     Collect metadata for all tables in a datamart.
    
#     Parameters:
#     cursor - database cursor
#     datamart_id - ID of the datamart
#     engine - SQLAlchemy engine for database connection
    
#     Returns:
#     Dictionary containing all tables' metadata (dimensions, measures, dates)
#     """
#     # Get all tables for this datamart
#     tables_info, _ = get_datamart_source_credentials(datamart_id, logesys_engine)
    
#     # Initialize result dictionaries
#     all_dimensions = {}
#     all_measures = {}
#     all_dates = {}
#     max_year_dict = {}
#     max_month_dict = {}
#     max_date_dict = {}
#     all_ty_ly_periods = {} 
#     ty_start_date_dict = {}
#     ty_end_date_dict = {}
#     ly_start_date_dict = {}
#     ly_end_date_dict = {}
#     last12months_start_date_dict = {}
#     last12months_end_date_dict = {}
#     last52weeks_start_date_dict = {}
#     last52weeks_end_date_dict = {}
#     date_column_dict = {}
    
#     # Process each table
#     for table_name, table_data in tables_info.items():
#         # Get the table_id
#         table_id = table_data['table_id']
        
#         # Call significant_fields for this table
#         dimensions, measures, dates, max_year, max_month, max_date, ty_ly_periods = significant_fields(
#             cursor,
#             source_engine,
#             datamart_id, 
#             table_id,
#             all_dimensions,
#             all_measures,
#             all_dates,
#             max_year_dict,
#             max_month_dict,
#             max_date_dict,
#             all_ty_ly_periods
#         )
        
#         # Update our accumulated results
#         Significant_dimensions = dimensions
#         Significant_measures = measures
#         date_columns = dates
#         print(f'max_year, max_month, max_date:{max_year, max_month, max_date}')
#         # print(f"Processed table: {table_name}")
#         if table_name in ty_ly_periods:
#             ty_start_date_dict[table_name] = ty_ly_periods[table_name]['ty_start']
#             ty_end_date_dict[table_name] = ty_ly_periods[table_name]['ty_end']
#             ly_start_date_dict[table_name] = ty_ly_periods[table_name]['ly_start']
#             ly_end_date_dict[table_name] = ty_ly_periods[table_name]['ly_end']
#             last12months_start_date_dict[table_name] = ty_ly_periods[table_name]['last_12_start']
#             last12months_end_date_dict[table_name] = ty_ly_periods[table_name]['last_12_end']
#             last52weeks_start_date_dict[table_name] = ty_ly_periods[table_name]['last_52_start']
#             last52weeks_end_date_dict[table_name] = ty_ly_periods[table_name]['last_52_end']
        
#         if table_name:
#             max_year_dict[table_name] = max_year
#             max_month_dict[table_name] = max_month
#             max_date_dict[table_name] = max_date

    # return {
    #     'significant_dimensions': Significant_dimensions,
    #     'significant_measures': Significant_measures,
    #     'date_columns': date_columns,
    #     'max_year_dict': max_year_dict,
    #     'max_month_dict': max_month_dict,
    #     'max_date_dict': max_date_dict,
    #     'ty_ly_periods': all_ty_ly_periods, 
    #     'ty_start_date_dict': ty_start_date_dict,
    #     'ty_end_date_dict': ty_end_date_dict,
    #     'ly_start_date_dict': ly_start_date_dict,
    #     'ly_end_date_dict': ly_end_date_dict,
    #     'last12months_start_date_dict': last12months_start_date_dict,
    #     'last12months_end_date_dict': last12months_end_date_dict,
    #     'last52weeks_start_date_dict': last52weeks_start_date_dict,
    #     'last52weeks_end_date_dict': last52weeks_end_date_dict
    # }


def rename_fields(datamart_id, rename_dim_meas, cnxn, cursor):
    display_fieldname_query = f"""SELECT FieldName, DisplayFieldName FROM m_datamart_metadata
                                    WHERE datamartid = '{datamart_id}'"""
    cursor.execute(display_fieldname_query)

    metadata_rows = cursor.fetchall()
    for row in metadata_rows:
        field_name, display_field_name = row
        if field_name != display_field_name:
            rename_dim_meas[field_name] = display_field_name
    return rename_dim_meas


def significant_fields(cursor, source_engine, datamart_id, table_id, start_month, end_month):
    """
    Get significant dimensions, measures, and date columns for a specific table ID.
    Calculate max_year, max_month, max_date for the first date column if present.

    Parameters:
    cursor - database cursor (for datamart)
    source_engine - SQLAlchemy engine connected to the source/client's database
    datamart_id - ID of the datamart
    table_id - ID of the specific table
    start_month - int, starting month of fiscal year
    end_month - int, ending month of fiscal year

    Returns:
    Tuple containing:
        - dimensions (list)
        - measures (list)
        - date_column (str or None)
        - max_year (int or None)
        - max_month (int or None)
        - max_date (datetime.date or None)
        - ty_start_date (datetime.date or None)
        - ty_end_date (datetime.date or None)
        - ly_start_date (datetime.date or None)
        - ly_end_date (datetime.date or None)
        - last12months_start_date (datetime.date or None)
        - last12months_end_date (datetime.date or None)
        - last52weeks_start_date (datetime.date or None)
        - last52weeks_end_date (datetime.date or None)
    """

    # Get the table name for this table ID
    cursor.execute(f"SELECT TableName FROM m_datamart_tables WHERE TableId = '{table_id}'")
    result = cursor.fetchone()

    if not result:
        return [], [], None, None, None, None, None, None, None, None, None, None, None, None  # Table not found

    table_name = result.TableName
    
    # Get significant fields for this table
    cursor.execute(f"SELECT * FROM m_datamart_metadata WHERE TableId = '{table_id}' AND DataMartId = '{datamart_id}' AND Significance = 'Primary'")
    records = cursor.fetchall()

    # Initialize lists for this table
    dimensions = []
    measures = []
    dates = []

    # Categorize fields based on data type
    for r in records:
        if r.DataType == 'Text':
            dimensions.append(r.FieldName)
        elif r.DataType == 'Decimal':
            measures.append(r.FieldName)
        elif r.DataType == 'Date':
            dates.append(r.FieldName)

    date_column = dates[0] if dates else None
    max_year, max_month, max_date = None, None, None
    ty_start_date, ty_end_date, ly_start_date, ly_end_date, last12months_start_date, last12months_end_date, last52weeks_start_date, last52weeks_end_date = [None] * 8

    if date_column:
        try:
            max_year, max_month, max_date = create_datetime_columns('table', pd.DataFrame(), table_name, source_engine, [], date_column)
            (ty_start, ty_end, ly_start, ly_end, last_12_start, last_12_end,
             last_52_start, last_52_end) = get_ty_ly_start_end_period(start_month, end_month, max_year, max_month, max_date, date_column)
            ty_start_date, ty_end_date, ly_start_date, ly_end_date, last12months_start_date, last12months_end_date, last52weeks_start_date, last52weeks_end_date = ty_start, ty_end, ly_start, ly_end, last_12_start, last_12_end, last_52_start, last_52_end
            
        except Exception as e:
            print(f"Error fetching date info for table {table_name}, column {date_column}: {e}")
    return dimensions, measures, date_column, max_year, max_month, max_date, ty_start_date, ty_end_date, ly_start_date, ly_end_date, last12months_start_date, last12months_end_date, last52weeks_start_date, last52weeks_end_date



def collect_sig_fields_for_all_tables(cursor, datamart_id, logesys_engine, source_engine, start_month, end_month):
    """
    Collect metadata for all tables in a datamart.

    Parameters:
    cursor - database cursor
    datamart_id - ID of the datamart
    engine - SQLAlchemy engine for database connection

    Returns:
    Dictionary containing all tables' metadata.  Structure:
    'significant_dimensions': {table_name: [dim1, dim2, ...], ...},
    'significant_measures': {table_name: [meas1, meas2, ...], ...},
    'date_columns': {table_name: date_column, ...},
    'max_year_dict': {table_name: max_year, ...},
    'max_month_dict': {table_name: max_month, ...},
    'max_date_dict': {table_name: max_date, ...},
    'ty_start_date_dict': {table_name: ty_start_date, ...},
    'ty_end_date_dict': {table_name: ty_end_date, ...},
    'ly_start_date_dict': {table_name: ly_start_date, ...},
    'ly_end_date_dict': {table_name: ly_end_date, ...},
    'last12months_start_date_dict': {table_name: last12months_start_date, ...},
    'last12months_end_date_dict': {table_name: last12months_end_date, ...},
    'last52weeks_start_date_dict': {table_name: last52weeks_start_date, ...},
    'last52weeks_end_date_dict': {table_name: last52weeks_end_date, ...},
    }
    """
    # Get all tables for this datamart
    tables_info, _ = get_datamart_source_credentials(datamart_id, logesys_engine)

    # Initialize result dictionaries
    all_dimensions = {}
    all_measures = {}
    date_columns = {}
    max_year_dict = {}
    max_month_dict = {}
    max_date_dict = {}
    ty_start_date_dict = {}
    ty_end_date_dict = {}
    ly_start_date_dict = {}
    ly_end_date_dict = {}
    last12months_start_date_dict = {}
    last12months_end_date_dict = {}
    last52weeks_start_date_dict = {}
    last52weeks_end_date_dict = {}

    # Process each table
    for table_name, table_data in tables_info.items():
        # Get the table_id
        table_id = table_data['table_id']

        # Call significant_fields for this table

        dimensions, measures, date_column, max_year, max_month, max_date, ty_start_date, ty_end_date, ly_start_date, ly_end_date, last12months_start_date, last12months_end_date, last52weeks_start_date, last52weeks_end_date = significant_fields(
            cursor,
            source_engine,
            datamart_id,
            table_id,
            start_month,
            end_month
        )
        
        # Update the dictionaries
        if dimensions:
            all_dimensions[table_name] = dimensions
        if measures:
            all_measures[table_name] = measures
        if date_column:
            date_columns[table_name] = date_column
        if max_year:
            max_year_dict[table_name] = max_year
        if max_month:
            max_month_dict[table_name] = max_month
        if max_date:
            max_date_dict[table_name] = max_date
        if ty_start_date:
            ty_start_date_dict[table_name] = ty_start_date
        if ty_end_date:
            ty_end_date_dict[table_name] = ty_end_date
        if ly_start_date:
            ly_start_date_dict[table_name] = ly_start_date
        if ly_end_date:
            ly_end_date_dict[table_name] = ly_end_date
        if last12months_start_date:
            last12months_start_date_dict[table_name] = last12months_start_date
        if last12months_end_date:
            last12months_end_date_dict[table_name] = last12months_end_date
        if last52weeks_start_date:
            last52weeks_start_date_dict[table_name] = last52weeks_start_date
        if last52weeks_end_date:
            last52weeks_end_date_dict[table_name] = last52weeks_end_date
    
    return {
        'significant_dimensions': all_dimensions,
        'significant_measures': all_measures,
        'date_columns': date_columns,
        'max_year_dict': max_year_dict,
        'max_month_dict': max_month_dict,
        'max_date_dict': max_date_dict,
        'ty_start_date_dict': ty_start_date_dict,
        'ty_end_date_dict': ty_end_date_dict,
        'ly_start_date_dict': ly_start_date_dict,
        'ly_end_date_dict': ly_end_date_dict,
        'last12months_start_date_dict': last12months_start_date_dict,
        'last12months_end_date_dict': last12months_end_date_dict,
        'last52weeks_start_date_dict': last52weeks_start_date_dict,
        'last52weeks_end_date_dict': last52weeks_end_date_dict,
    }



def json_manipulation(inp_json):
    inp_dict = json.loads(inp_json)
    for key in inp_dict.keys():
        for inner_key in inp_dict[key].keys():
            if(inner_key == 'Formula'):
                continue
            else:
                py_func = inp_dict[key][inner_key]['Formula']
                inp_dict[key].update({inner_key: py_func})
    return json.dumps(inp_dict, indent = 2)


# def transform_derived_measures(inp_dict):
#     transformed_dict = {}
#     for key, value in inp_dict.items():
#         formula = value.get('Formula')
#         inner_key = next((k for k in value if k != 'Formula'), None)
#         inner_formula = value.get(inner_key, {}).get('Formula')
#         if formula and inner_key and inner_formula:
#             transformed_dict[key] = {
#                 'Formula': formula,
#                 inner_key: inner_formula
#             }
#     return transformed_dict

def transform_derived_measures(inp_dict):
    transformed_dict = {}
    for key, value in inp_dict.items():
        formula = value.get('Formula')
        transformed_dict[key] = {'Formula': formula}
        for inner_key, inner_value in value.items():
            if inner_key != 'Formula' and isinstance(inner_value, dict) and 'Formula' in inner_value:
                transformed_dict[key][inner_key] = inner_value['Formula']
    return transformed_dict


def create_table_name_mapping(tables_info):
    """
    Create a simple mapping dictionary where each table name maps to itself.
    
    Parameters:
    tables_info - Dictionary with table information
    
    Returns:
    Dictionary with table_name: table_name for each table
    """
    df_sql_table_names = {}
    
    for table_name in tables_info.keys():
        df_sql_table_names[table_name] = table_name
    
    return df_sql_table_names
