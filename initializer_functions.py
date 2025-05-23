from sqlalchemy import create_engine, text
from dateutil.relativedelta import relativedelta
from FinalCommon import *
import json
import re
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


def json_creation(datamartId, cursor):
    metrics = cursor.execute(f"SELECT MetricName FROM derived_metrics WHERE DataMartID = '{datamartId}'").fetchall()
    formulae = cursor.execute(f"SELECT Formula FROM derived_metrics WHERE DataMartID = '{datamartId}'").fetchall()
    func_map = {
        "SUM": "sum()",
        "AVERAGE": "mean()",
        "MAX": "max()",
        "MIN": "min()",
        "COUNT": "count()"
    }

    # Main function to convert a formula to JSON
    def basic_json_conversion(metric, dax_str):
        dax_str = dax_str.strip()
        if dax_str.startswith("="):
            dax_str = dax_str[1:]

        result = {metric: {"Formula": dax_str}}

        # Use regex to extract function calls like SUM(t_sales.markdown)
        pattern = r"(SUM|AVERAGE|COUNT)\(\s*([\w\.\s]+\(?[\w\s]*\)?)\s*\)\.?([\w]*)"
        matches = re.findall(pattern, dax_str)

        for func_name, table_column, filterMatch in matches:
            if func_name not in func_map:
                continue
            filterName = ""
            table_name, column_name = table_column.split('.')
            operand = f"{func_name}({table_column})"
            py_formula = f"{table_name}['{column_name}'].{func_map[func_name]}"

            if filterMatch == "filter":
                filterName = "Sales_Auto_Insights_Dist['Posting Date'] >= (Sales_Auto_Insights_Dist['Posting Date'].max() - pd.Timedelta(days=30))"
                py_formula = f"{table_name}[{filterName}][{column_name}].{func_map[func_name]}"
            result[metric][operand] = {
                "Formula": py_formula,
                "Table": table_name,
                "Column": column_name,
                "Filter": filterName,
                "Function": func_map[func_name]
            }

        return json.dumps(result, indent=2)
    result = {}
    for i in range(0,len(metrics)):
        result.update(json.loads(basic_json_conversion(metrics[i].MetricName, formulae[i].Formula)))
    return json.dumps(result, indent = 2)


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
    dim_meas_date_query = f"SELECT * FROM m_datamart_metadata WHERE TableId = '{table_id}' AND DataMartId = '{datamart_id}' AND Significance = 'Primary'"
    # cursor.execute(dim_meas_date_query)
    df_records = pd.read_sql_query(dim_meas_date_query, cursor.connection)
    
    # Initialize lists for this table
    dimensions = []
    measures = []
    dates = []

    #Categorize fields based on DataType
    for index, row in df_records.iterrows():
        data_type = row['DataType']
        field_name = row['FieldName']
        if data_type in ('Text', 'VARCHAR'):
            dimensions.append(field_name)
        elif data_type == 'Decimal':
            measures.append(field_name)
        elif data_type == 'Date':
            dates.append(field_name)

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
