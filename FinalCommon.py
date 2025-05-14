# FinalCommon.py

# --- Imports ---
import pandas as pd
import json
import numpy as np
from datetime import datetime
import uuid
import os
import pyodbc
import datetime as dt
import math
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text
import warnings

from FinalParameters import *

warnings.filterwarnings("ignore")

# --- Functions ---
def read_data(conn_string, type='xlsx'):
    if type == 'xlsx':
        df = pd.read_excel(conn_string)
    elif type == 'csv':
        df = pd.read_csv(conn_string)
    return df

# def sql_connect():
#     cnxn = pyodbc.connect()
#     cursor = cnxn.cursor()
#     return cnxn, cursor

# Story Functions
def StoryData(st_data):
    text_count = 1
    data = '{'
    for d in st_data:
        data = data + '"text' + str(text_count) + '" : "' + d + '",'
        text_count += 1
    data = data[:-1] + '}'
    return data



def sql_connect():
    username = "lsdbadmin"
    password = "logesys@1"
    server = "logesyssolutions.database.windows.net"
    database = "Insights_DB_Dev"
    logesys_engine = create_engine(f"mssql+pymssql://{username}:{password.replace('@', '%40')}@{server}/{database}")

    cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                  "Server=logesyssolutions.database.windows.net;"
                  "Database=Insights_DB_Dev;"
                 "UID=lsdbadmin;"
                 "PWD=logesys@1;")
    cursor = cnxn.cursor()
    return cnxn, cursor, logesys_engine

def get_metadata_json(jsondata):
    jsondata = jsondata.replace("'", '"')
    metadata = json.loads(jsondata)
    df_metadata = pd.DataFrame([metadata['Meta Data'][0]])
    for i in range(1, len(metadata['Meta Data'])):
        df_metadata = df_metadata.append([metadata['Meta Data'][i]])
    try:
        df_metadata['New Name'].fillna(df_metadata['Field Name'], inplace=True)
    except:
        df_metadata['New Name'] = np.nan
        df_metadata['New Name'].fillna(df_metadata['Field Name'], inplace=True)
    return df_metadata

def update_metadata(datamart_id, table_id, df_metadata, cnxn, cursor):
    updated_date = datetime.now()
    query = "delete from m_datamart_metadata where TableId = '" + str(table_id) + "'"
    cursor.execute(query)
    cnxn.commit()
    id = 0
    for i in range(0, len(df_metadata['New Name'])):
        id += 1
        field_name = str(list(df_metadata['Field Name'])[i])
        data_type = str(list(df_metadata['Data Type'])[i])
        field_type = str(list(df_metadata['Field Type'])[i])
        display_new_name = str(list(df_metadata['New Name'])[i])
        significance = '' if str(list(df_metadata['Significance'])[i]) == 'nan' else str(list(df_metadata['Significance'])[i])
        measure_type = '' if ('Measure Type' not in df_metadata.columns) or (str(list(df_metadata['Measure Type'])[i]) == 'nan') else str(list(df_metadata['Measure Type'])[i])
        metadata_id = uuid.uuid1()
        cursor.execute('''INSERT INTO m_datamart_metadata ([MetaDataId]
        ,[DataMartId]
        ,[FieldId]
        ,[FieldName]
        ,[DisplayFieldName]
        ,[DataType]
        ,[FieldType]
        ,[MeasureType]
        ,[Significance]
        ,[UpdatedDatetime]
        ,[CreatedDate]
        ,[TableId]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''', (metadata_id, datamart_id, id, field_name, display_new_name, data_type, field_type, measure_type, significance, updated_date, updated_date, table_id))
        cnxn.commit()

def get_metadata_sql(datamart_id, table_id, cnxn, cursor):
    sql = "SELECT * FROM m_datamart_metadata where DataMartId = '" + str(datamart_id) + "' and TableId =  '" + str(table_id) + "'"
    df_metadata = pd.read_sql(sql, cnxn)
    return df_metadata

def rename_columns(df, df_metadata):
    display_field_name = list(df_metadata['DisplayFieldName'])
    field_name = list(df_metadata['FieldName'])
    column_names = {}
    for i in range(len(list(df_metadata['FieldName']))):
        column_names[field_name[i]] = display_field_name[i]
    for i in range(len(list(df_metadata['FieldName']))):
        df.rename(columns={df.columns[i]: column_names[df.columns[i]]}, inplace=True)
    return df

# def df_others_modified(split_values, df, df_aggr, dim, meas, func='sum', split=8, split_top=True, index=True, sort_by_measure=True, name='', ascending=False):
#     df_aggr = df_aggr.sort_values(meas, ascending=ascending)
#     if not index:
#         df_aggr = df_aggr.set_index(dim)
#     if df_aggr.index.nunique() > split + 1:
#         if split_top:
#             df_others = df_aggr[:split]
#         else:
#             df_others = df_aggr[df_aggr.index.isin(split_values)]
#             if sort_by_measure:
#                 df_others = df_others.sort_values(meas, ascending=ascending)
#             else:
#                 df_others = df_others.reindex(split_values)
#         if func == 'sum':
#             df_to_send = df[df[dim].isin(list(set(df_aggr.index) - set(df_others.index)))]
#             df_others.loc[str(df_aggr.index.nunique() - split) + ' Others' + name, meas] = derived_meas_formula(df_to_send, dim, meas, "target_df[", meas, is_df=False, is_average=False)
#         elif func == 'mean':
#             df_to_send = df[df[dim].isin(list(set(df_aggr.index) - set(df_others.index)))]
#             df_others.loc[str(df_aggr.index.nunique() - split) + ' Others' + name, meas] = derived_meas_formula(df_to_send, dim, meas, "target_df[", meas, is_df=False, is_average=True)
#         if not index:
#             df_others = df_others.reset_index()
#         return df_others
#     else:
#         return df_aggr

# def df_others(column_dict, df, dim, meas, func='sum', split=8, split_top=True, index=True, aggregate=True, sort_by_measure=True, name='', ascending=False):
#     if aggregate:
#         if func == 'sum':
#             df = df.groupby(dim)[meas].sum().sort_values(meas, ascending=False)
#         elif func == 'mean':
#             df = df.groupby(dim)[meas].mean().sort_values(meas, ascending=False)
#     df = df.sort_values(meas, ascending=ascending)
#     if not index:
#         df = df.set_index(dim)
#     if df.index.nunique() > split + 1:
#         if split_top:
#             df_others = df[:split]
#         else:
#             df_others = df[(df.index.isin(column_dict[dim][-split:].index))]
#             if sort_by_measure:
#                 df_others = df_others.sort_values(meas, ascending=ascending)
#             else:
#                 df_others = df_others.reindex(column_dict[dim][-split:][::-1].index)
#         if func == 'sum':
#             df_others.loc[str(df.shape[0] - split) + ' Others' + name, meas] = df[df.index.isin(list(set(df.index) - set(df_others.index)))][meas].sum()
#         elif func == 'mean':
#             df_others.loc[str(df.shape[0] - split) + ' Others' + name, meas] = df[df.index.isin(list(set(df.index) - set(df_others.index)))][meas].mean()
#         if not index:
#             df_others = df_others.reset_index()
#         return df_others
#     else:
#         return df

def chart_index_styling(df, index, column, average = 'empty', avg_color = '#FF0000', def_color = '#FEE2B5', highlight_color = '#FF9E00'):    
    df.loc[(df.index.isin(index)), column] = '{\"data\":' + df[(df.index.isin(index))][column].astype(str) + ', \"style\": "color:' +  highlight_color + '"}'
    df.loc[~(df.index.isin(index)), column] = '{\"data\":' + df[~(df.index.isin(index))][column].astype(str) + ', \"style\": "color:' +  def_color + '"}'
    if(average != 'empty'):
        df.loc[(df.index.isin(index)), average] = '{\"data\":' + df.loc[(df.index.isin(index)), average].astype(str) + ', \"style\": "color:' +  avg_color + '"}'
        df.loc[~(df.index.isin(index)), average]  = '{\"data\":' + df.loc[~(df.index.isin(index)), average].astype(str)  + ', \"style\": "color:' +  avg_color + '"}'
    return df

def human_format(num):
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        if magnitude == 1:
            num /= 1000.0
        else:
            num /= 100.0
    return '%.2f%s' % (num, ['', 'K', 'L', 'C', 'P', 'P', 'P', 'P'][magnitude])

def get_hierarchy(df, Significant_dimensions):
    hier_dict = {}
    hierarchy_dimension = {}
    iteration = 0
    for i in Significant_dimensions:
        for j in Significant_dimensions:
            if i != j:
                df2 = df.groupby(i)[j].nunique().to_frame()
                sample_sum = df2[df2[j] == 1][j].sum()
                if df2[df2[j] == 1][j].sum() == df2.shape[0]:
                    hier_dict[iteration] = {1: j, 2: i}
                    iteration += 1
                    if i == 'City':
                        j = 'State'
                    hierarchy_dimension[i] = j
                    string = j + ' is divided into ' + i
    return hier_dict, hierarchy_dimension

def create_datetime_columns(sourcetype, df, table_name, engine, Significant_dimensions, date_column):
    if sourcetype == 'csv' or sourcetype == 'xlsx':
        df[date_column] = pd.to_datetime(df[date_column])
        df = df.sort_values(by=date_column)
        df_Significant_dimensions = []
        for dim in Significant_dimensions:
            if dim in df.columns:
                df_Significant_dimensions.append(dim)
                df.loc[(df[dim].isna()) | (df[dim].str.replace(' ', '') == ''), dim] = 'Blank'
        df['Month'] = pd.to_datetime(df[date_column]).dt.month
        df['Year'] = pd.to_datetime(df[date_column]).dt.year
        df['Quarter'] = 'Q' + pd.to_datetime(df[date_column]).dt.quarter.astype(str)
        df['Week'] = 'W' + pd.to_datetime(df[date_column]).dt.week.astype(str)
        df['Month-Day'] = df[date_column].dt.month.astype(str) + '-' + df[date_column].dt.day.astype(str)
        df['Year-Month'] = df[date_column].dt.strftime('%Y-%b')
        df['Year-Week'] = df[date_column].dt.strftime('%Y-W%U')
        df[df_Significant_dimensions] = df[df_Significant_dimensions].astype(str)
        max_year = int(df['Year'].max())
        df_cy = df[df['Year'] == max_year]
        max_month = int(df_cy['Month'].max())
        max_date = df[date_column].max()
        return df, max_year, max_month, max_date
    elif sourcetype == 'table':
        query = f"""
        SELECT 
            YEAR(MAX(TRY_CONVERT(DATETIME, [{date_column}], 103))) AS MaxYear,
            MONTH(MAX(TRY_CONVERT(DATETIME, [{date_column}], 103))) AS MaxMonth,
            MAX(TRY_CONVERT(DATETIME, [{date_column}], 103)) AS MaxDate
        FROM 
            [dbo].[{table_name}]
        WHERE
            TRY_CONVERT(DATETIME, [{date_column}], 103) IS NOT NULL
        """
        with engine.connect() as conn:
            result = conn.execute(text(query))
            max_year, max_month, max_date = result.fetchone()
        return max_year, max_month, max_date


def get_ty_ly_start_end_period(start_month, end_month, max_year, max_month, max_date, date_column):
    max_year_str = str(max_year)
    ty_start_period_dt = datetime.strptime(f'01-0{start_month}-{max_year_str}', '%d-%m-%Y')
    ty_end_period_dt = max_date
    ly_start_period_dt = ty_start_period_dt - relativedelta(years=1)
    ly_end_period_dt = ty_end_period_dt - relativedelta(years=1)
    start_of_last_12_months_dt = max_date - relativedelta(months=12)
    end_of_last_12_months_dt = max_date
    start_of_last_52_weeks_dt = max_date - relativedelta(weeks=52)
    end_of_last_52_weeks_dt = max_date
    ty_start_period = ty_start_period_dt.strftime('%d-%m-%Y')
    ty_end_period = ty_end_period_dt.strftime('%d-%m-%Y')
    ly_start_period = ly_start_period_dt.strftime('%d-%m-%Y')
    ly_end_period = ly_end_period_dt.strftime('%d-%m-%Y')
    last_12_months_start = start_of_last_12_months_dt.strftime('%d-%m-%Y')
    last_12_months_end = end_of_last_12_months_dt.strftime('%d-%m-%Y')
    last_52_weeks_start = start_of_last_52_weeks_dt.strftime('%d-%m-%Y')
    last_52_weeks_end = end_of_last_52_weeks_dt.strftime('%d-%m-%Y')
    return ty_start_period, ty_end_period, ly_start_period, ly_end_period, last_12_months_start, last_12_months_end, last_52_weeks_start, last_52_weeks_end


# def get_ty_ly_start_end_period(start_month, end_month, max_year, max_month, max_date, date_column):
#     """
#     Calculates the start and end dates for the current year (TY), last year (LY),
#     last 12 months, and last 52 weeks, considering a flexible fiscal year.

#     Args:
#         start_month (int): The starting month of the fiscal year (e.g., 1 for January, 3 for March).
#         end_month (int): The ending month of the fiscal year (e.g., 12 for December, 4 for April).
#         max_year (int): The maximum year.
#         max_month (int): The maximum month.
#         max_date (datetime.date): The maximum date.
#         date_column (str): The name of the date column (not used in the calculations but kept for consistency).

#     Returns:
#     tuple: (ty_start_period, ty_end_period, ly_start_period, ly_end_period,
#             last_12_months_start, last_12_months_end, last_52_weeks_start, last_52_weeks_end)
#     """
#     # Calculate TY start date
#     ty_start_period_dt = datetime.date(max_year, start_month, 1)

#     # Calculate TY end date.  This is the same as max_date
#     ty_end_period_dt = max_date

#     # Calculate LY start and end dates
#     ly_start_period_dt = ty_start_period_dt - relativedelta(years=1)
#     ly_end_period_dt = ty_end_period_dt - relativedelta(years=1)

#     # Calculate last 12 months start and end dates.
#     last_12_months_start_dt = max_date - relativedelta(months=12)
#     last_12_months_end_dt = max_date # This should be max_date

#     # Calculate last 52 weeks start and end dates.
#     last_52_weeks_start_dt = max_date - relativedelta(weeks=52)
#     last_52_weeks_end_dt = max_date #This should be max_date

#     ty_start_period = ty_start_period_dt.strftime('%d-%m-%Y')
#     ty_end_period = ty_end_period_dt.strftime('%d-%m-%Y')
#     ly_start_period = ly_start_period_dt.strftime('%d-%m-%Y')
#     ly_end_period = ly_end_period_dt.strftime('%d-%m-%Y')
#     last_12_months_start = last_12_months_start_dt.strftime('%d-%m-%Y')
#     last_12_months_end = last_12_months_end_dt.strftime('%d-%m-%Y')
#     last_52_weeks_start = last_52_weeks_start_dt.strftime('%d-%m-%Y')
#     last_52_weeks_end = last_52_weeks_end_dt.strftime('%d-%m-%Y')

#     return (
#         ty_start_period,
#         ty_end_period,
#         ly_start_period,
#         ly_end_period,
#         last_12_months_start,
#         last_12_months_end,
#         last_52_weeks_start,
#         last_52_weeks_end,
#     )



def insert_summary(datamart_id, data, SummaryFunction, ChartType, section_id, dim, meas, cnxn, cursor):
    summary_id = uuid.uuid1()
    cursor.execute('''INSERT INTO tt_summary ([SummaryId]
      ,[DataMartId]
      ,[SummaryTitle]
      ,[SummaryData]
      ,[SummaryFunction]
      ,[ChatType]
      ,[SectionId]
      ,[Dimension]
      ,[Measure]) VALUES (?,?,?,?,?,?,?,?,?)''', (summary_id, datamart_id, '', data, SummaryFunction, ChartType, section_id, dim, meas))
    cnxn.commit()

def insert_insights(datamart_id, string, data, InsightFunction, ChartType, related_fields_list, importance, tags, Group, Type, cnxn, cursor, insight_code, version_num):
    insight_id = uuid.uuid1()
    cursor.execute('''INSERT INTO tt_insights ([InsightId]
      ,[DataMartId]
      ,[InsightTitle]
      ,[InsightData]
      ,[InsightFunction]
      ,[ChartType]
      ,[RelatedFields]
      ,[Importance]
      ,[Tags]
      ,[Group]
      ,[GroupId]
      ,[Status]
      ,[VersionNumber]
      ,[InsightCode]
      ,[Type]
      ,[DataMartRoleId]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (insight_id, datamart_id, str(string), data, InsightFunction, ChartType, str(related_fields_list), importance, tags, Group, 0, '', str(version_num), insight_code, Type, None))
    cnxn.commit()

def determine_output(input_value):
    if 0 <= input_value <= 100:
        return 5
    elif 100 < input_value <= 1000:
        return 50
    elif 1000 < input_value <= 10000:
        return 500
    elif 10000 < input_value <= 100000:
        return 5000
    elif 100000 < input_value <= 1000000:
        return 50000
    elif 1000000 < input_value <= 10000000:
        return 500000
    else:
        return 500000

def bucketing(df, numerical_features):
    for num in numerical_features:
        df[num + '_old'] = df[num]
        df[num] = df[num].astype(float)
        num_bins_sturges = int(np.log2(len(df[num])) + 1)
        if num_bins_sturges > 0:
            data_range = df[num].max() - df[num].min()
            if data_range == 0:
                print("Data range is too small to calculate bin width.")
            else:
                bin_width = (data_range / num_bins_sturges)
        else:
            print("Invalid value for num_bins_sturges.")
        bin_edges = [round((df[num].min() + i * bin_width), 2) for i in range(num_bins_sturges + 1)]
        labels = [str(bin_edges[i]) + ' - ' + str(bin_edges[i + 1]) for i in range(len(bin_edges) - 1)]
        df[num] = pd.cut(df[num], bins=bin_edges, include_lowest=True, labels=labels)
    return df

def azure_sql_database_connect(username, password, server, database):
    conn_str = (
        f"mssql+pyodbc://{username}:{password.replace('@', '%40')}@{server}/{database}?"
        "driver=ODBC+Driver+17+for+SQL+Server"
    )
    engine = create_engine(conn_str)
    return engine

def insert_into_m_datamart(organisation_id, engine_id, datamart_name, filetype, cnxn, cursor, datamart_id=None):
    today = datetime.now()
    if datamart_id is None:
        datamart_id = uuid.uuid1()
    else:
        cursor.execute("EXEC [dbo].[DeleteDataMart] '" + datamart_id + "'")
        cnxn.commit()
        print('Deleted')
    cursor.execute("INSERT INTO m_datamart ([DataMartId],[Type],[Name],[Status],[InsightEngineId],[OrganizationId],[CreatedDate],[UpdatedDate],[Description]) VALUES (?,?,?,?,?,?,?,?,?)", (datamart_id, filetype, datamart_name, 1, engine_id, organisation_id, today, today, 'JM Baxi'))
    cnxn.commit()
    return datamart_id

def insert_into_m_datamart_tables(datamart_id, df_list, df_name, table_list, table_name, sourcetype, filepath, username, password, cnxn, cursor):
    df_table = pd.read_sql(f""" SELECT * FROM m_datamart_tables WHERE datamartid = '{datamart_id}'""", cnxn)
    record_count = len(df_table)
    required_count = len(table_list.values()) if sourcetype == 'table' else len(df_list.keys())
    df_table_name_in_table = (table_name if sourcetype == 'table' else df_name)
    if record_count <= required_count:
        table_id = str(uuid.uuid1())
        today = datetime.now()
        cursor.execute('''
            INSERT INTO m_datamart_tables ([TableId], [DataMartId], [TableName], [Type], [SourceType], [FilePath],
                                           [Status], [CreatedDate], [UpdatedDate], [UserName], [Password])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (table_id, datamart_id, df_table_name_in_table, sourcetype, sourcetype, filepath, 'Active', today, today, username, password))
        cnxn.commit()
    else:
        table_id = df_table.iloc[0]['TableId']
    return table_id

def insert_into_m_datamart_metadata(datamart_id, table_id, Significant_dimensions, Significant_measure, cnxn, cursor):
    today = datetime.now()
    id = 0
    for i in Significant_dimensions:
        id += 1
        metadata_id = uuid.uuid1()
        cursor.execute('''INSERT INTO m_datamart_metadata ([MetaDataId]
        ,[DataMartId]
        ,[FieldId]
        ,[FieldName]
        ,[DisplayFieldName]
        ,[DataType]
        ,[FieldType]
        ,[MeasureType]
        ,[Significance]
        ,[UpdatedDatetime]
        ,[CreatedDate]
        ,[TableId]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''', (metadata_id, datamart_id, id, i, '', 'Text', 'Dimension', '', '', today, today, table_id))
        cnxn.commit()
    for i in Significant_measure:
        id += 1
        metadata_id = uuid.uuid1()
        cursor.execute('''INSERT INTO m_datamart_metadata ([MetaDataId],[DataMartId],[FieldId],[FieldName],[DisplayFieldName],[DataType],[FieldType],[MeasureType],[Significance],[UpdatedDatetime],[CreatedDate],[TableId]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''', (metadata_id, datamart_id, id, i, '', 'Decimal', 'Measure', '', '', today, today, table_id))
        cnxn.commit()

def get_metadata_sql(datamart_id, cnxn, cursor):
    sql = "SELECT * FROM m_datamart_metadata where DataMartId = '" + str(datamart_id) + "'"
    df_metadata = pd.read_sql(sql, cnxn)
    return df_metadata

def find_dim_meas(sourcetype, SQL_SERVER_RESERVED_KEYWORDS, engine):
    Significant_measure = []
    Significant_dimensions = []
    Dimensions_dtype = ['object', 'datetime64[ns]', 'category', 'datetime64']
    common_dimensions = ['CODE', 'ID', 'YEAR', 'MONTH', 'DATE', 'NO']
    query = text(f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{sourcetype}'
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        columns = result.fetchall()
        for col, dtype in columns:
            safe_col = f"[{col}]" if any(c in col for c in ' []%/') or col.upper() in SQL_SERVER_RESERVED_KEYWORDS else col
            if dtype not in Dimensions_dtype:
                unique_query = text(f"SELECT COUNT(DISTINCT {safe_col}) FROM {sourcetype}")
                unique_count = conn.execute(unique_query).fetchone()[0]
                if unique_count > 1:
                    Significant_measure.append(col)
            else:
                unique_query = text(f"SELECT COUNT(DISTINCT {safe_col}) FROM {sourcetype}")
                unique_count = conn.execute(unique_query).fetchone()[0]
                if unique_count > 1:
                    Significant_dimensions.append(col)
        for col in columns:
            col_name = col[0]
            k = 0
            for j in common_dimensions:
                if col_name.upper().find(j) == -1:
                    k += 1
            if k != len(common_dimensions) and col_name not in Significant_dimensions:
                Significant_dimensions.append(col_name)
            if col_name in Significant_measure and k != len(common_dimensions):
                Significant_measure.remove(col_name)
    return Significant_dimensions, Significant_measure

def significance_engine(df, Significant_dimensions, Significant_measure):
    column_dict = {}
    for dimension in Significant_dimensions:
        sig_list = []
        r = 0
        for i in Significant_measure:
            r += 1
            rank = 'rank' + str(r)
            df[i] = df[i].astype(float)
            pm = pd.DataFrame(df.groupby([dimension])[i].apply(lambda c: c.abs().sum()))
            pm[rank] = pm[i].rank()
            sig_list.append(pm)
        r += 1
        rank = 'rank' + str(r)
        value = df[dimension].value_counts().to_frame()
        value = value.sort_values(by=[dimension])
        value[rank] = value[dimension].rank()
        sig_list.append(value)
        temp = sig_list[0]
        for i in range(0, len(sig_list) - 1):
            temp = temp.join(sig_list[i + 1], how='outer')
        r = 0
        add = 0
        for i in range(0, len(sig_list)):
            r += 1
            rank = 'rank' + str(r)
            add = add + temp[rank]
        output = pd.DataFrame(add / len(sig_list))
        output.columns = ['rank']
        output = output.sort_values(by=['rank'])
        output['rank'] = output['rank'] * 10 / output['rank'].max()
        column_dict[dimension] = output
        column_dict[dimension]['rank'] = round(column_dict[dimension]['rank'], 2)
    return column_dict

def query_on_table(query, engine):
    query = text(query)
    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        column_names = result.keys()
        df_data = pd.DataFrame(rows, columns=column_names)
    return df_data

def significance_engine_sql(source_engine, df_sql_table_names, df_sql_meas_functions, Significant_dimensions, Significant_measures, df_relationship):
    column_dict = {}
    for meas_table, measures in Significant_measures.items():
        meas_sourcetable = df_sql_table_names[meas_table]
        if not measures:
            continue
        for dim_table, dimensions in Significant_dimensions.items():
            dim_sourcetable = df_sql_table_names[dim_table]
            table_key = dim_table
            if table_key not in column_dict:
                column_dict[table_key] = {}
            for dim_col in dimensions:
                sig_list = []
                r = 0
                for meas_col in measures:
                    r += 1
                    rank = f'rank{r}'
                    meas_operation = df_sql_meas_functions.get('sum', 'SUM')
                    if meas_sourcetable == dim_sourcetable:
                        query = f"""
                            SELECT [{dim_col}], {meas_operation}(ABS([{meas_col}])) AS [{meas_col}]
                            FROM [dbo].[{meas_sourcetable}]
                            GROUP BY [{dim_col}];
                        """
                    else:
                        relationship = df_relationship.loc[
                            ((df_relationship['table 1'] == meas_sourcetable) & (df_relationship['table 2'] == dim_sourcetable)) |
                            ((df_relationship['table 2'] == meas_sourcetable) & (df_relationship['table 1'] == dim_sourcetable))
                        ]
                        if relationship.empty:
                            continue
                        meas_key_col = relationship['column 1'].iloc[0]
                        dim_key_col = relationship['column 2'].iloc[0]
                        query = f"""
                            SELECT t2.[{dim_col}], {meas_operation}(ABS([{meas_col}])) AS [{meas_col}]
                            FROM [dbo].[{meas_sourcetable}] t1
                            RIGHT JOIN [dbo].[{dim_sourcetable}] t2
                            ON t1.[{meas_key_col}] = t2.[{dim_key_col}]
                            GROUP BY t2.[{dim_col}];
                        """
                    df_result = query_on_table(query, source_engine)
                    df_result = df_result.reset_index()
                    df_result[rank] = df_result[meas_col].rank()
                    sig_list.append(df_result)
                # if sig_list:
                #     df_combined = sig_list[0]
                #     for df_rank in sig_list[1:]:
                #         df_combined = df_combined.merge(df_rank, on=dim_col, how='outer')

                if sig_list:
                    df_combined = sig_list[0].copy()  # Use .copy() to avoid modifying the original in sig_list
                    for df_rank_original in sig_list[1:]:
                        df_rank = df_rank_original.copy() # Use .copy() to avoid modifying the original
                        # Drop the 'index' column from both DataFrames before merging
                        if 'index' in df_combined.columns:
                            df_combined = df_combined.drop(columns=['index'])
                        if 'index' in df_rank.columns:
                            df_rank = df_rank.drop(columns=['index'])
                        df_combined = df_combined.merge(df_rank, on=dim_col, how='outer')
                    rank_columns = [col for col in df_combined.columns if col.startswith('rank')]
                    df_combined['rank'] = df_combined[rank_columns].sum(axis=1) / len(rank_columns)
                    df_combined = df_combined[[dim_col, 'rank']].sort_values(by=['rank'])
                    df_combined['rank'] = df_combined['rank'] * 10 / df_combined['rank'].max()
                    df_combined['rank'] = df_combined['rank'].round(2)
                    df_combined.set_index(dim_col, inplace=True)
                    df_combined = df_combined.rename(index={'': 'Blank'})
                    df_combined.index.name = None
                    column_dict[table_key][dim_col] = df_combined
    return column_dict