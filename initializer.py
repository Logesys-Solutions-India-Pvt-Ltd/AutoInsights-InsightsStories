from sqlalchemy import create_engine, text
from datetime import datetime
from initializer_functions import *
from multiple_tables_csv_excel import *
from Insights import *
from Stories.stories_call import stories_call
import os
import pandas as pd
import numpy as np
import importlib
import json
import boto3


#def main_handler(event):

#### Received from API ####
datamart_id = "68F4413C-FD9A-11EF-BA6C-2CEA7F154E8D"
organization_id = "1F3B7012-EFEA-46EF-ABED-BD297BF3BB61"
engine_id = "BA2ACCBB-31B4-11EB-9A5D-A85E45BE6945"
organization = "Timesquare"
df_relationship_path = 'Relationship Table Dist.xlsx'
#### Received from API ####

# datamart_id = "5C8A4096-25B7-11F0-92B1-3CE9F73E436E"
# organization = "JMBaxi"
# df_relationship_path = ''

if df_relationship_path != '':
    df_relationship = read_data(df_relationship_path, type='xlsx')
else:
    df_relationship = pd.DataFrame()

start_month = 1
end_month = 12
#########selected_insights = f"SELECT selected_insights FROM table_name WHERE datamart_id = '{datamart_id}'"
selected_insights = ['Hi-Pots']

########## Establish Logesys Database Connection ##########
cnxn, cursor, logesys_engine = sql_connect()

########## Get client's credentials from the database ##########
tables_info, common_credentials = get_datamart_source_credentials(datamart_id, logesys_engine)

file_path = common_credentials['file_path']
source_server, source_database = file_path.split("//")
source_username = common_credentials['username']
source_password = common_credentials['password']
source_type = common_credentials['source_type']
source_engine = create_engine(f"mssql+pymssql://{source_username}:{source_password.replace('@', '%40')}@{source_server}/{source_database}")

########## Get the derived measures formula from S3 bucket ##########
############### Get DAX formula input from table ################
s3_client = boto3.client('s3', region_name='us-east-1')
s3_bucket_derived_meas_formula = "auto-insights-ask-db-cred-formula"
s3_key_name_derived_meas_formula = f"{datamart_id.lower()}_formula.json"
derived_measures_dict_expanded = s3_client.get_object(Bucket=s3_bucket_derived_meas_formula, Key=s3_key_name_derived_meas_formula)
derived_measures_dict_expanded = json.loads(derived_measures_dict_expanded['Body'].read().decode('utf-8'))
########## Transform expanded derived measures dictionary to a compressed format ##########
derived_measures_dict = transform_derived_measures(derived_measures_dict_expanded)
df_sql_table_names = create_table_name_mapping(tables_info)

df_sql_meas_functions = {
    'sum()': 'SUM',
    'mean()': 'AVG'
}

if source_type == 'table':
    df_list, df_list_ly, df_list_ty = [], [], []
    df_list_last12months, df_list_last52weeks = [], []


# ########## Creation of Significant Fields and all date related fields ##########
sig_fields = collect_sig_fields_for_all_tables(cursor, datamart_id, logesys_engine, source_engine, start_month, end_month)
Significant_dimensions = sig_fields['significant_dimensions']
Significant_measures = sig_fields['significant_measures']
date_columns = sig_fields['date_columns']
max_year_dict = sig_fields['max_year_dict']
max_month_dict = sig_fields['max_month_dict']
max_date_dict = sig_fields['max_date_dict']

max_year = max(max_year_dict.values())
max_month = max(max_month_dict.values())
max_date = max(max_date_dict.values())

print(f'Significant_dimensions:{Significant_dimensions}')


dates_filter_dict = {
    'ty_start_date_dict': sig_fields['ty_start_date_dict'],
    'ty_end_date_dict': sig_fields['ty_end_date_dict'],
    'ly_start_date_dict': sig_fields['ly_start_date_dict'],
    'ly_end_date_dict': sig_fields['ly_end_date_dict'],
    'last12months_start_date_dict': sig_fields['last12months_start_date_dict'],
    'last12months_end_date_dict': sig_fields['last12months_end_date_dict'],
    'last52weeks_start_date_dict': sig_fields['last52weeks_start_date_dict'],
    'last52weeks_end_date_dict': sig_fields['last52weeks_end_date_dict']
}


# # ########## Dates calculations for Outliers ##########
outliers_dates = calculate_periodic_dates_for_outliers(source_type, source_engine, date_columns, df_sql_table_names, df_list, df_list_ty, df_list_ly)


# ######### Significance Score ##########
significance_score = significance_engine_sql(source_engine, df_sql_table_names, df_sql_meas_functions, Significant_dimensions, Significant_measures, df_relationship)

# # ########################################## Automate this ##########################################
rename_dim_meas = {
    'CRANE_MINS': 'Crane Mins',
    'BERTH_MINS': 'Berth Mins',
    'TEUS': 'TEUs',
    'BERTH_PROD': 'Berth Prod',
    'VESSEL_NAME': 'Vessel Name',
    'SERVICE_ID': 'Service ID'
}
# # ########################################## Automate this ##########################################

### Stories Call ###
stories_call(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, Significant_dimensions, df_list_ly, df_list_ty, df_relationship, rename_dim_meas, significance_score, cnxn, cursor)

# # # # ################################################## Insights Call ##########################################

insightcode_sql = "SELECT InsightCode, MAX(VersionNumber) AS VersionNumber, MAX(Importance) AS Importance FROM tt_insights WHERE datamartid = '" + str(datamart_id) + "' GROUP BY InsightCode"
df_version_number = pd.read_sql(insightcode_sql, cnxn)

# # Map display names to module names
insight_module_map = {
    'Hi-Pots': 'hi_pots',
    'Movements': 'movements',
    'Rank Analysis': 'rank_analysis',
    'Delta Analysis': 'delta_analysis', 
    'New Entrants': 'new_entrants',
    'Trends': 'trends',
    'Monthly Anomalies': 'monthly_anomalies',
    'Weekly Anomalies': 'weekly_anomalies',
    'Outliers': 'outliers'
}


# Import the selected modules from the Insights folder
insight_functions = {}
for insight in selected_insights:
    if insight in insight_module_map:
        module_name = insight_module_map[insight]
        print(f"module_name:{module_name}")
    try:
        module = importlib.import_module(f"{module_name}")
        # Assuming the function name in each module is the same as the module name
        insight_functions[insight] = getattr(module, module_name)
    except ModuleNotFoundError:
        print(f"Warning: Module '{module_name}.py' not found.")
    except AttributeError:
        print(f"Warning: Function '{module_name}' not found in module '{module_name}.py'.")



# # # for dim_table, dim_list in Significant_dimensions.items():
# # #     for dim in dim_list:
# # #         for meas in list(derived_measures_dict.keys()):
# # #             print(f"dim_table:{dim_table}, dim:{dim}, meas:{meas}")
# # #             for insight_name, insight_function in insight_functions.items():
# # #                 if insight_name == 'Hi-Pots':
# # #                     # print(f'insight_function:{insight_function}')
# # #                     # insight_function(datamart_id, source_type, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, rename_dim_meas, dim, meas, df_list_ly, df_list_ty, dim_table, cnxn, cursor)
# # #                     insight_function()


dim_table = 'Location_Dist'
dim = 'Store Name'
meas = 'Markdown %'


for insight_name, insight_function in insight_functions.items():
    if insight_name == 'Hi-Pots':
        insight_function(datamart_id, source_type, source_engine, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, rename_dim_meas, dim, meas, date_columns, dates_filter_dict, df_list_ty, dim_table, df_version_number, cnxn, cursor)
    elif insight_name == 'Movements':     
        insight_function(datamart_id, source_type, source_engine, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, rename_dim_meas, dim, meas, date_columns, dates_filter_dict, df_list_ly, df_list_ty, dim_table, df_version_number, significance_score, cnxn, cursor)
    elif insight_name == 'Rank Analysis':
        insight_function(datamart_id, source_type, source_engine, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, rename_dim_meas, df_list_ty, df_list_ly, dim_table, dim, meas, date_columns, dates_filter_dict, df_version_number, cnxn, cursor)
    elif insight_name == 'Delta Analysis':
        insight_function(datamart_id, source_type, source_engine, dim, meas, date_columns, dates_filter_dict, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, rename_dim_meas, df_list, df_list_ty, df_list_ly, dim_table, df_version_number, cnxn, cursor)
    elif insight_name == 'New Entrants':
        insight_function(datamart_id, source_type, source_engine, dim, meas, dim_table, date_columns, dates_filter_dict, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_list_last12months, df_relationship, rename_dim_meas, significance_score, max_month, max_date, df_version_number, cnxn, cursor)
    elif insight_name == 'Trends':
        insight_function(datamart_id, source_type, source_engine, date_columns, dates_filter_dict, Significant_dimensions, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_list_last12months, df_relationship, rename_dim_meas, significance_score, max_month, max_date, df_version_number, cnxn, cursor)
    elif insight_name == 'Monthly Anomalies':
        insight_function(datamart_id, source_type, source_engine, date_columns, dates_filter_dict, Significant_dimensions, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_list_last12months, df_relationship, rename_dim_meas, significance_score, max_month, max_date, df_version_number, cnxn, cursor)
    elif insight_name == 'Weekly Anomalies':
        insight_function(datamart_id, source_type, source_engine, date_columns, dates_filter_dict, Significant_dimensions, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_list_last52weeks, df_relationship, rename_dim_meas, significance_score, max_month, max_date, df_version_number, cnxn, cursor)
    elif insight_name == 'Outliers':
        insight_function(datamart_id, source_type, source_engine, date_columns, dates_filter_dict, Significant_dimensions, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, df_list_ly, df_list_ty, rename_dim_meas, significance_score, max_year, max_month, outliers_dates, df_version_number, cnxn, cursor)
