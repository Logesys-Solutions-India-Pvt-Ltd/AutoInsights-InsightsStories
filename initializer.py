from sqlalchemy import create_engine, text
from datetime import datetime
from initializer_functions import *
from multiple_tables_csv_excel import *
from Stories.stories_call import stories_call
from Insights.insights_call import insights_call
from Playlist.playlist_call import playlist_call
from DataOverview.data_overview_call import data_overview_call
import os
import sys
import pandas as pd
import numpy as np
import importlib
import constants
import json
import boto3


def insights_generator(event):
    datamart_id = event.get('datamart_id')
    engine_id = event.get('engine_id')
    # datamart_id = "68F4413C-FD9A-11EF-BA6C-2CEA7F154E8D" ## Timesquare
    # datamart_id = "6AA6BCAA-258A-11F0-A1AD-2CEA7F154E8D" ## JMBaxi- old

    # engine_id = 'BA2ACCBB-31B4-11EB-9A5D-A85E45BE6945'
    # datamart_id= "7F2C4256-3449-447A-B1CC-FAE49431BF7C" ## Vessel Visit testing datamart

    cnxn, cursor, logesys_engine = sql_connect()
    count_tables_in_datamart_query = f"""
                                    SELECT COUNT(*) AS table_count
                                    FROM m_datamart_tables 
                                    WHERE DataMartId = '{datamart_id}'"""
    count_tables_in_datamart = pd.read_sql(count_tables_in_datamart_query, cnxn)
    count_tables_in_datamart = count_tables_in_datamart['table_count'].iloc[0]

    if count_tables_in_datamart == 1:
        df_relationship = pd.DataFrame()
    else:
        df_relationship_query = f"""
                                SELECT * FROM relationship_table 
                                WHERE datamartid = '{datamart_id}'"""
        
    # df_relationship_path = 'Relationship Table Dist.xlsx'

    # if df_relationship_path != '':
    #     df_relationship = read_data(df_relationship_path, type='xlsx')
    # else:
        # df_relationship = pd.DataFrame()

    start_month = 1
    end_month = 12

    try:
        print('Process started.')
        
        ########## Get the selected insights #########
        selected_insights_query = f"""
                                SELECT selected_insights FROM insight_settings WHERE datamartid = '{datamart_id}'"""
        cursor.execute(selected_insights_query)
        selected_insights_list = cursor.fetchone()
        selected_insights = json.loads(selected_insights_list[0])
        print(f'Insights selected for {datamart_id}: {selected_insights}')  
        
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
        print(f'Formulae received and processed.')
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
        print('Significance score assigned to dimensions and metrics.')

        ########### Getting Display Names ##########
        rename_dim_meas = {}
        rename_dim_meas = rename_fields(datamart_id, rename_dim_meas, cnxn, cursor)

        # ### Timesquare ###
        # dim_allowed_for_derived_metrics = {
        #     'Markdown %': [dim for dims in Significant_dimensions.values() for dim in dims],
        #     'ASP': [dim for dims in Significant_dimensions.values() for dim in dims],
        #     'Stock Cover': [dim for dims in Significant_dimensions.values() for dim in dims],
        #     'ATV': [dim for dim in Significant_dimensions['Location_Dist']
        #             if dim in ['Store Name', 'Region', 'Business', 'Mall Name', 'Territory']],
        #     'UPT': [dim for dim in Significant_dimensions['Location_Dist']
        #             if dim in ['Store Name', 'Region', 'Business', 'Mall Name', 'Territory']],
        # }
        # ### Timesquare ###

        ### JM Baxi ###
        dim_allowed_for_derived_metrics = {
            'MOVES': [dim for dims in Significant_dimensions.values() for dim in dims],
            'NCR': [dim for dims in Significant_dimensions.values() for dim in dims],
            'Crane Productivity': [dim for dims in Significant_dimensions.values() for dim in dims]
        }
        ### JM Baxi ###

        ########### Function Call ###########
        insightcode_sql = "SELECT InsightCode, MAX(VersionNumber) AS VersionNumber, MAX(Importance) AS Importance FROM tt_insights WHERE datamartid = '" + str(datamart_id) + "' GROUP BY InsightCode"
        df_version_number = pd.read_sql(insightcode_sql, cnxn)

        insights_to_skip = ['Trends', 'Outliers', 'Monthly Anomalies', 'Weekly Anomalies']

        stories_call(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, Significant_dimensions, df_list_ly, df_list_ty, df_relationship, rename_dim_meas, significance_score, cnxn, cursor)
        print('Stories generated.')    

        insights_call(datamart_id, source_type, source_engine, selected_insights, insights_to_skip, dim_allowed_for_derived_metrics, Significant_dimensions, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, rename_dim_meas, date_columns, dates_filter_dict, outliers_dates, df_list, df_list_ly, df_list_ty, df_list_last12months, df_list_last52weeks, max_month, max_year, max_date, significance_score, df_version_number, cnxn, cursor)
        print('Insights generated')

        playlist_call(datamart_id, engine_id, source_engine, Significant_dimensions, df_sql_table_names, cnxn, cursor)
        print('Playlist generated')

        data_overview_call(source_type, source_engine, dim_allowed_for_derived_metrics, datamart_id, date_columns, dates_filter_dict, outliers_dates, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, Significant_dimensions, df_list, df_list_ly, df_list_ty, df_relationship, rename_dim_meas, significance_score, max_year, max_month, cnxn, cursor)
        print('Data Overview generated')

        return {
            "status": "success",
            "message": "Insights and stories processed",
            "datamart_id": datamart_id
        }

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        file_name = exc_tb.tb_frame.f_code.co_filename
        line_number = exc_tb.tb_lineno

        # To get the origin of the exception, we need to go to the last frame in the traceback
        while exc_tb.tb_next:
            exc_tb = exc_tb.tb_next

        original_file_name = exc_tb.tb_frame.f_code.co_filename
        original_line_number = exc_tb.tb_lineno

        error_message = f"Error in insights_generator: {e} (originally from file '{original_file_name}' at line {original_line_number})"
        print(error_message)
        return {"status": "error", "message": error_message}

    finally:
        if cnxn:
            cnxn.close()