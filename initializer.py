from sqlalchemy import create_engine, text
from datetime import datetime
from initializer_functions import *
from multiple_tables_csv_excel import *
from Stories.stories_call import stories_call
from Insights.insights_call import insights_call, insights_call_threaded
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
import logging


def insights_generator(event):
    constants.DATAMART_ID = event.get('datamart_id')
    constants.ENGINE_ID = event.get('engine_id')

    # constants.ENGINE_ID = "BA2ACCBB-31B4-11EB-9A5D-A85E45BE6945" #Test Integration 23-05-2025
    # constants.DATAMART_ID = "326C3C83-FB19-4E33-9AED-90690FAAF1CC" ## Vessel Visit testing datamart

    constants.CNXN, constants.CURSOR, constants.LOGESYS_ENGINE = sql_connect()
    count_tables_in_datamart_query = f"""
                                    SELECT COUNT(*) AS table_count
                                    FROM m_datamart_tables 
                                    WHERE DataMartId = '{constants.DATAMART_ID}'"""
    count_tables_in_datamart = pd.read_sql(count_tables_in_datamart_query, constants.CNXN)
    count_tables_in_datamart = count_tables_in_datamart['table_count'].iloc[0]

    if count_tables_in_datamart == 1:
        constants.DF_RELATIONSHIP = pd.DataFrame()
    else:
        df_relationship_query = f"""
                                    SELECT 
                                        FromTable, FromColumn, ToTable, ToColumn
                                    FROM relationship
                                    WHERE datamartid = '{constants.DATAMART_ID}'
                                    UNION ALL
                                    SELECT 
                                        ToTable as FromTable,ToColumn as FromColumn,
                                        FromTable as ToTable, FromColumn as ToColumn
                                    FROM relationship
                                    WHERE datamartid = '{constants.DATAMART_ID}'
                                    """

        constants.DF_RELATIONSHIP = pd.read_sql(df_relationship_query, constants.CNXN)
        
    # df_relationship_path = 'Relationship Table Dist.xlsx'

    # if df_relationship_path != '':
    #     df_relationship = read_data(df_relationship_path, type='xlsx')
    # else:
        # df_relationship = pd.DataFrame()

    start_month = 1
    end_month = 12

    try:
        # print('Process started.')
        constants.logger.info('Process started')
        
        ########## Get the selected insights #########
        selected_insights_query = f"""
                                SELECT selected_insights FROM insight_settings 
                                WHERE datamartid = '{constants.DATAMART_ID}'"""
        constants.CURSOR.execute(selected_insights_query)
        selected_insights_list = constants.CURSOR.fetchone()
        constants.SELECTED_INSIGHTS = json.loads(selected_insights_list[0])
        constants.logger.info(f'Insights selected for {constants.DATAMART_ID}: {constants.SELECTED_INSIGHTS}')  
        
        ########## Get client's credentials from the database ##########
        tables_info, common_credentials = get_datamart_source_credentials(constants.DATAMART_ID, constants.LOGESYS_ENGINE)

        file_path = common_credentials['file_path']
        source_server, source_database = file_path.split("//")
        source_username = common_credentials['username']
        source_password = common_credentials['password']
        constants.SOURCE_TYPE = common_credentials['source_type']
        constants.SOURCE_ENGINE = create_engine(f"mssql+pymssql://{source_username}:{source_password.replace('@', '%40')}@{source_server}/{source_database}")

        ########## Get the derived measures formula from derived_metrics table ##########
        constants.DERIVED_MEASURES_DICT_EXPANDED = combine_formula_json(constants.DATAMART_ID, constants.LOGESYS_ENGINE)
        constants.logger.info(f'Formula json:{constants.DERIVED_MEASURES_DICT_EXPANDED}')

        # Convert the dictionary to a JSON string
        DERIVED_MEASURES_DICT_EXPANDED_JSON_CONTENT = json.dumps(constants.DERIVED_MEASURES_DICT_EXPANDED, indent=4)

        # Upload the JSON string to S3
        constants.S3_CLIENT = boto3.client('s3', region_name='us-east-1')
        s3_bucket_derived_meas_formula = constants.S3_BUCKET_DERIVED_MEAS_FORMULA
        s3_key_name_derived_meas_formula = f"{constants.DATAMART_ID.lower()}_formula.json"
        constants.S3_CLIENT.put_object(
            Bucket=s3_bucket_derived_meas_formula,
            Key=s3_key_name_derived_meas_formula,
            Body=DERIVED_MEASURES_DICT_EXPANDED_JSON_CONTENT,
            ContentType='application/json' 
        )

        # ########## Transform expanded derived measures dictionary to a compressed format ##########
        constants.DERIVED_MEASURES_DICT_EXPANDED = json.loads(constants.DERIVED_MEASURES_DICT_EXPANDED)
        constants.DERIVED_MEASURES_DICT = transform_derived_measures(constants.DERIVED_MEASURES_DICT_EXPANDED)
        
        constants.logger.info(f'Formulae received and processed.')

        constants.DF_SQL_TABLE_NAMES = create_table_name_mapping(tables_info)

        constants.DF_SQL_MEAS_FUNCTIONS = {
            'sum()': 'SUM',
            'mean()': 'AVG'
        }

        if constants.SOURCE_TYPE == 'table':
            constants.DF_LIST, constants.DF_LIST_LY, constants.DF_LIST_TY = [], [], []
            constants.DF_LIST_LAST12MONTHS, constants.DF_LIST_LAST52WEEKS = [], []


        # ########## Creation of Significant Fields and all date related fields ##########
        sig_fields = collect_sig_fields_for_all_tables(constants.CURSOR, constants.DATAMART_ID, constants.LOGESYS_ENGINE, constants.SOURCE_ENGINE, start_month, end_month)
        
        constants.SIGNIFICANT_DIMENSIONS = sig_fields['significant_dimensions']
        constants.SIGNIFICANT_MEASURES = sig_fields['significant_measures']
        constants.DATE_COLUMNS = sig_fields['date_columns']
        max_year_dict = sig_fields['max_year_dict']
        max_month_dict = sig_fields['max_month_dict']
        max_date_dict = sig_fields['max_date_dict']

        constants.MAX_YEAR = max(max_year_dict.values())
        constants.MAX_MONTH = max(max_month_dict.values())
        constants.MAX_DATE = max(max_date_dict.values())

        constants.DATES_FILTER_DICT = {
            'ty_start_date_dict': sig_fields['ty_start_date_dict'],
            'ty_end_date_dict': sig_fields['ty_end_date_dict'],
            'ly_start_date_dict': sig_fields['ly_start_date_dict'],
            'ly_end_date_dict': sig_fields['ly_end_date_dict'],
            'last12months_start_date_dict': sig_fields['last12months_start_date_dict'],
            'last12months_end_date_dict': sig_fields['last12months_end_date_dict'],
            'last52weeks_start_date_dict': sig_fields['last52weeks_start_date_dict'],
            'last52weeks_end_date_dict': sig_fields['last52weeks_end_date_dict']
        }

        constants.logger.info(f'Significant dimensions:{constants.SIGNIFICANT_DIMENSIONS}')
        constants.logger.info(f'Significant measures:{constants.SIGNIFICANT_MEASURES}')
        constants.logger.info(f'Date columns:{constants.DATE_COLUMNS}')
        
        # # ########## Dates calculations for Outliers ##########
        constants.OUTLIERS_DATES = calculate_periodic_dates_for_outliers(constants.SOURCE_TYPE, constants.SOURCE_ENGINE, 
                                                                constants.DATE_COLUMNS, constants.DF_SQL_TABLE_NAMES, 
                                                                constants.DF_LIST, 
                                                                constants.DF_LIST_TY, constants.DF_LIST_LY)

        # ######### Significance Score ##########
        constants.logger.info('Generating significance score for dimensions and metrics.')
        constants.SIGNIFICANCE_SCORE = significance_engine_sql(constants.SOURCE_ENGINE, constants.DF_SQL_TABLE_NAMES, 
                                                        constants.DF_SQL_MEAS_FUNCTIONS, constants.SIGNIFICANT_DIMENSIONS, 
                                                        constants.SIGNIFICANT_MEASURES, constants.DF_RELATIONSHIP)
        constants.logger.info('Significance score assigned to dimensions and metrics.')

        ########### Getting Display Names ##########
        constants.RENAME_DIM_MEAS = rename_fields(constants.DATAMART_ID, constants.RENAME_DIM_MEAS, 
                                        constants.CNXN, constants.CURSOR)

        # # ### Timesquare ###
        # # constants.DIM_ALLOWED_FOR_DERIVED_METRICS = {
        # #     'Markdown %': [dim for dims in constants.SIGNIFICANT_DIMENSIONS.values() for dim in dims],
        # #     'ASP': [dim for dims in constants.SIGNIFICANT_DIMENSIONS.values() for dim in dims],
        # #     'Stock Cover': [dim for dims in constants.SIGNIFICANT_DIMENSIONS.values() for dim in dims],
        # #     'ATV': [dim for dim in constants.SIGNIFICANT_DIMENSIONS['Location_Dist']
        # #             if dim in ['Store Name', 'Region', 'Business', 'Mall Name', 'Territory']],
        # #     'UPT': [dim for dim in constants.SIGNIFICANT_DIMENSIONS['Location_Dist']
        # #             if dim in ['Store Name', 'Region', 'Business', 'Mall Name', 'Territory']],
        # # }
        # # ### Timesquare ###


        for meas in list(constants.DERIVED_MEASURES_DICT.keys()):
            meas_id_query = f"""SELECT MetricID FROM derived_metrics
                            WHERE DatamartId = '{constants.DATAMART_ID}' 
                            AND MetricName = '{meas}'
                            AND Indicator = '1'"""
            constants.CURSOR.execute(meas_id_query)
            meas_id_tuple = constants.CURSOR.fetchone()
            meas_id = meas_id_tuple[0]

            included_dim_query = f"""
                                SELECT Included_Dimensions FROM metric_settings
                                WHERE DatamartId = '{constants.DATAMART_ID}'
                                AND MetricID = '{meas_id.upper()}'"""
            constants.CURSOR.execute(included_dim_query)
            included_dim_tuple = constants.CURSOR.fetchone()
            included_dim_str = included_dim_tuple[0]
            included_dim_list = json.loads(included_dim_str)

            constants.DIM_ALLOWED_FOR_DERIVED_METRICS[meas] = included_dim_list

            included_insights_query = f"""
                                SELECT Included_Insights FROM metric_settings
                                WHERE DatamartId = '{constants.DATAMART_ID}'
                                AND MetricID = '{meas_id.upper()}'"""
            constants.CURSOR.execute(included_insights_query)
            included_insights_tuple = constants.CURSOR.fetchone()
            included_insights_str = included_insights_tuple[0]
            included_insights_list = json.loads(included_insights_str)

            constants.INSIGHTS_ALLOWED_FOR_DERIVED_METRICS[meas] = included_insights_list

        constants.logger.info(f'DIM_ALLOWED_FOR_DERIVED_METRICS:{constants.DIM_ALLOWED_FOR_DERIVED_METRICS}')
        constants.logger.info(f'INSIGHTS_ALLOWED_FOR_DERIVED_METRICS:{constants.INSIGHTS_ALLOWED_FOR_DERIVED_METRICS}')

        ########### Function Call ###########
        insightcode_sql = "SELECT InsightCode, MAX(VersionNumber) AS VersionNumber, MAX(Importance) AS Importance FROM tt_insights WHERE datamartid = '" + str(constants.DATAMART_ID) + "' GROUP BY InsightCode"
        constants.DF_VERSION_NUMBER = pd.read_sql(insightcode_sql, constants.CNXN)

        constants.INSIGHTS_TO_SKIP = ['Trends', 'Outliers', 'Monthly Anomalies', 'Weekly Anomalies']

        stories_call()
        constants.logger.info('Stories generated.')    

        insights_call()
        constants.logger.info('Insights generated')

        playlist_call()
        constants.logger.info('Playlist generated')

        data_overview_call()
        constants.logger.info('Data Overview generated')

        return {
            "status": "success",
            "message": "Insights and stories processed",
            "datamart_id": constants.DATAMART_ID
        }

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        file_name = exc_tb.tb_frame.f_code.co_filename
        line_number = exc_tb.tb_lineno

        while exc_tb.tb_next:
            exc_tb = exc_tb.tb_next

        original_file_name = exc_tb.tb_frame.f_code.co_filename
        original_line_number = exc_tb.tb_lineno

        error_message = f"Error in insights_generator: {e} (originally from file '{original_file_name}' at line {original_line_number})"
        constants.logger.error(error_message)
        return {"status": "error", "message": error_message}

    finally:
        if constants.CNXN:
            constants.CNXN.close()