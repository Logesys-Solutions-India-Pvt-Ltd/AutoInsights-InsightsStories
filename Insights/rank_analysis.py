from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd
import numpy as np
import constants


def rank_analysis(dim_table, dim, meas):
    print('--RANK ANALYSIS--')
    
    datamart_id = constants.DATAMART_ID
    source_type = constants.SOURCE_TYPE
    source_engine = constants.SOURCE_ENGINE
    date_columns = constants.DATE_COLUMNS
    dates_filter_dict = constants.DATES_FILTER_DICT
    rename_dim_meas = constants.RENAME_DIM_MEAS
    derived_measures_dict = constants.DERIVED_MEASURES_DICT
    derived_measures_dict_expanded = constants.DERIVED_MEASURES_DICT_EXPANDED
    df_list_ly = constants.DF_LIST_LY
    df_list_ty = constants.DF_LIST_TY
    df_relationship = constants.DF_RELATIONSHIP
    df_sql_table_names = constants.DF_SQL_TABLE_NAMES
    df_sql_meas_functions = constants.DF_SQL_MEAS_FUNCTIONS
    significance_score = constants.SIGNIFICANCE_SCORE
    df_version_number = constants.DF_VERSION_NUMBER
    cnxn = constants.CNXN
    cursor = constants.CURSOR
    logesys_engine = constants.LOGESYS_ENGINE

    is_ratio = False
    all_df_non_empty = True
    meas_table_list = []
    meas_table = derived_measures_dict_expanded[meas]
    
    for key, value in derived_measures_dict_expanded[meas].items():
        if key == 'Formula':
            continue
        meas_table = value['Table']
#         meas_table = meas_table.split('[')[1].split(']')[0].strip("'")
        meas_table_list.append(meas_table)
    meas_table_list = list(set(meas_table_list))
    
#     for df_meas_name in meas_table_list:
#         if df_list_ly[df_meas_name].shape[0]>0:
#             continue
#         else:
#             all_df_non_empty = False
#             break
            
    if all_df_non_empty:
        if source_type == 'xlsx':
            this_year_setting, last_year_setting = df_list_ty, df_list_ly
        elif source_type == 'table':
            this_year_setting, last_year_setting = 'ThisYear', 'LastYear'

        df_ThisYear = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio, is_total=False, is_others=False, outliers_val=None)   
        df_ThisYear = df_ThisYear.sort_values(by=meas, ascending=False)
        df_ThisYear.replace([np.inf, -np.inf], 0, inplace=True)
        df_ThisYear.fillna(0, inplace = True)
        df_ThisYear['This Year'] = df_ThisYear[meas].rank(ascending=False).astype(int)
        df_ThisYear.drop(meas, axis=1, inplace=True)

        df_LastYear = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, last_year_setting, is_ratio, is_total=False, is_others=False, outliers_val=None)   
        df_LastYear = df_LastYear.sort_values(by=meas, ascending=False)
        df_LastYear.replace([np.inf, -np.inf], 0, inplace=True)
        df_LastYear.fillna(0, inplace = True)
        df_LastYear['Last Year'] = df_LastYear[meas].rank(ascending=False).astype(int) 
        df_LastYear.drop(meas, axis=1, inplace=True)


        df_ThisYear_top10 = df_ThisYear.head(10)  # Get top 10 values
        df_LastYear_top10 = df_LastYear.head(10)  # Get top 10 values
        unique_values_top_10 = list(set(list(df_ThisYear_top10.index) + list(df_LastYear_top10.index)))

        df_filtered_ty = df_ThisYear[df_ThisYear.index.isin(unique_values_top_10)]
        df_filtered_ly = df_LastYear[df_LastYear.index.isin(unique_values_top_10)]

        df_data = pd.merge(df_filtered_ly, df_filtered_ty, left_index=True, right_index=True, how='outer')
        df_data = df_data.fillna(np.nan).astype({col: 'Int64' for col in df_data.columns})
        df_data['Diff'] = abs(df_data['Last Year'] - df_data['This Year'])
        df_temp_data = df_data[abs(df_data['Diff']) >= df_data.shape[0]/3]
        df_filter_data = df_data.nlargest(3, 'Diff').fillna(0)
        string_parts = []
        related_fields_parts = []
        string = ''
        
        if not df_LastYear.empty:
            for i in range(df_filter_data.shape[0]):
                df_last_year = df_filter_data.iloc[i]['Last Year']
#                     if df_last_year == np.nan:
#                         df_last_year = 0
                df_this_year = df_filter_data.iloc[i]['This Year']
                df_index_value = df_filter_data.index[i]

                if (df_last_year < df_this_year):
                    action = 'lowered'
                    action_description = 'Decrease'
                elif (df_last_year > df_this_year):
                    action = 'raised'
                    action_description = 'Increase'
                else:
                    continue  # Skip if there's no change in rank

                string_parts.append(
                    f'{b_tag_open}{dim}: {df_index_value}{b_tag_close} {action} from rank {b_tag_open}{df_last_year} to {df_this_year}{b_tag_close} compared to last year based on YTD {meas} contribution.|'
                )

                related_fields_parts.append(
                    f'{dim} : {df_index_value} | measure : {meas} | function : Story Rank CY vs LY {action_description}#|#'
                )

            string = ''.join(string_parts)
            related_fields_list = ''.join(related_fields_parts)

            insight_code = 'Rank Analysis#'+ dim + '#' + meas
            version_num = 0
            df_version_num_filtered = df_version_number[df_version_number['InsightCode'] == insight_code] 
            tags = dim + '|' + meas + '|' + '|'.join(list(df_temp_data[df_temp_data['Diff'] >= 0].index)) + '|' + '|'.join(list(df_temp_data[df_temp_data['Diff'] < 0].index)) + '|' + 'YTD' 
            if df_version_num_filtered.empty:
                version_num = 0
                importance = 143
            else:
                version_num = df_version_num_filtered['VersionNumber'].iloc[0]
                version_num += 1
                importance = df_version_num_filtered['Importance'].iloc[0] + 10
                tags = tags + '|' + 'Version: ' + str(version_num)

            if len(string) > 0:
                df_data.drop(['Diff'], axis = 1, inplace = True)
                df_data = df_data.T.to_json()
            # print(f'df_data in rank analysis:\n{df_data}')
            if string!= '':
#                 print(f'')
                ### Renaming ###
                string = rename_variables(string, rename_dim_meas)
                tags = rename_variables(tags, rename_dim_meas)
                ### Renaming ###
                # engine = azure_sql_database_connect(source_username, source_password, source_server, source_database)
                cnxn, cursor, logesys_engine = sql_connect()
                print(f'String:\n{string}')
                # insert_insights(datamart_id, str(string), str(df_data), 'Rank CY vs LY', 'Rank', str(related_fields_list), importance, tags, 'Rank Analysis', 'Insight', cnxn, cursor, insight_code, version_num)