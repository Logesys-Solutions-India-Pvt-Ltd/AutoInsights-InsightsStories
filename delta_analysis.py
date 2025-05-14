from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd
import numpy as np


def delta_analysis(datamart_id, sourcetype, source_engine, dim, meas, date_columns, dates_filter_dict, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, rename_dim_meas, df_list, df_list_ty, df_list_ly, dim_table, df_version_number, cnxn, cursor):
    print('--DELTA ANALYSIS--')
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
        split = 10
        tags = dim +'|' + meas + '|' 
        related_fields_list = []
        
        if sourcetype == 'xlsx':
            this_year_setting, last_year_setting = df_list_ty, df_list_ly
        elif sourcetype == 'table':
            this_year_setting, last_year_setting = 'ThisYear', 'LastYear'

#         df_ThisYearDimVal = ThisYear.groupby([dim])[meas].sum().rename('This Year').to_frame()
        df_ThisYearDimVal = parent_get_group_data(sourcetype, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio=False, is_total=False, is_others=False, outliers_val=None)
        df_ThisYearDimVal = df_ThisYearDimVal.rename(columns={meas: 'This Year'})
        df_ThisYearDimVal.replace([np.inf, -np.inf], 0, inplace=True)

#         df_LastYearDimVal = LastYear.groupby([dim])[meas].sum().rename('Last Year').to_frame()
        df_LastYearDimVal = parent_get_group_data(sourcetype, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, last_year_setting, is_ratio=False, is_total=False, is_others=False, outliers_val=None)
        df_LastYearDimVal = df_LastYearDimVal.rename(columns={meas: 'Last Year'})
        df_LastYearDimVal.replace([np.inf, -np.inf], 0, inplace=True)
        
        if '/' in derived_measures_dict[meas]['Formula']:
            is_ratio = True
        

# #         ly_meas = LastYear[meas].sum()
#       ly_meas = parent_get_group_data(sourcetype, source_engine, '', meas, date_columns, dates_filter_dict, '', derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, last_year_setting, is_ratio=True, is_total=True, is_others=False, outliers_val=None)
        ly_meas = parent_get_group_data(sourcetype, source_engine, '', meas, date_columns, dates_filter_dict, '',  derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, last_year_setting, True, True, False, None)

# #         ty_meas = ThisYear[meas].sum()
        ty_meas = parent_get_group_data(sourcetype, source_engine, '', meas, date_columns, dates_filter_dict, '',  derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, True, True, False, None)
        
        df_diff_val = pd.merge(df_LastYearDimVal, df_ThisYearDimVal, left_index = True, right_index = True, how = 'outer').fillna(0)
        df_diff_val[meas] = df_diff_val['This Year'] - df_diff_val['Last Year']
        df_diff_val.drop(['Last Year', 'This Year'], axis = 1, inplace = True)
        df_diff_val.sort_values(by = meas, ascending = False, inplace = True)
        df_diff_val.replace([np.inf, -np.inf], 0, inplace=True)
        
        if sourcetype == 'xlsx':
            all_years_setting = df_list
        elif sourcetype == 'table':
            all_years_setting = 'AllYears'

        df_diff_val_positive = df_diff_val[df_diff_val[meas] >= 0]
        # Only calculate others if rows > split+2
        if df_diff_val_positive.shape[0] > split+2:
            df_diff_val_positive, others_count, others_value = df_others(sourcetype, source_engine, df_diff_val_positive, split, all_years_setting, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, is_ratio, is_total=False)
            df_diff_val_positive = df_diff_val_positive.iloc[:split]
            df_diff_val_positive.loc[f"{others_count} Others (+)"] = others_value
        else:
            # No need for "others" row as we have fewer rows than the threshold
            others_count = 0
            others_value = 0
        
        df_diff_val_negative = df_diff_val[df_diff_val[meas] < 0]
        df_diff_val_negative = df_diff_val_negative.sort_values(by=meas, ascending=True)

        # Only calculate others if rows > split+2
        if df_diff_val_negative.shape[0] > split+2:
            df_diff_val_negative, others_count, others_value = df_others(sourcetype, source_engine, df_diff_val_negative, split, all_years_setting, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, is_ratio, is_total=False)
            df_diff_val_negative = df_diff_val_negative.iloc[:split]
            df_diff_val_negative.loc[f"{others_count} Others (-)"] = others_value
        else:
            # No need for "others" row
            others_count = 0
            others_value = 0        
        
        string_list = ''
        if df_diff_val[df_diff_val[meas] >= 0].shape[0] > 0:
            positive_string = blue_tag + 'YTD ' + meas + ' Growth: ' + b_span_tag + b_tag_open + str(df_diff_val[df_diff_val[meas] >= 0].shape[0]) + ' out of ' + str(df_diff_val.shape[0]) + ' ' + dim + b_tag_close + ' have registered positive growth. ' + b_tag_open + ', '.join(list(df_diff_val_positive[:3].index)) + b_tag_close + ' has increased the growth by ' + b_tag_open + human_format(df_diff_val_positive[:3][meas].sum()) + b_tag_close + '|'
            string_list = string_list + positive_string
            related_fields = ' -or- '.join([f'{dim} : {val}' for val in list(df_diff_val_positive[:3].index)]) + ' | ' + 'measure : ' + meas + ' | ' + 'functions : Story Avg CY vs LY ' + 'Increase'
#             print({related_fields})
            related_fields_list.append(related_fields)

        if df_diff_val[df_diff_val[meas] < 0].shape[0] > 0:
            negative_string = blue_tag + 'YTD ' + meas + ' Growth: ' + b_span_tag + b_tag_open + str(df_diff_val[df_diff_val[meas] < 0].shape[0]) + ' out of ' + str(df_diff_val.shape[0]) + ' ' + dim + b_tag_close + ' have registered negative growth. ' + b_tag_open + ', '.join(list(df_diff_val_negative[:3].index)) + b_tag_close + ' has decreased the growth by ' + b_tag_open + human_format(df_diff_val_negative[:3][meas].sum()) + b_tag_close
            string_list = string_list + negative_string
            related_fields = ' -or- '.join([f'{dim} : {val}' for val in list(df_diff_val_negative[:3].index)]) + ' | ' + 'measure : ' + meas + ' | ' + 'functions : Story Avg CY vs LY ' + 'Decrease'
            related_fields_list.append(related_fields)
        
        xAxisTitle = dim
        yAxisTitle = meas
        chart_title = 'YTD ' + meas + ' growth by ' + dim
        chartSubTitle = 'Overall delta is ' + human_format(ty_meas - ly_meas)
        chartFooterTitle = ''
        
        insight_code = 'Delta Analysis#' + dim + '#' + meas
        version_num = 0
        df_version_num_filtered = df_version_number[df_version_number['InsightCode'] == insight_code] 
        if df_version_num_filtered.empty:
            version_num = 0
            importance = 77
        else:
            version_num = df_version_num_filtered['VersionNumber'].iloc[0]
            version_num += 1
            importance = df_version_num_filtered['Importance'].iloc[0] + 10
            chartFooterTitle = chartFooterTitle + ' (Version: '+str(version_num) + ')'
            tags = tags + '|'.join(list(df_diff_val_positive[:3].index)) + '|' + '|'.join(list(df_diff_val_negative[:3].index)) + '|' + 'Version: ' + str(version_num)
        
        
        df_diff_val = pd.concat([df_diff_val_positive, df_diff_val_negative])
        waterfall = waterfallChart(dim, meas, df_diff_val, xAxisTitle, yAxisTitle, chart_title, chartSubTitle, chartFooterTitle, ty_meas, ly_meas)
        insight_id = uuid.uuid1()
#         importance = 0
        related_fields_list = "#|#".join(related_fields_list)
        # engine = azure_sql_database_connect(source_username, source_password, source_server, source_database)
        cnxn, cursor, logesys_engine = sql_connect()
        insert_insights(datamart_id, string_list, str(waterfall), 'Avg CY vs LY', 'Waterfall', str(related_fields_list), importance, tags, 'Delta Analysis', 'Insight', cnxn, cursor, insight_code, version_num)