from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd
import numpy as np
import constants


def hi_pots(dim_table, dim, meas):
    print('--HI POTS--')
    datamart_id = constants.DATAMART_ID
    source_type = constants.SOURCE_TYPE
    source_engine = constants.SOURCE_ENGINE
    date_columns = constants.DATE_COLUMNS
    dates_filter_dict = constants.DATES_FILTER_DICT
    rename_dim_meas = constants.RENAME_DIM_MEAS
    derived_measures_dict = constants.DERIVED_MEASURES_DICT
    derived_measures_dict_expanded = constants.DERIVED_MEASURES_DICT_EXPANDED
    df_list_ty = constants.DF_LIST_TY
    df_relationship = constants.DF_RELATIONSHIP
    df_sql_table_names = constants.DF_SQL_TABLE_NAMES
    df_sql_meas_functions = constants.DF_SQL_MEAS_FUNCTIONS
    df_version_number = constants.DF_VERSION_NUMBER
    cnxn = constants.CNXN
    cursor = constants.CURSOR
    logesys_engine = constants.LOGESYS_ENGINE


    related_fields_list = []
    split = 10
    is_ratio = False    
    df_others_value = pd.DataFrame()
    
    if source_type == 'xlsx':
        this_year_setting = df_list_ty
    elif source_type == 'table':
        this_year_setting = 'ThisYear'
    
# #     df_data = ThisYear.groupby([dim])[meas].mean().to_frame()
    df_data = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio, is_total=False, is_others=False, outliers_val=None)   
    df_data.sort_values(by = meas, ascending=False, inplace=True)
                                        # df_others(sourcetype, source_engine, df_data, split, df_to_use, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, is_ratio, is_total):

    df_data, others_count, others_value = df_others(source_type, source_engine, df_data, split, this_year_setting, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions,df_relationship, is_ratio, False)   
    df_data = df_data.head(split)
    
    
    if not (others_count == 0):
        df_others_value = pd.DataFrame({meas: [others_value]}, index=[f"{others_count} others"])
        df_others_value.index.name = dim
        
    df_data = pd.concat([df_data, df_others_value]) 
    
    
    if '/' in derived_measures_dict[meas]['Formula']:
        is_ratio = True
        
    for key, value in derived_measures_dict[meas].items():
        if key != 'Formula': 
            if 'mean()' in value:
                is_ratio = True
#     average = round(ThisYear[meas].mean(), 2)
    if is_ratio:
        average = parent_get_group_data(source_type, source_engine, '', meas, date_columns, dates_filter_dict, '', derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio = True, is_total = False,is_others=False, outliers_val=None) 
    else:
        average = df_data[meas].mean()

    df_data = round(df_data, 2)
    # print(f'df_data in hi pots after others:\n{df_data}')
    average = round(average, 2)
    
    df_data['Average'] = average
    df_data['X Times'] = round(df_data[meas] / average, 2)
    df_data.replace([np.inf, -np.inf], 0, inplace=True)
    
    df_temp_data = df_data[df_data['X Times'] > 1.3].sort_values(by='X Times', ascending=False)[:3]
    df_temp_data.replace([np.inf, -np.inf], 0, inplace=True)
    
#     print(f'df_temp_data in hi pots:\n{df_temp_data}')
    ## Percentage and Units
    is_percentage, meas_units = '', ''
    show_in_percentage_query = f"""
                        SELECT ShowInPercentage, Units 
                        FROM derived_metrics
                        WHERE DatamartId = '{datamart_id}'
                        AND MetricName = '{meas}'
                        """
    
    show_in_percentage_result = query_on_table(show_in_percentage_query, logesys_engine)

    if not show_in_percentage_result.empty:
        show_in_percentage = show_in_percentage_result['ShowInPercentage'][0]
        if show_in_percentage:
            is_percentage = '%'
        
        meas_units = show_in_percentage_result['Units'][0]

    string = ''
    if df_temp_data.shape[0] > 1 and average != 0:
        for i, j in zip(df_temp_data.index, df_temp_data['X Times']):
            if i in df_data.index:
                string += f'{meas} of {b_tag_open}{dim} : {i}{b_tag_close} is {b_tag_open}{human_format(df_temp_data.loc[i][meas])}{meas_units}{is_percentage}{b_tag_close} which is {b_tag_open}{j}x{b_tag_close} the overall Average ({human_format(average)}{meas_units})|'
                related_fields = f'{dim} : {i} | measure : {meas} | function : Story X times'
                related_fields_list.append(related_fields)
        df_data.fillna(0, inplace=True)
        df_data.drop(['X Times'], axis=1, inplace=True)
        df_data['Average'] = df_data['Average'].max()
#         print(f'df_data in hi pots:\n{df_data}')
        df_data = chart_index_styling(df_data, list(df_temp_data.index), meas, average='Average', def_color='#B0CBFF',highlight_color='#3862FF')
        xAxisTitle = dim
        yAxisTitle = f'{meas}'
        chart_title = f'{meas} by {dim}'
        chartSubTitle = ''
        chartFooterTitle = ''

        insight_code = 'Hi-Pots#' + dim + '#' + meas
        version_num = 0
        df_version_num_filtered = df_version_number[df_version_number['InsightCode'] == insight_code] 
        tags = f'{dim}|{meas}|{"|".join(df_temp_data.index)}'

        if df_version_num_filtered.empty:
            version_num = 0
            importance = 165
        else:
            version_num = df_version_num_filtered['VersionNumber'].iloc[0]
            version_num += 1
            importance = df_version_num_filtered['Importance'].iloc[0] + 10
            chartFooterTitle = chartFooterTitle + ' (Version: '+str(version_num) + ')'
#             tags = tags + '|' + 'Version: ' + str(version_num)


        df_data.rename(columns={meas: f'{meas}'}, inplace=True)
        df_data = ComboChart(df_data, [f'{meas}'], ['Average'], [], xAxisTitle, yAxisTitle, chart_title,chartSubTitle, chartFooterTitle)
        related_fields_list = "#|#".join(related_fields_list)
        
        string = rename_variables(string, rename_dim_meas)
        xAxisTitle = rename_variables(xAxisTitle, rename_dim_meas)
        yAxisTitle = rename_variables(yAxisTitle, rename_dim_meas)
        chart_title = rename_variables(chart_title, rename_dim_meas)
        tags = rename_variables(tags, rename_dim_meas)
        cnxn, cursor, logesys_engine = sql_connect()
        print(f'String:\n{string}')
        # insert_insights(datamart_id, str(string), str(df_data), 'X Times', 'Combo', str(related_fields_list), importance,tags, 'Hi-Pots', 'Insight', cnxn, cursor, insight_code, version_num)