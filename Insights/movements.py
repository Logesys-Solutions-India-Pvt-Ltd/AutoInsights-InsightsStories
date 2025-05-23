from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd
import numpy as np
import constants


def movements(dim_table, dim, meas):
    print('--MOVEMENTS--')
    
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


    split = 10
    related_fields_list = []
    is_ratio = False
    df_data = pd.DataFrame()
    df_others_value = pd.DataFrame()

    if source_type == 'xlsx':
        this_year_setting, last_year_setting = df_list_ty, df_list_ly
    elif source_type == 'table':
        this_year_setting, last_year_setting = 'ThisYear', 'LastYear'
        
#     df_ThisYearDimMean = ThisYear.groupby([dim])[meas].sum().to_frame()
#    df_data = parent_get_group_data(sourcetype, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio, is_total=False, is_others=False, outliers_val=None)   

    df_ThisYearDimMean = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio, is_total=False, is_others=False, outliers_val=None) 
    df_ThisYearDimMean = df_ThisYearDimMean.reset_index()
    
#     df_LastYearDimMean = LastYear.groupby([dim])[meas].sum().to_frame()
    df_LastYearDimMean = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, last_year_setting, is_ratio, is_total=False,is_others=False, outliers_val=None) 
    df_LastYearDimMean = df_LastYearDimMean.reset_index()
    
    df_data = pd.merge(
        df_ThisYearDimMean,
        df_LastYearDimMean,
        on=dim,  
        how='outer' 
    )
    df_data = df_data.set_index(dim)
    df_data.rename(columns={meas+'_x': 'This Year', meas+'_y': 'Last Year'}, inplace=True)
    df_data['Last Year'] = round(df_data['Last Year'].astype(float),2)
    df_data['This Year'] = round(df_data['This Year'].astype(float),2)
    
    shape = df_data.shape[0]
    
    if '%' not in meas:
        df_data['Growth%'] = (round(((df_data['This Year'] - df_data['Last Year']) * 100)/ df_data['Last Year'],2))
    elif '%' in meas:
        df_data['Growth%'] = round(df_data['This Year'] - df_data['Last Year'],2)
    
    df_data['Abs Growth%'] = abs(df_data['Growth%'])
    df_data.replace([np.inf, -np.inf], 0, inplace=True)
    df_data.fillna(0, inplace = True)

    df_data = df_data.sort_values(by = 'Abs Growth%', ascending = False)
    

    df_data, others_count_ty, others_value_ty = df_others(source_type, source_engine, df_data, split, this_year_setting, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, is_ratio, is_total=False)
    df_data, others_count_ly, others_value_ly = df_others(source_type, source_engine, df_data, split, last_year_setting, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, is_ratio, is_total=False)
    df_data = df_data.head(split)
    
    if not (others_count_ty == 0 and others_count_ly == 0):
        df_others_value = pd.DataFrame(
            {
                'This Year': [others_value_ty], 
                'Last Year': [others_value_ly]  
            },
            index=[f"{max(others_count_ty, others_count_ly)} others"]  
        )
        df_others_value.index.name = dim
        
    df_data.drop(['Growth%', 'Abs Growth%'], axis=1, inplace=True)
    
    df_data = pd.concat([df_data, df_others_value]) 
    df_data['Diff'] = df_data['This Year'] - df_data['Last Year']
    
    if '%' not in meas:
        df_data['Growth%'] = (round(((df_data['This Year'] - df_data['Last Year']) * 100)/ df_data['Last Year'],2))
    elif '%' in meas:
        df_data['Growth%'] = round(df_data['This Year'] - df_data['Last Year'],2)
    
    df_data['Abs Growth%'] = abs(df_data['Growth%'])
#     print(f'df_data after calc Abs Growth %:\n{df_data}')
    
    if shape > split + 1:
        # df_data = df_data.iloc[:-1].sort_values(by = 'Abs Growth%', ascending = False).append(df_data[-1:])
        last_row = df_data.iloc[[-1]]  # Select the last row as a DataFrame
        df_except_last = df_data.iloc[:-1].sort_values(by='Abs Growth%', ascending=False)
        df_data = pd.concat([df_except_last, last_row])
        unique_list = list(df_data.iloc[:-1].index)
    else:
        df_data = df_data.sort_values(by = 'Abs Growth%', ascending = False)
        unique_list = list(df_data.index)

    df_temp_data = df_data[(df_data['Abs Growth%'] > 30) & (df_data.index.isin(unique_list))][0:3]

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

    string= ''
    if shape > split + 1:
        # df_data = df_data.iloc[:-1].sort_values(by = 'Abs Growth%', ascending = False).append(df_data[-1:])
        df_data = pd.concat([df_data.iloc[:-1].sort_values(by='Abs Growth%', ascending=False), df_data.iloc[[-1]]])
    else:
        df_data = df_data.sort_values(by = 'Abs Growth%', ascending = False)
    
    importance_list = []
    if df_temp_data.shape[0] > 1:
        for i, j in zip(list(df_temp_data[(df_temp_data['Abs Growth%'] > 30)].index), list(df_temp_data[(df_temp_data['Abs Growth%'] > 30)]['Growth%'])):
            delta_percent = df_temp_data.loc[i]['Diff']
            if delta_percent >= 0:
                action = 'increased'
                action_description = 'Rank CY vs LY Increase'
            else:
                action = 'decreased'
                action_description = 'Rank CY vs LY Decrease'

            if df_temp_data.loc[i]['Last Year'] == 0 or np.isinf(j):
                j = human_format(df_temp_data.loc[i]['This Year'])
            
            # string = string + 'YTD ' + meas + ' in ' + b_tag_open + dim + ': ' + i + b_tag_close + ' has ' + blue_tag + action + b_span_tag + ' ' + b_tag_open + str(j) + '% ' + '(' + str(human_format(df_temp_data.loc[i]['This Year'] - df_temp_data.loc[i]['Last Year'])) + ')' + b_tag_close + ', from ' + b_tag_open + str(human_format(df_temp_data.loc[i]['Last Year'])) + b_tag_close + ' last year to ' + b_tag_open + str(human_format(df_temp_data.loc[i]['This Year'])) + b_tag_close + ' this year|'
            string = f"{string}YTD {meas} in {dim}: {i}{b_tag_close} has {blue_tag}{action}{b_span_tag} {j}% ({human_format(df_temp_data.loc[i]['This Year'] - df_temp_data.loc[i]['Last Year'])}){b_tag_close}, from {human_format(df_temp_data.loc[i]['Last Year'])}{b_tag_close} last year to {human_format(df_temp_data.loc[i]['This Year'])}{b_tag_close} this year|"
            related_fields = dim + ' : ' + i + ' | ' + 'measure : ' + meas + ' | function : Story ' + action_description
            related_fields_list.append(related_fields)
            
            importance_list.append(significance_score[dim_table][dim].loc[i]['rank'])
            
        df_data.fillna(0, inplace = True)
        df_data.replace([np.inf, -np.inf], 0, inplace=True)
        
        df_data = chart_index_styling(df_data, list(df_temp_data[df_temp_data['Abs Growth%'] > 30].index), 'Last Year', def_color = '#D3D4D6',  highlight_color = '#656565')
        df_data = chart_index_styling(df_data, list(df_temp_data[df_temp_data['Abs Growth%'] > 30].index), 'This Year', def_color = '#B0CBFF', highlight_color = '#3862FF')
        df_data = chart_index_styling(df_data, list(df_temp_data[df_temp_data['Abs Growth%'] > 30].index), 'Growth%', average = 'empty', def_color = '#B0CBFF', highlight_color='#3862FF')
        df_data.drop(['Abs Growth%','Diff'], axis = 1, inplace = True)
        
        xAxisTitle = dim
        yAxisTitle = meas
        yAxisTitleRight = 'Growth %'
        chart_title = 'YTD ' + meas + ' growth by ' + dim
        chartSubTitle = ''
        chartFooterTitle = ''
        
        insight_code = 'Movements#' + dim + '#' + meas
        version_num = 0
        df_version_num_filtered = df_version_number[df_version_number['InsightCode'] == insight_code] 
        tags = dim + '|' + meas + '|' + '|'.join(list(df_temp_data[(df_temp_data['Growth%'] > 30) & (df_temp_data['Diff'] < 0)].index)) + '|' + '|'.join(list(df_temp_data[(df_temp_data['Growth%'] > 30) & (df_temp_data['Diff'] >= 0)].index)) + '|' + 'YTD'
        if df_version_num_filtered.empty:
            version_num = 0
            importance = 154 + np.mean(importance_list)
        else:
            version_num = df_version_num_filtered['VersionNumber'].iloc[0]
            version_num += 1
#             importance = df_version_num_filtered['Importance'].iloc[0] + 10
            importance = df_version_num_filtered['Importance'].iloc[0] + np.mean(importance_list)
            chartFooterTitle = chartFooterTitle + ' (Version: '+str(version_num) + ')'
            tags = tags + '|' + 'Version: ' + str(version_num)
            
        # print(f'df_data:\n{df_data}')
        df_data = ComboChart(df_data, ['This Year', 'Last Year'], [], ['Growth%'], xAxisTitle, yAxisTitle, chart_title, chartSubTitle, chartFooterTitle, yAxisTitleRight = yAxisTitleRight)
        related_fields_list = "#|#".join(related_fields_list)
        
        ### Renaming ###
        string = rename_variables(string, rename_dim_meas)
        xAxisTitle = rename_variables(xAxisTitle, rename_dim_meas)
        yAxisTitle = rename_variables(yAxisTitle, rename_dim_meas)
        chart_title = rename_variables(chart_title, rename_dim_meas)
        tags = rename_variables(tags, rename_dim_meas)
        chartSubTitle = rename_variables(chartSubTitle, rename_dim_meas)
        ### Renaming ###
        # engine = azure_sql_database_connect(source_username, source_password, source_server, source_database)
        cnxn, cursor, logesys_engine = sql_connect()
        print(f'String:\n{string}')
        # insert_insights(datamart_id, str(string), str(df_data), 'Avg CY vs LY', 'Combo', str(related_fields_list), importance, tags, 'Movements', 'Insight', cnxn, cursor, insight_code, version_num)