from scipy import stats
from sklearn.linear_model import LinearRegression
from scipy.stats import zscore
from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd
import numpy as np
import constants


def outliers():
    constants.logger.info('--OUTLIERS--')
    datamart_id = constants.DATAMART_ID
    source_type = constants.SOURCE_TYPE
    source_engine = constants.SOURCE_ENGINE
    date_columns = constants.DATE_COLUMNS
    dates_filter_dict = constants.DATES_FILTER_DICT
    outliers_dates = constants.OUTLIERS_DATES
    max_date = constants.MAX_DATE
    Significant_dimensions = constants.SIGNIFICANT_DIMENSIONS
    rename_dim_meas = constants.RENAME_DIM_MEAS
    derived_measures_dict = constants.DERIVED_MEASURES_DICT
    derived_measures_dict_expanded = constants.DERIVED_MEASURES_DICT_EXPANDED
    dim_allowed_for_derived_metrics = constants.DIM_ALLOWED_FOR_DERIVED_METRICS
    insights_allowed_for_derived_metrics = constants.INSIGHTS_ALLOWED_FOR_DERIVED_METRICS
    df_list = constants.DF_LIST
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


    tags_list, related_fields_final_list, string_final_list, df_actual_list, growth_list, meas_list, charttitle_list,chartsubtitle_list, xAxisTitle_list, yAxisTitle_list = [],[],[],[],[],[],[],[], [], []
    importance_list, val_list = [], []
    outlier = 2
    split = 15
    is_ratio = False
    
#     dim = 'Category'
#     dim_table = 'df_sales'
#     meas = 'ASP'
#     val = 'MTD'
#     importance = 187
    
    for dim_table, dim_list in Significant_dimensions.items():
        for dim in dim_list:
            for meas in list(derived_measures_dict.keys()):
            # for meas in ['Markdown %', 'ASP', 'UPT', 'ATV']:
#             for meas in ['Markdown %']:
                if dim in dim_allowed_for_derived_metrics[meas]:
                    # logger.info(f'dim:{dim}, meas:{meas}')
                    if 'Outliers' in insights_allowed_for_derived_metrics[meas]:
                        for val, importance in zip(['Week On Week', 'Month On Month', 'Rolling 3 Months', 'MTD', 'YTD'], [220, 209, 198, 187, 176]):  
                            # logger.info(f'Val:{val}')
                            if val == 'MTD':
                                if source_type == 'xlsx':
                                    this_period_setting, last_period_setting = df_list_ty.copy(), df_list_ly.copy()
                                    modified_val_this_period, modified_val_last_period = 'ThisPeriodMTD', 'LastPeriodMTD'
                                elif source_type == 'table':
                                    this_period_setting, last_period_setting = 'ThisPeriodMTD', 'LastPeriodMTD'
                                    modified_val_this_period, modified_val_last_period = val, val
                            elif val == 'Rolling 3 Months':
                                if source_type == 'xlsx':
                                    this_period_setting, last_period_setting = df_list_ty.copy(), df_list_ly.copy()
                                    modified_val_this_period, modified_val_last_period = 'ThisPeriodR3M', 'LastPeriodR3M'
                                elif source_type == 'table':
                                    this_period_setting, last_period_setting = 'ThisPeriodR3M', 'LastPeriodR3M'
                                    modified_val_this_period, modified_val_last_period = val, val
                            elif val == 'YTD':
                                if source_type == 'xlsx':
                                    this_period_setting, last_period_setting = df_list_ty.copy(), df_list_ly.copy()
                                    modified_val_this_period, modified_val_last_period = 'ThisPeriodYTD', 'LastPeriodYTD'
                                elif source_type == 'table':
                                    this_period_setting, last_period_setting = 'ThisPeriodYTD', 'LastPeriodYTD'
                                    modified_val_this_period, modified_val_last_period = val, val
                            elif val == 'Week On Week':
                                if source_type == 'xlsx':
                                    this_period_setting, last_period_setting = df_list_ty.copy(), df_list_ly.copy()
                                    modified_val_this_period, modified_val_last_period = 'ThisPeriodWeekOnWeek', 'LastPeriodWeekOnWeek'
                                elif source_type == 'table':
                                    this_period_setting, last_period_setting = 'ThisPeriodWeekOnWeek', 'LastPeriodWeekOnWeek'
                                    modified_val_this_period, modified_val_last_period = val, val
                            elif val == 'Month On Month':
                                if source_type == 'xlsx':
                                    this_period_setting, last_period_setting = df_list_ty.copy(), df_list_ly.copy()
                                    modified_val_this_period, modified_val_last_period = 'ThisPeriodMonthOnMonth', 'LastPeriodMonthOnMonth'
                                elif source_type == 'table':
                                    this_period_setting, last_period_setting = 'ThisPeriodMonthOnMonth', 'LastPeriodMonthOnMonth'
                                    modified_val_this_period, modified_val_last_period = val, val

                            df_this_period = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_period_setting, is_ratio, is_total=False, is_others=False, outliers_val=modified_val_this_period, outliers_dates=outliers_dates)
                            df_this_period.index = df_this_period.index.map(lambda x: 'Blank' if x is None else x)
                            df_this_period.rename(columns = {meas : meas + ' Gr %'}, inplace = True)
                            df_this_period.sort_values(by = meas + ' Gr %', ascending = False, inplace = True)
                            df_this_period.replace([np.inf, -np.inf], 0, inplace=True)
                            
                            df_last_period = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, last_period_setting, is_ratio, is_total=False, is_others=False, outliers_val=modified_val_last_period, outliers_dates=outliers_dates)
                            df_last_period.index = df_last_period.index.map(lambda x: 'Blank' if x is None else x)
                            df_last_period.rename(columns = {meas : meas + ' Gr %'}, inplace = True)
                            df_last_period.sort_values(by = meas + ' Gr %', ascending = False, inplace = True)
                            df_last_period.replace([np.inf, -np.inf], 0, inplace=True)
                            
                            
                            tags = val + '|' + dim + '|' + meas + '|'
                            unique_list = list(set(list(df_last_period.iloc[:split].index) + list(df_this_period.iloc[:split].index)))

                            df_growth = pd.merge(df_this_period, df_last_period, left_index = True, right_index = True, how='outer', suffixes=('_this_year', '_last_year'))
                            df_growth = df_growth.fillna(0)
                            df_growth[meas + ' Gr %'] = (df_growth[meas + ' Gr %_this_year'] - df_growth[meas + ' Gr %_last_year'])/df_growth[meas + ' Gr %_last_year']
                            df_growth = df_growth.fillna(0)
                            df_growth = df_growth[~df_growth.applymap(np.isinf).any(axis=1)]

                            zscore_list = list(stats.zscore(list(df_growth[meas + ' Gr %'])))
                            df_growth['zscore'] = zscore_list
                            avg = round((df_this_period.sum() - df_last_period.sum())*100/df_last_period.sum(),2)[meas + ' Gr %']
                            df_final_growth = df_growth[df_growth.index.isin(unique_list)].drop(['zscore'], axis = 1)
                            df_final_growth.loc[str(df_growth[~(df_growth.index.isin(unique_list))].shape[0]) + ' Others', meas + ' Gr %'] = (df_growth[~(df_growth.index.isin(unique_list))][meas + ' Gr %_this_year'].sum() - df_growth[~(df_growth.index.isin(unique_list))][meas + ' Gr %_last_year'].sum())/ df_growth[~(df_growth.index.isin(unique_list))][meas + ' Gr %_last_year'].sum()
                            df_final_growth.drop([meas + ' Gr %_this_year', meas + ' Gr %_last_year'], axis = 1, inplace = True)
                            df_final_growth[meas + ' Gr %'] = round(df_final_growth[meas + ' Gr %'] * 100, 2)
                            df_final_growth['Average Gr %'] = avg
                            function_name = val
                            df_final_growth.replace([np.inf, -np.inf], 0, inplace=True)
                            
                            
                            for result, increase_decrease in zip(['Positive', 'Negative'], ['Increase', 'Decrease']):
                                if result == 'Positive':
                                    zscore_outlier = df_growth[(df_growth['zscore'] > outlier) & (df_growth[meas + ' Gr %'] != 0)][[meas + ' Gr %', 'zscore']]
                                else:
                                    zscore_outlier = df_growth[(df_growth['zscore'] < -outlier) & (df_growth[meas + ' Gr %'] != 0)][[meas + ' Gr %', 'zscore']]

                                zscore_outlier = zscore_outlier[zscore_outlier.index.isin(unique_list)]
                                zscore_outlier[meas + ' Gr %'] = round(zscore_outlier[meas + ' Gr %'] * 100, 2)

                                growth_list.append(zscore_outlier['zscore'].max())
                                string_list = '' 
                                related_fields_list = []
                                if(result == 'Positive'):
                                    for i in range(zscore_outlier.shape[0]):
                                        string = b_tag_open + val + ' ' + b_tag_close + meas + ' Gr % of ' + dim + ': '  + b_tag_open + zscore_outlier.index[i] + b_tag_close + ' is ' + blue_tag + str(zscore_outlier[meas + ' Gr %'].loc[zscore_outlier.index[i]]) + '%' + b_span_tag + ', ' + b_tag_open + 'Higher ' + b_tag_close + 'than Average Gr % ' + blue_tag + '(' + str(avg) + '%' + ')'+ b_span_tag
                                        string_list = string_list + string + '|'
                                else:
                                    for i in range(zscore_outlier.shape[0]):
                                        string = b_tag_open + val + ' ' + b_tag_close + meas + ' Gr % of ' + dim + ': '  + b_tag_open + zscore_outlier.index[i] + b_tag_close + ' is ' + blue_tag + str(zscore_outlier[meas + ' Gr %'].loc[zscore_outlier.index[i]]) + '%' + b_span_tag + ', ' + b_tag_open + 'Lesser ' + b_tag_close + 'than Average Gr % ' + blue_tag + '(' + str(avg) + '%' + ')'+ b_span_tag
                                        string_list = string_list + string + '|'
                                for i in range(zscore_outlier.shape[0]):
                                    related_fields = dim + ' : ' + zscore_outlier.index[i] + ' | ' + 'measure : ' + meas + ' | ' + 'function : ' + val + ' ' + increase_decrease
                                    related_fields_list.append(related_fields)
                                    tags = tags + zscore_outlier.index[i] + '|'
                                    importance  = importance + significance_score[dim_table][dim].loc[zscore_outlier.index[i]]['rank']

                                val_list.append(val)
                                df_final_growth.dropna(inplace = True)
                                df_final_growth.replace([np.inf, -np.inf], 0, inplace=True)
                                df_final_growth_styled = chart_index_styling(df_final_growth.copy(), list(zscore_outlier[meas + ' Gr %'].index), meas + ' Gr %', average = 'Average Gr %', def_color = '#B0CBFF', highlight_color = '#3862FF')
                                xAxisTitle = dim
                                yAxisTitle = meas + ' Gr %'
                                chart_title = val + ' growth % of ' + meas + ' for ' + dim
                                chartSubTitle = ''
                                chartFooterTitle = ''

                                insight_code= 'Outlier#' + dim + '#' + 'val' + '#' + meas
                                version_num = 0
                                df_version_num_filtered = df_version_number[df_version_number['InsightCode'] == insight_code] 
                                if df_version_num_filtered.empty:
                                    version_num = 0
                                else:
                                    version_num = df_version_num_filtered['VersionNumber'].iloc[0]
                                    version_num += 1
                                    importance = df_version_num_filtered['Importance'].iloc[0] + 10
                                    tags = tags + '|' + 'Version: ' + str(version_num)
                                    chartFooterTitle = chartFooterTitle + ' (Version: '+str(version_num) + ')'

                                importance_list.append(importance)
                                xAxisTitle_list.append(xAxisTitle)
                                yAxisTitle_list.append(yAxisTitle)
                                charttitle_list.append(chart_title)
                                chartsubtitle_list.append(chartSubTitle)
                                meas_list.append(meas)
                                df_actual_list.append(df_final_growth_styled)
                                tags_list.append(tags)
                                string_final_list.append(string_list)

                                related_fields_list = "#|#".join(related_fields_list)
                                related_fields_final_list.append(related_fields_list)
                            
    zscore_list_actual = [3.0, 2.5, 2.0, 1.5]
    count = 0
    diff = 0.5
    for j, zscore_val in enumerate(zscore_list_actual):  
        # logger.info(f'j:{j}')
        temp_count = 0
        for tags, related_fields, df_actual, string, zscore, meas, xAxis, yAxis, title, subtitle, importance,val in zip(tags_list, related_fields_final_list, df_actual_list, string_final_list, growth_list, meas_list, xAxisTitle_list, yAxisTitle_list, charttitle_list, chartsubtitle_list, importance_list, val_list):
            if j == 0:  
                if zscore >= zscore_val or zscore <= -zscore_val:
                    string = rename_variables(string, rename_dim_meas)
                    tags = rename_variables(tags, rename_dim_meas)
                    meas = rename_variables(meas, rename_dim_meas)
                    xAxis = rename_variables(xAxis, rename_dim_meas)
                    yAxis = rename_variables(yAxis, rename_dim_meas)
                    title = rename_variables(title, rename_dim_meas)
                    subtitle = rename_variables(subtitle, rename_dim_meas)
                    
                    data = ComboChart(df_actual, [meas + ' Gr %'] , ['Average Gr %'], [], xAxis , yAxis, title, subtitle, chartFooterTitle)
                    cnxn, cursor, logesys_engine = sql_connect()
                    insert_insights(datamart_id, string, data, val, 'Combo', str(related_fields), importance , tags, 'Outlier', 'Insight', cnxn, cursor, insight_code, version_num)
                    temp_count += 1
            elif j > 0:  
                if (zscore > zscore_val and zscore <= zscore_val + diff) or (zscore < -zscore_val and zscore >= -zscore_val - diff):
                    string = rename_variables(string, rename_dim_meas)
                    tags = rename_variables(tags, rename_dim_meas)
                    meas = rename_variables(meas, rename_dim_meas)
                    xAxis = rename_variables(xAxis, rename_dim_meas)
                    yAxis = rename_variables(yAxis, rename_dim_meas)
                    title = rename_variables(title, rename_dim_meas)
                    subtitle = rename_variables(subtitle, rename_dim_meas)
                    
                    data = ComboChart(df_actual, [meas + ' Gr %'] , ['Average Gr %'], [], xAxis , yAxis, title, subtitle, chartFooterTitle)
                    cnxn, cursor, logesys_engine = sql_connect()
                    insert_insights(datamart_id, string, data, val, 'Combo', str(related_fields), importance , tags, 'Outlier', 'Insight', cnxn, cursor, insight_code, version_num)
                    temp_count += 1
        count = count + temp_count
        # logger.info(f'count:{count}\n\n')
        if count >= 150:
            break