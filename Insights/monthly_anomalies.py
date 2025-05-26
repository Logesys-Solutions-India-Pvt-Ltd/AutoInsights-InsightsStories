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


def monthly_anomalies():
    constants.logger.info('--MONTHLY ANOMALIES--')
    datamart_id = constants.DATAMART_ID
    source_type = constants.SOURCE_TYPE
    source_engine = constants.SOURCE_ENGINE
    date_columns = constants.DATE_COLUMNS
    dates_filter_dict = constants.DATES_FILTER_DICT
    max_date = constants.MAX_DATE
    Significant_dimensions = constants.SIGNIFICANT_DIMENSIONS
    rename_dim_meas = constants.RENAME_DIM_MEAS
    derived_measures_dict = constants.DERIVED_MEASURES_DICT
    derived_measures_dict_expanded = constants.DERIVED_MEASURES_DICT_EXPANDED
    dim_allowed_for_derived_metrics = constants.DIM_ALLOWED_FOR_DERIVED_METRICS
    insights_allowed_for_derived_metrics = constants.INSIGHTS_ALLOWED_FOR_DERIVED_METRICS
    df_list_last12months = constants.DF_LIST_LAST12MONTHS
    df_relationship = constants.DF_RELATIONSHIP
    df_sql_table_names = constants.DF_SQL_TABLE_NAMES
    df_sql_meas_functions = constants.DF_SQL_MEAS_FUNCTIONS
    significance_score = constants.SIGNIFICANCE_SCORE
    df_version_number = constants.DF_VERSION_NUMBER
    cnxn = constants.CNXN
    cursor = constants.CURSOR
    logesys_engine = constants.LOGESYS_ENGINE


    tags_list, related_fields_list, string_list, df_actual_list, zscore_list, meas_list, charttitle_list,chartsubtitle_list, xAxisTitle_list, yAxisTitle_list, importance_list = [],[],[],[],[],[],[],[],[],[],[]
    
    for dim_table, dim_list in Significant_dimensions.items():
        for dim in dim_list:
            for meas in list(derived_measures_dict.keys()):
                if dim in dim_allowed_for_derived_metrics[meas]:
                    # logger.info(f'dim:{dim}, meas:{meas}')
                    if 'Monthly Anomalies' in insights_allowed_for_derived_metrics[meas]:
                        start_of_last_12_months, start_of_month = calculate_month_dates(max_date)
                        start_of_last_12_months = start_of_last_12_months.strftime("%d-%m-%Y")
                        start_of_month = start_of_month.strftime("%d-%m-%Y")
                        is_ratio=False
                        if source_type == 'xlsx':
                            all_years_setting = df_list_last12months
                        elif source_type == 'table':
                            all_years_setting = 'Last12Months'

                        df_monthly_data = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, all_years_setting, is_ratio, is_total=False, is_others=False, extra_groupby_col='Year-Month') 
                        df_monthly_data['Year-Month'] = pd.Categorical(df_monthly_data['Year-Month'], 
                                                                        categories=pd.date_range(start_of_last_12_months, max_date, freq='M').strftime('%Y-%b'),
                                                                        ordered=True)
                        df_monthly_data = df_monthly_data.sort_values('Year-Month')
                        df_monthly_data[meas].fillna(0, inplace = True)
                        df_monthly_data.replace([np.inf, -np.inf], 0, inplace=True)

                        all_combinations_dict = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, 
                                                                df_sql_meas_functions, df_relationship, all_years_setting, is_ratio, is_total=False, is_others=False, 
                                                                extra_groupby_col=None, other_operation_column='Year-Month', other_operation=True)   
                        if source_type == 'xlsx':
                            all_combinations = list(all_combinations_dict['unique_values'][0])
                        elif source_type == 'table':
                            all_combinations = all_combinations_dict['unique_values'][0].split(', ')
                            # Convert strings to datetime objects for sorting
                            all_combinations = sorted(all_combinations, key=lambda x: datetime.strptime(x, '%Y-%b'))

                        combinations_df = pd.DataFrame(all_combinations, columns=['Year-Month'])
                        for value in list(significance_score[dim_table][dim][:].index):
                            df_monthly_data_filtered = df_monthly_data[df_monthly_data[dim] == value]
                            df_monthly_data_filtered['zscore'] = np.round(stats.zscore(df_monthly_data_filtered[meas]), 2)
                            df_monthly_data_filtered.drop([dim], axis = 1, inplace = True)
                            df_monthly_data_filtered = pd.merge(combinations_df[['Year-Month']], df_monthly_data_filtered, on=['Year-Month'], how='left').fillna(0)

                            for z_thres, action, jan_action in zip([-2, 2], ['Dropped', 'Increased'], ['Lower', 'Higher']):
                                if z_thres <= 0:
                                    month_list = list(df_monthly_data_filtered[df_monthly_data_filtered['zscore'] <= z_thres]['Year-Month'])
                                    zscore_temp = df_monthly_data_filtered['zscore'].min()
                                else:    
                                    month_list = list(df_monthly_data_filtered[df_monthly_data_filtered['zscore'] >= z_thres]['Year-Month'])
                                    zscore_temp = df_monthly_data_filtered['zscore'].max()                    

                                if month_list:
                                    zscore_list.append(zscore_temp)
                                    xAxisTitle = ''
                                    yAxisTitle = meas
                                    chart_title = f'Month-wise {meas}'
                                    chartSubTitle = f'{dim}: {value}'
                                    chartFooterTitle = 'Showing last 12 months'

                                    insight_code = 'Monthly Anomalies#' + dim + '#' + value + '#' + meas
                                    version_num = 0
                                    df_version_num_filtered = df_version_number[df_version_number['InsightCode'] == insight_code] 
                                    tags = ''
                                    if df_version_num_filtered.empty:
                                        version_num = 0
                                        importance = 121 + significance_score[dim_table][dim].loc[value]['rank']
                                    else: 
                                        version_num = df_version_num_filtered['VersionNumber'].iloc[0]
                                        version_num += 1
                #                             importance = df_version_num_filtered['Importance'].iloc[0] + 10
                                        importance = df_version_num_filtered['Importance'].iloc[0] + significance_score[dim_table][dim].loc[value]['rank']
                                        chartFooterTitle = chartFooterTitle + ' (Version: '+str(version_num) + ')'
                                        tags = tags + 'Version: ' + str(version_num) + '|'
                                    month_description = str(month_list[0])
                                    related_fields = f'{dim} : {value} | measure : {meas} | functions : {action} Monthly Dip # {month_description}'
                                    tags = f'{dim}|{value}|{meas}|{action}||' + '|'.join(month_list) 
                                    related_fields_list.append(related_fields)
                                    tags_list.append(tags)

                                    start_of_month_dt = datetime.strptime(start_of_month, "%d-%m-%Y")
                                    if len(month_list) == 1 and month_description == start_of_month_dt.strftime('%Y-%b'):
                                        string = f'{b_tag_open}{meas}{b_tag_close} for {dim}: {b_tag_open}{value}{b_tag_close} was {b_tag_open}{jan_action}{b_tag_close} in {", ".join(month_list)} compared to other months'
                                    else:
                                        string = f'{b_tag_open}{meas}{b_tag_close} for {dim}: {value} {blue_tag}{action}{b_span_tag} in {b_tag_open}{", ".join(month_list)}{b_tag_close}'

                                    importance_list.append(importance)
                                    xAxisTitle_list.append(xAxisTitle)
                                    yAxisTitle_list.append(yAxisTitle)
                                    string_list.append(string)
                                    meas_list.append(meas)
                                    charttitle_list.append(chart_title)
                                    chartsubtitle_list.append(chartSubTitle)
                                    df_actual_list.append(df_monthly_data_filtered[['Year-Month', meas]].set_index('Year-Month'))
                            
    zscore_list_actual = [3.0, 2.5, 2.0, 1.5]
    count = 0
    diff = 0.5
    for j, zscore_val in enumerate(zscore_list_actual):
        # logger.info(f'j:{j}')
        temp_count = 0
        for tags, related_fields, df_actual, string, zscore, meas, title, subtitle, xAxis, yAxis, importance in zip(tags_list, related_fields_list, df_actual_list, string_list, zscore_list, meas_list, charttitle_list, chartsubtitle_list,xAxisTitle_list, yAxisTitle_list,importance_list):
            if j == 0:  
                if zscore >= zscore_val or zscore <= -zscore_val:
                    string = rename_variables(string, rename_dim_meas)
                    yAxis = rename_variables(yAxis, rename_dim_meas)
                    title = rename_variables(title, rename_dim_meas)
                    subtitle = rename_variables(subtitle, rename_dim_meas)
                    tags = rename_variables(tags, rename_dim_meas)
                    
                    data = LineChart(df_actual, [meas], [], xAxis, yAxis, title, subtitle, chartFooterTitle, non_highlight_color='#B0CBFF', highlight_color='#3862FF')
                    cnxn, cursor, logesys_engine = sql_connect()
                    insert_insights(datamart_id, str(string), str(data), 'Related Measures', 'Line', str(related_fields), importance, tags, 'Monthly Anomalies', 'Insight', cnxn, cursor, insight_code, version_num)
                    temp_count += 1
            elif j > 0:  
                if (zscore > zscore_val and zscore <= zscore_val + diff) or (zscore < -zscore_val and zscore >= -zscore_val - diff):                    
                    string = rename_variables(string, rename_dim_meas)
                    yAxis = rename_variables(yAxis, rename_dim_meas)
                    title = rename_variables(title, rename_dim_meas)
                    subtitle = rename_variables(subtitle, rename_dim_meas)
                    tags = rename_variables(tags, rename_dim_meas)
                    
                    data = LineChart(df_actual, [meas], [], xAxis, yAxis, title, subtitle, chartFooterTitle, non_highlight_color='#B0CBFF', highlight_color='#3862FF')
                    cnxn, cursor, logesys_engine = sql_connect()
                    insert_insights(datamart_id, str(string), str(data), 'Related Measures', 'Line', str(related_fields), importance, tags, 'Monthly Anomalies', 'Insight', cnxn, cursor, insight_code, version_num)
                    temp_count += 1
        count = count + temp_count
        # logger.info(f'count:{count}\n\n')
        if count >= 150:
            break