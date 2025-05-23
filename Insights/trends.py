from sklearn.linear_model import LinearRegression
from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd
import numpy as np
import constants


def trends():
    print('--TRENDS--')
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

    tags_list, related_fields_list, string_list, df_actual_list, cut_off_list, meas_list, charttitle_list,chartsubtitle_list, xAxisTitle_list, yAxisTitle_list, importance_list = [],[],[],[],[],[],[],[],[],[],[]
    # dim_table = 'Location_Dist'
    # dim = 'Store Name'
    # meas = 'Markdown %'
    for dim_table, dim_list in Significant_dimensions.items():
        for dim in dim_list:
            for meas in list(derived_measures_dict.keys()):
                if dim in dim_allowed_for_derived_metrics[meas]:
                    print(f'dim:{dim}, meas:{meas}')
                    if 'Trends' in insights_allowed_for_derived_metrics[meas]:
                        start_of_last_12_months, start_of_month = calculate_month_dates(max_date)
                        start_of_last_12_months = start_of_last_12_months.strftime("%d-%m-%Y")
                        start_of_month = start_of_month.strftime("%d-%m-%Y")
                        is_ratio=False
                        if source_type == 'xlsx':
                            all_years_setting = df_list_last12months
                        elif source_type == 'table':
                            all_years_setting = 'Last12Months'

                        df_data = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, all_years_setting, is_ratio, is_total=False, is_others=False, extra_groupby_col='Year-Month') 
                        df_data['Year-Month'] = pd.Categorical(df_data['Year-Month'], 
                                                                            categories=pd.date_range(start_of_last_12_months, max_date, freq='M').strftime('%Y-%b'),
                                                                            ordered=True)
                        df_data = df_data.sort_values('Year-Month')
                        df_data[meas].fillna(0, inplace = True)
                        df_data.replace([np.inf, -np.inf], 0, inplace=True)

                        lr = LinearRegression()
                        cut_off = 0.7
                        all_dims = df_data[dim].unique()

                        all_combinations_dict = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, 
                                                                    df_sql_meas_functions, df_relationship, all_years_setting, is_ratio, is_total=False, is_others=False, 
                                                                    extra_groupby_col=None, other_operation_column='Year-Month', other_operation=True)   
                        if source_type == 'xlsx':
                            all_combinations = list(all_combinations_dict['unique_values'][0])
                        elif source_type == 'table':
                            all_combinations = all_combinations_dict['unique_values'][0].split(', ')
                            # Convert strings to datetime objects for sorting
                            all_combinations = sorted(all_combinations, key=lambda x: datetime.strptime(x, '%Y-%b'))

                        combinations_df = pd.DataFrame([(year_month, zone, 0.0) for year_month in all_combinations for zone in all_dims],columns=df_data.columns)
                        df_data = pd.merge(combinations_df[['Year-Month', dim]], df_data, on=['Year-Month', dim], how='left')#.fillna(0)
                        df_data[meas].fillna(0, inplace = True)

                        for dim_val in list(significance_score[dim_table][dim][:].index):
                            df_filtered = df_data[df_data[dim] == dim_val]
                    #                     print(f'df_filtered:\n{df_filtered}')
                            if df_filtered.shape[0] > 1:
                                df_filtered['Year-Month LE'] = range(1, df_filtered['Year-Month'].shape[0] + 1)
                                df_filtered[dim].fillna(0, inplace = True)
                                df_actual = df_filtered.set_index('Year-Month')[[meas]]

                                x = df_filtered['Year-Month LE'].values.reshape(-1,1)
                                y = df_actual[meas]
                                lr.fit(x, y)
                                df_actual['Trend'] = pd.DataFrame({'Trend' : lr.predict(x)}).set_index(df_filtered['Year-Month'])
                                # corr = round(df_filtered.corr(),2)
                                corr = round(df_filtered[[meas, 'Year-Month LE']].corr(), 2)

                                if(corr.loc['Year-Month LE'][meas]  >= cut_off) or (corr.loc['Year-Month LE'][meas]  <= -cut_off):
                                    x = df_filtered['Year-Month LE'].values.reshape(-1,1)
                                    y = df_actual[meas]
                                    lr.fit(x, y)
                                    df_actual['Trend'] = pd.DataFrame({'Trend' : lr.predict(x)}).set_index(df_filtered['Year-Month'])
                                    # corr = round(df_filtered.corr(),2)
                                    corr = round(df_filtered[[meas, 'Year-Month LE']].corr(), 2)

                                    xAxisTitle = ''
                                    increasing_decreasing = ''
                                    yAxisTitle = meas
                                    chart_title = 'Monthly trend of ' + meas
                                    chartSubTitle = dim  + ': ' + dim_val
                                    chartFooterTitle = 'Showing last 12 months'

                                    insight_code = 'Trends#' + dim + '#' + dim_val + '#' + meas
                                    version_num = 0
                                    df_version_num_filtered = df_version_number[df_version_number['InsightCode'] == insight_code] 

                                    df_actual_list.append(df_actual)
                                    related_fields = dim  + ' : ' + dim_val + ' | ' + 'measure : ' + meas + ' | ' + 'function :'
                                    string = 'For ' + b_tag_open + dim  + ': ' + dim_val + b_tag_close +' the ' + b_tag_open + meas + b_tag_close + ' is in ' + blue_tag
                                    if(corr.loc['Year-Month LE'][meas]  >= cut_off):
                                        increasing_decreasing = 'Increasing'
                                    elif(corr.loc['Year-Month LE'][meas]  <= -cut_off):
                                        increasing_decreasing = 'Decreasing'

                                    tags = dim + '|' + meas + '|' + dim_val + '|' + increasing_decreasing + '|' + 'Last 12 Months' + '|' + "Trends" 
                                    if df_version_num_filtered.empty:
                                        version_num = 0
                                        importance = 132 + significance_score[dim_table][dim].loc[dim_val]['rank']
                                    else:
                                        version_num = df_version_num_filtered['VersionNumber'].iloc[0]
                                        version_num += 1
                        #                             importance = df_version_num_filtered['Importance'].iloc[0] + 10
                                        importance = df_version_num_filtered['Importance'].iloc[0] + significance_score[dim_table][dim].loc[dim_val]['rank']
                                        chartFooterTitle = chartFooterTitle + ' (Version: '+str(version_num) + ')'
                                        tags = tags + '|' + 'Version: ' + str(version_num)

                                    importance_list.append(importance)
                                    xAxisTitle_list.append(xAxisTitle)
                                    yAxisTitle_list.append(yAxisTitle)
                                    charttitle_list.append(chart_title)
                                    chartsubtitle_list.append(chartSubTitle)
                                    tags_list.append(tags)
                                    related_fields = related_fields + ' ' + increasing_decreasing + ' Slope'
                                    related_fields_list.append(related_fields)
                                    string = string + increasing_decreasing + ' trend' + b_span_tag + ' for last 12 months' 
                                    string_list.append(string)
                                    cut_off_list.append(corr.loc['Year-Month LE'][meas])
                                    meas_list.append(meas)

    cut_off_list_actual = [0.95, 0.90, 0.85, 0.80, 0.75, 0.70, 0.65]
    count = 0
    diff = 0.05
    for j, cut_off_val in enumerate(cut_off_list_actual):
        print(f'j:{j}')
        temp_count = 0
        for tags, related_fields, df_actual, string, cut_off, meas, title, subtitle, xAxis, yAxis, importance in zip(tags_list, related_fields_list, df_actual_list, string_list, cut_off_list, meas_list, charttitle_list, chartsubtitle_list, xAxisTitle_list, yAxisTitle_list, importance_list):
            if (cut_off > cut_off_val and cut_off <= cut_off_val + diff) or (cut_off < -cut_off_val and cut_off >= -cut_off_val - diff):
                string = rename_variables(string, rename_dim_meas)
                yAxis = rename_variables(yAxis, rename_dim_meas)
                title = rename_variables(title, rename_dim_meas)
                subtitle = rename_variables(subtitle, rename_dim_meas)
                tags = rename_variables(tags, rename_dim_meas)
                meas = rename_variables(meas, rename_dim_meas)
                dim = rename_variables(dim, rename_dim_meas)
                
                data = LineChart(df_actual, [meas],['Trend'], xAxis, yAxis, title, subtitle, chartFooterTitle, '', non_highlight_color = '#B0CBFF', highlight_color = '#3862FF')
                temp_count += 1
                # engine = azure_sql_database_connect(source_username, source_password, source_server, source_database)
                cnxn, cursor, logesys_engine = sql_connect()
                insert_insights(datamart_id, string, str(data), 'Slope', 'Line', str(related_fields), importance, tags, 'Trends', 'Insight', cnxn, cursor, insight_code, version_num)
        count = count + temp_count
        # count = 50, 90, 170
        print(f'count:{count}\n')
        if count >= 150:
            break