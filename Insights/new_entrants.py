from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd
import numpy as np
import constants


def new_entrants(dim_table, dim, meas):
    print('--NEW ENTRANTS--')
    
    datamart_id = constants.DATAMART_ID
    source_type = constants.SOURCE_TYPE
    source_engine = constants.SOURCE_ENGINE
    date_columns = constants.DATE_COLUMNS
    dates_filter_dict = constants.DATES_FILTER_DICT
    max_date = constants.MAX_DATE
    max_month = constants.MAX_MONTH
    rename_dim_meas = constants.RENAME_DIM_MEAS
    derived_measures_dict = constants.DERIVED_MEASURES_DICT
    derived_measures_dict_expanded = constants.DERIVED_MEASURES_DICT_EXPANDED
    df_list_last12months = constants.DF_LIST_LAST12MONTHS
    df_relationship = constants.DF_RELATIONSHIP
    df_sql_table_names = constants.DF_SQL_TABLE_NAMES
    df_sql_meas_functions = constants.DF_SQL_MEAS_FUNCTIONS
    significance_score = constants.SIGNIFICANCE_SCORE
    df_version_number = constants.DF_VERSION_NUMBER
    cnxn = constants.CNXN
    cursor = constants.CURSOR
    logesys_engine = constants.LOGESYS_ENGINE

    start_of_last_12_months, start_of_month = calculate_month_dates(max_date)
    start_of_last_12_months = start_of_last_12_months.strftime("%d-%m-%Y")
    start_of_month = start_of_month.strftime("%d-%m-%Y")
    is_ratio=False
    
    if source_type == 'xlsx':
        all_years_setting = df_list_last12months
    elif source_type == 'table':
        all_years_setting = 'Last12Months'

    df_groupby_month_dim = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, all_years_setting, is_ratio, is_total=False, is_others=False, extra_groupby_col='Year-Month') 
    df_groupby_month_dim['Year-Month'] = pd.Categorical(df_groupby_month_dim['Year-Month'], 
                                                      categories=pd.date_range(start_of_last_12_months, max_date, freq='M').strftime('%Y-%b'),
                                                      ordered=True)
    df_groupby_month_dim = df_groupby_month_dim.sort_values('Year-Month')
    df_groupby_month_dim.replace([np.inf, -np.inf], 0, inplace=True)
    df_groupby_month_dim = df_groupby_month_dim[df_groupby_month_dim[meas] != 0]
    
    unique_values = df_groupby_month_dim[dim].unique()
    df_empty_all_months = pd.DataFrame(list(df_groupby_month_dim['Year-Month'].unique()), columns = ['Year-Month'])
    
    for dim_value in unique_values:
        df_new_entrant = pd.merge(df_empty_all_months, df_groupby_month_dim[df_groupby_month_dim[dim] == dim_value].drop(dim, axis = 1), how = 'left').fillna(0)
        if df_new_entrant.iloc[-4:-1][meas].sum() == 0 and df_new_entrant.iloc[-1][meas] != 0:
            first_non_zero_row = df_new_entrant.iloc[-1]
            df_new_entrant.set_index('Year-Month', inplace = True)
            if df_new_entrant.iloc[:-1][meas].sum() == 0:
                new_entrant_text = 'a new entrant '
            elif df_new_entrant.iloc[:-1][meas].sum() != 0:
                new_entrant_text = 're-emerged '
            xAxisTitle = ''
            yAxisTitle = meas
            chart_title = 'Monthly trend of ' + meas
            chartSubTitle = dim  + ': ' + dim_value
            chartFooterTitle = 'Showing last 12 months'
            
            insight_code = 'New Entrants#' + dim + '#' + dim_value + '#' + meas
            
            version_num = 0
            df_version_num_filtered = df_version_number[df_version_number['InsightCode'] == insight_code] 
            tags = ''
            if df_version_num_filtered.empty:
                version_num = 0
                importance = 99
            else:
                version_num = df_version_num_filtered['VersionNumber'].iloc[0]
                version_num += 1
                importance = df_version_num_filtered['Importance'].iloc[0] + 10
                chartFooterTitle = chartFooterTitle + ' (Version: '+str(version_num) + ')'
                tags = dim + '|' + dim_value + '|' + meas + '|' + 'New Entrant' + '|' + str(first_non_zero_row['Year-Month']) + '|' + 'Month' + '|' + 'Version: ' + str(version_num)

            xAxisTitle = rename_variables(xAxisTitle, rename_dim_meas)
            yAxisTitle = rename_variables(yAxisTitle, rename_dim_meas)
            chart_title = rename_variables(chart_title, rename_dim_meas)
            chartSubTitle = rename_variables(chartSubTitle, rename_dim_meas)
            chartFooterTitle = rename_variables(chartFooterTitle, rename_dim_meas)
            tags = rename_variables(tags, rename_dim_meas)
            
            data = LineChart(df_new_entrant,[meas],[], xAxisTitle, yAxisTitle, chart_title, chartSubTitle, chartFooterTitle, '', non_highlight_color = '#B0CBFF', highlight_color = '#3862FF')
            related_fields = dim  + ' : ' + dim_value + ' | ' + 'measure : ' + meas + ' | ' + 'function : New Entrant'
            string = b_tag_open + dim + ' : ' + dim_value + b_tag_close + ' is ' + new_entrant_text + ' from the Month ' + b_tag_open + str(first_non_zero_row['Year-Month']) + b_tag_close
            string = rename_variables(string, rename_dim_meas)

            cnxn, cursor, logesys_engine = sql_connect()
            insert_insights(datamart_id, string, str(data), 'Slope', 'Line', str(related_fields), importance, tags, 'New Entrants', 'Insight', cnxn, cursor, insight_code, version_num)
