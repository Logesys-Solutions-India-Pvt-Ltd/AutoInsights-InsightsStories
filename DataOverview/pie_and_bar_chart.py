from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd


def data_overview_pie(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict_expanded, derived_measures_dict, df_sql_table_names, df_sql_meas_functions, df_list_ty, df_relationship, dim, meas, dim_table, cnxn, cursor):
    is_ratio = False
    if source_type == 'xlsx':
        this_year_setting = df_list_ty
    elif source_type == 'table':
        this_year_setting = 'ThisYear'
    # pie = ThisYear.groupby(dim)[meas].sum().to_frame()

    pie = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio, is_total=False, is_others=False) 
    
    if pie[pie[meas]<0].shape[0] == 0:  
        data = pie.to_json()
        section_id = 3
        # engine = azure_sql_database_connect(source_username, source_password, source_server, source_database)
        cnxn, cursor, logesys_engine = sql_connect()
        insert_summary(datamart_id, data, 'data_overview_pie', 'Donut', section_id, dim, meas, cnxn, cursor)
    else:
        data_overview_bar(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, dim, meas, dim_table, df_list_ty, df_relationship, cnxn, cursor)



def data_overview_bar(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, dim, meas, dim_table, df_list_ty, df_relationship, cnxn, cursor):
    split = 10
    is_ratio = False
    df_others_value = pd.DataFrame()
    
    if source_type == 'xlsx':
        this_year_setting = df_list_ty
    elif source_type == 'table':
        this_year_setting = 'ThisYear'
    
#     df_bar = ThisYear.groupby(dim)[meas].sum().to_frame()   
    df_bar = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio, is_total=False, is_others=False) 
    df_bar.sort_values(by = meas, ascending = False, inplace = True)

    df_bar, others_count_ty, others_value_ty = df_others(source_type, source_engine, df_bar, split, this_year_setting, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, is_ratio, is_total=False)    
    df_bar = df_bar.head(split)
    
    if not (others_count_ty == 0):
        df_others_value = pd.DataFrame({meas: [others_value_ty]}, index=[f"{others_count_ty} others"])
        df_others_value.index.name = dim
    df_bar = pd.concat([df_bar, df_others_value]) 
        
    df_bar = chart_index_styling(df_bar, list(df_bar.index), meas, average = 'empty', avg_color = '#FF0000', def_color = '#FEE2B5', highlight_color = '#FF9E00')

    xAxisTitle = dim
    yAxisTitle = meas
    chart_title = meas + ' by ' + dim
    chartSubTitle = ''
    chartFooterTitle = ''

    df_data = BarChart(df_bar, [meas], xAxisTitle, yAxisTitle, chart_title, chartSubTitle, chartFooterTitle)
    df_data = df_data.replace('#FEE2B5','#FF9E00')
    section_id = 5
    
    # engine = azure_sql_database_connect(source_username, source_password, source_server, source_database)
    cnxn, cursor, logesys_engine = sql_connect()
    insert_summary(datamart_id, df_data, 'data_overview_bar', 'Combo', section_id, dim, meas, cnxn, cursor)