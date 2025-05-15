from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd


def data_overview_area(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict_expanded, derived_measures_dict, df_sql_table_names, df_sql_meas_functions, df_list_ly, df_list_ty, df_relationship, pdim, meas, cnxn, cursor):
    is_ratio = False
    if source_type == 'xlsx':
        this_year_setting, last_year_setting = df_list_ty, df_list_ly
    elif source_type == 'table':
        this_year_setting, last_year_setting = 'ThisYear', 'LastYear'
    
#     currsales = ThisYear.groupby(pdim)[meas].sum().to_frame()
    currsales = parent_get_group_data(source_type, source_engine, pdim, meas, date_columns, dates_filter_dict, '', derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio, is_total=False, is_others=False) 
    currsales.rename(columns = {meas:'This Year'}, inplace=True)
    
#     prevsales = LastYear.groupby(pdim)[meas].sum().to_frame()  
    prevsales = parent_get_group_data(source_type, source_engine, pdim, meas, date_columns, dates_filter_dict, '', derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship,last_year_setting, is_ratio, is_total=False, is_others=False)
    prevsales.rename(columns = {meas:'Last Year'}, inplace=True)
    data = pd.merge(currsales,prevsales,left_index=True,right_index=True, how = 'outer').fillna(0)
    
    
    if pdim in ['Month'] and source_type == 'table':
        month_to_num = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4,
            'May': 5, 'June': 6, 'July': 7, 'August': 8,
            'September': 9, 'October': 10, 'November': 11, 'December': 12
        }

        data.index = data.index.map(month_to_num)
        data = data.sort_index()
        
    if pdim in ['Month', 'Quarter']:
        d = {1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr',5:'May', 6:'Jun', 7:'Jul', 8:'Aug',9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec', 'Q1':'Qtr 1', 'Q2':'Qtr 2','Q3':'Qtr 3','Q4':'Qtr 4'}
    else:
        d = ''
        data = data.reset_index()
        data[pdim] = data[pdim].astype(str)
        ## SORT
        if pdim == 'Month-Day':
            data[['Month', 'Day']] = data[pdim].str.split('-', expand=True).astype(int)
            data.sort_values(by=['Month', 'Day'], inplace=True)
            data.drop(columns=['Month', 'Day'], inplace=True)
        ## SORT
        data = data.set_index(pdim)
    
    xAxisTitle = ''
    yAxisTitle = meas
    chart_title = pdim + ' wise ' + meas
    chartSubTitle = ''
    chartFooterTitle = ''
    
    if data.shape[0] == 1:
        highlight_columns = ['This Year', 'Last Year']
        non_highlight_columns = []
    else:
        highlight_columns = ['This Year']
        non_highlight_columns = ['Last Year']
    data = LineChart(data, highlight_columns, non_highlight_columns, xAxisTitle, yAxisTitle, chart_title, chartSubTitle, chartFooterTitle,mapping=d)
    section_id = 4
    chart_title = meas + ' by ' + pdim
    # engine = azure_sql_database_connect(source_username, source_password, source_server, source_database)
    cnxn, cursor, logesys_engine = sql_connect()
    # insert_summary(datamart_id, data, 'data_overview_area', 'Area', section_id, pdim, meas, cnxn, cursor)
    