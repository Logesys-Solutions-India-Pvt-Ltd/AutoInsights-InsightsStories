from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd


def main_kpi_filter(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict_expanded, derived_measures_dict, df_sql_table_names, df_sql_meas_functions, df_list, df_relationship, rename_dim_meas, meas, cnxn, cursor):
    is_ratio = False
    
    if source_type == 'xlsx':
        all_years_setting = df_list
    elif source_type == 'table':
        all_years_setting = 'AllYears'
        
    if '/' in derived_measures_dict[meas]['Formula']:
        is_ratio = True
#     val = df[meas].sum()
#   ty = parent_get_group_data(sourcetype, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio, is_total=False, is_others=False)

    val = parent_get_group_data(source_type, source_engine, '', meas, date_columns, dates_filter_dict, '', derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, all_years_setting, is_ratio, is_total=True, is_others=False)
    section_id = 1
    primary_kpi_title = 'Total ' + meas   
    ty_ytd = human_format(val)
    
    primary_kpi_color = '#00D506'
    kpi = '{"data":{"Primary_kpi" : {"title" : "' + str(primary_kpi_title) + '","value" : "' + str(ty_ytd) + '","color" : "' + str(primary_kpi_color) + '"}}}'
    
    kpi = rename_variables(kpi, rename_dim_meas)
    primary_kpi_title = rename_variables(primary_kpi_title, rename_dim_meas)
    
    # engine = azure_sql_database_connect(source_username, source_password, source_server, source_database)
    cnxn, cursor, logesys_engine = sql_connect()
    # insert_summary(datamart_id, kpi, 'DataOverview_KPI_YTD', 'KPI', section_id, '', meas, cnxn, cursor)