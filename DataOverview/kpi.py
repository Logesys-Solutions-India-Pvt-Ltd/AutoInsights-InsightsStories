from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd


def data_overview_kpi(source_type, source_engine, datamart_id, meas, date_columns, dates_filter_dict, outliers_dates, derived_measures_dict_expanded, derived_measures_dict, df_sql_table_names, df_sql_meas_functions, df_list, df_list_ly, df_list_ty, df_relationship, cnxn, cursor):
    is_ratio = False
    
    if '/' in derived_measures_dict[meas]['Formula']:
        is_ratio = True
        
    if source_type == 'xlsx':
        this_year_setting, last_year_setting = df_list_ty, df_list_ly
    elif source_type == 'table':
        this_year_setting, last_year_setting = 'ThisYear', 'LastYear'
    
#     ty_ytd =  ThisYear[meas].sum()
    ty_ytd = parent_get_group_data(source_type, source_engine, '', meas, date_columns, dates_filter_dict, '', derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio, is_total=True, is_others=False, outliers_dates=outliers_dates)
#     ly_ytd =  LastYear[meas].sum()
    ly_ytd = parent_get_group_data(source_type, source_engine, '', meas, date_columns, dates_filter_dict, '', derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, last_year_setting, is_ratio, is_total=True, is_others=False, outliers_dates=outliers_dates)
    if (ly_ytd != 0):
        ytd_gr = round(((ty_ytd - ly_ytd) / ly_ytd)*100,2)
        primary_kpi_title = 'YTD Growth ' + meas
        primary_kpi_value = str(ytd_gr) + '%'            
        sub_kpi_value = str(human_format(round(ty_ytd - ly_ytd,1)))            
        seconday_kpi_1_title = 'This Year'
        seconday_kpi_1_value = human_format(ty_ytd)
        seconday_kpi_2_title = 'Last Year'
        seconday_kpi_2_value = human_format(ly_ytd)


        dd_string = ''
        deepdive_id = uuid.uuid1()
        category = ""
        section_id = 2.1

        if(ytd_gr >= 0):
            primary_kpi_color = '#00D506'
            sub_kpi_color = '#00D506'
            sub_kpi_icon = '▲'
        else:
            primary_kpi_color = '#FF0000'
            sub_kpi_color = '#FF0000'
            sub_kpi_icon = '▼'

        kpi = '{"data":{"Primary_kpi" : {"title" : "' + str(primary_kpi_title) + '","value" : "' + str(primary_kpi_value) + '","color" : "' + str(primary_kpi_color) + '"},"Sub_kpi" : {"Icon" : "' + str(sub_kpi_icon) + '","value" : "' + str(sub_kpi_value) + '","color" : "' + str(sub_kpi_color) + '"},"Secondary_kpi" : [{"title" : "' + str(seconday_kpi_1_title) + '","value" : "' + str(seconday_kpi_1_value) + '"},{"title" : "' + str(seconday_kpi_2_title) + '","value" : "' + str(seconday_kpi_2_value) + '"}]}}'
        # engine = azure_sql_database_connect(source_username, source_password, source_server, source_database)
        cnxn, cursor, logesys_engine = sql_connect()
        insert_summary(datamart_id, kpi, 'DataOverview_KPI_YTD', 'KPI', section_id, '', meas, cnxn, cursor)
    else:
        print('YTD growth : NA')
    
    ################################################## MTD ####################################################
    if source_type == 'xlsx':
        this_period_setting, last_period_setting = df_list_ty.copy(), df_list_ly.copy()
        modified_val_this_period, modified_val_last_period = 'ThisPeriodMTD', 'LastPeriodMTD'
    elif source_type == 'table':
        this_period_setting, last_period_setting = 'ThisPeriodMTD', 'LastPeriodMTD'
        modified_val_this_period, modified_val_last_period = 'MTD', 'MTD'
        
#     ty_mtd = ThisYear[(ThisYear['Month'] == max_month)][meas].sum()
#            parent_get_group_data(sourcetype, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_period_setting, is_ratio, is_total=False, is_others=False, outliers_val=modified_val_this_period, outliers_dates=outliers_dates)

    ty_mtd = parent_get_group_data(source_type, source_engine, '', meas, date_columns, dates_filter_dict, '', derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_period_setting, is_ratio, is_total=True, is_others=False, outliers_val=modified_val_this_period, outliers_dates=outliers_dates)
#     ly_mtd = LastYear[(LastYear['Month'] == max_month)][meas].sum()
    ly_mtd = parent_get_group_data(source_type, source_engine, '', meas, date_columns, dates_filter_dict, '', derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, last_period_setting, is_ratio, is_total=True, is_others=False, outliers_val=modified_val_last_period, outliers_dates=outliers_dates)
    
    mtd_gr = round((ty_mtd - ly_mtd) * 100/ly_mtd,1)
    if (ly_mtd != 0):
        dd_string = ''
        deepdive_id = uuid.uuid1()
        category = ""

        primary_kpi_title = 'MTD Growth ' + meas
        primary_kpi_value = str(mtd_gr) + '%'            
        sub_kpi_value = str(human_format(round(ty_mtd - ly_mtd,1)))            
        seconday_kpi_1_title = 'This Year'
        seconday_kpi_1_value = human_format(ty_mtd)
        seconday_kpi_2_title = 'Last Year'
        seconday_kpi_2_value = human_format(ly_mtd)
        if(mtd_gr >= 0):
            primary_kpi_color = '#00D506'
            sub_kpi_color = '#00D506'
            sub_kpi_icon = '▲'
        else:
            primary_kpi_color = '#FF0000'
            sub_kpi_color = '#FF0000'
            sub_kpi_icon = '▼'    

        kpi = '{"data":{"Primary_kpi" : {"title" : "' + str(primary_kpi_title) + '","value" : "' + str(primary_kpi_value) + '","color" : "' + str(primary_kpi_color) + '"},"Sub_kpi" : {"Icon" : "' + str(sub_kpi_icon) + '","value" : "' + str(sub_kpi_value) + '","color" : "' + str(sub_kpi_color) + '"},"Secondary_kpi" : [{"title" : "' + str(seconday_kpi_1_title) + '","value" : "' + str(seconday_kpi_1_value) + '"},{"title" : "' + str(seconday_kpi_2_title) + '","value" : "' + str(seconday_kpi_2_value) + '"}]}}'  
        section_id = 2.2
        # engine = azure_sql_database_connect(source_username, source_password, source_server, source_database)
        cnxn, cursor, logesys_engine = sql_connect()
        insert_summary(datamart_id, kpi, 'DataOverview_KPI_MTD', 'KPI', section_id, '', meas, cnxn, cursor)
    else:
        print('MTD growth : NA')
        
    ################################################## Rolling 3 Months ##################################################
    if source_type == 'xlsx':
        this_period_setting, last_period_setting = df_list_ty.copy(), df_list_ly.copy()
        modified_val_this_period, modified_val_last_period = 'ThisPeriodR3M', 'LastPeriodR3M'
    elif source_type == 'table':
        this_period_setting, last_period_setting = 'ThisPeriodR3M', 'LastPeriodR3M'
        modified_val_this_period, modified_val_last_period = 'Rolling 3 Months', 'Rolling 3 Months'


    ty_3_month = parent_get_group_data(source_type, source_engine, '', meas, date_columns, dates_filter_dict, '', derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_period_setting, is_ratio, is_total=True, is_others=False, outliers_val=modified_val_this_period, outliers_dates=outliers_dates)
    ly_3_month = parent_get_group_data(source_type, source_engine, '', meas, date_columns, dates_filter_dict, '', derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, last_period_setting, is_ratio, is_total=True, is_others=False, outliers_val=modified_val_last_period, outliers_dates=outliers_dates)    
    
    month_3_gr = round((ty_3_month - ly_3_month) * 100/ly_3_month,1)

    if (ly_3_month != 0):
        dd_string = ''
        deepdive_id = uuid.uuid1()
        category = ""

        primary_kpi_title = 'Rolling 3 Growth ' + meas
        primary_kpi_value = str(month_3_gr) + '%'            
        sub_kpi_value = str(human_format(round(ty_3_month - ly_3_month,1)))            
        seconday_kpi_1_title = 'This Period'
        seconday_kpi_1_value = human_format(ty_3_month)
        seconday_kpi_2_title = 'Last Period'
        seconday_kpi_2_value = human_format(ly_3_month)
        if(month_3_gr >= 0):
            primary_kpi_color = '#00D506'
            sub_kpi_color = '#00D506'
            sub_kpi_icon = '▲'
        else:
            primary_kpi_color = '#FF0000'
            sub_kpi_color = '#FF0000'
            sub_kpi_icon = '▼'
        kpi = '{"data":{"Primary_kpi" : {"title" : "' + str(primary_kpi_title) + '","value" : "' + str(primary_kpi_value) + '","color" : "' + str(primary_kpi_color) + '"},"Sub_kpi" : {"Icon" : "' + str(sub_kpi_icon) + '","value" : "' + str(sub_kpi_value) + '","color" : "' + str(sub_kpi_color) + '"},"Secondary_kpi" : [{"title" : "' + str(seconday_kpi_1_title) + '","value" : "' + str(seconday_kpi_1_value) + '"},{"title" : "' + str(seconday_kpi_2_title) + '","value" : "' + str(seconday_kpi_2_value) + '"}]}}'  
        section_id = 2.3
        # engine = azure_sql_database_connect(source_username, source_password, source_server, source_database)
        cnxn, cursor, logesys_engine = sql_connect()
        insert_summary(datamart_id, kpi, 'DataOverview_KPI_3Month', 'KPI', section_id, '', meas, cnxn, cursor)
    else:
        print('3 month growth : NA')