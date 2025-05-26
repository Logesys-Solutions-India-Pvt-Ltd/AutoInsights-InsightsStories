from DataOverview.main_kpi_filter import main_kpi_filter
from DataOverview.pie_and_bar_chart import data_overview_pie, data_overview_bar
from DataOverview.area import data_overview_area
from DataOverview.delta import data_overview_delta
from DataOverview.kpi import data_overview_kpi
import constants


def data_overview_call():
    constants.logger.info('Generating Data Overview')
    datamart_id = constants.DATAMART_ID
    source_type = constants.SOURCE_TYPE
    source_engine = constants.SOURCE_ENGINE
    date_columns = constants.DATE_COLUMNS
    dates_filter_dict = constants.DATES_FILTER_DICT
    outliers_dates = constants.OUTLIERS_DATES
    Significant_dimensions = constants.SIGNIFICANT_DIMENSIONS
    derived_measures_dict = constants.DERIVED_MEASURES_DICT
    derived_measures_dict_expanded = constants.DERIVED_MEASURES_DICT_EXPANDED
    dim_allowed_for_derived_metrics = constants.DIM_ALLOWED_FOR_DERIVED_METRICS
    rename_dim_meas = constants.RENAME_DIM_MEAS
    df_list = constants.DF_LIST
    df_list_ly = constants.DF_LIST_LY
    df_list_ty = constants.DF_LIST_TY
    df_relationship = constants.DF_RELATIONSHIP
    df_sql_table_names = constants.DF_SQL_TABLE_NAMES
    df_sql_meas_functions = constants.DF_SQL_MEAS_FUNCTIONS
    cnxn = constants.CNXN
    cursor = constants.CURSOR
    

    for meas in list(derived_measures_dict.keys()): 
        main_kpi_filter(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict_expanded, derived_measures_dict, df_sql_table_names, df_sql_meas_functions, df_list, df_relationship, rename_dim_meas, meas, cnxn, cursor)  

    for dim_table, dim_list in Significant_dimensions.items():
        for dim in dim_list:
            for meas in list(derived_measures_dict.keys()):  
                if dim in dim_allowed_for_derived_metrics[meas]:
                    data_overview_pie(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict_expanded, derived_measures_dict, df_sql_table_names, df_sql_meas_functions, df_list_ty, df_relationship, dim, meas, dim_table, cnxn, cursor)



    for dim_table, dim_list in Significant_dimensions.items():
        for dim in dim_list:
            for meas in list(derived_measures_dict.keys()):
                if dim in dim_allowed_for_derived_metrics[meas]:
                    data_overview_bar(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, dim, meas, dim_table, df_list_ty, df_relationship, cnxn, cursor)


    period_dim = ['Month-Day', 'Week', 'Month', 'Quarter']
    for pdim in period_dim:
        for meas in list(derived_measures_dict.keys()):
            data_overview_area(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict_expanded, derived_measures_dict, df_sql_table_names, df_sql_meas_functions, df_list_ly, df_list_ty, df_relationship, pdim, meas, cnxn, cursor)

    
    for dim_table, dim_list in Significant_dimensions.items():
        for dim in dim_list:
            for meas in list(derived_measures_dict.keys()):
                if dim in dim_allowed_for_derived_metrics[meas]:
                    data_overview_delta(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict_expanded, derived_measures_dict, df_sql_table_names, df_sql_meas_functions, df_list, df_list_ly, df_list_ty, dim, meas, dim_table, df_relationship, cnxn, cursor)


    for meas in list(derived_measures_dict.keys()):
        data_overview_kpi(source_type, source_engine, datamart_id, meas, date_columns, dates_filter_dict, outliers_dates, derived_measures_dict_expanded, derived_measures_dict, df_sql_table_names, df_sql_meas_functions, df_list, df_list_ly, df_list_ty, df_relationship, cnxn, cursor)