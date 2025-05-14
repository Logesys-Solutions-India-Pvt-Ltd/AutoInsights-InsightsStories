from DataOverview.main_kpi_filter import main_kpi_filter
from DataOverview.pie_and_bar_chart import data_overview_pie, data_overview_bar
from DataOverview.area import data_overview_area
from DataOverview.delta import data_overview_delta
from DataOverview.kpi import data_overview_kpi


def data_overview_call(source_type, source_engine, dim_allowed_for_derived_metrics, datamart_id, date_columns, dates_filter_dict, outliers_dates, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, Significant_dimensions, df_list, df_list_ly, df_list_ty, df_relationship, rename_dim_meas, significance_score, max_year, max_month, cnxn, cursor):
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