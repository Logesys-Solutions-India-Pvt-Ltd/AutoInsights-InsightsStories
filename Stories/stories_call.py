from Stories.stories_avg_cy_ly import stories_avg_cy_ly
from Stories.stories_x_times import stories_x_times
from Stories.stories_rank_cy_ly import stories_rank_cy_ly


################################################## Stories Call ##########################################

def stories_call(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, Significant_dimensions, df_list_ly, df_list_ty, df_relationship, rename_dim_meas, significance_score, cnxn, cursor):
    print('Generating stories.')
    importance = 1
    for meas in list(derived_measures_dict.keys()):
    # for meas in ['Markdown %', 'ASP', 'ATV', 'UPT']: ## Timesquare ##
        importance += 1
        print('---------------')
        print('Stories - Average CY LY')
        stories_avg_cy_ly(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, Significant_dimensions, meas, df_list_ly, df_list_ty, df_relationship, rename_dim_meas, significance_score, importance, cnxn, cursor)

    importance = 1
    for meas in list(derived_measures_dict.keys()):
    # for meas in ['Markdown %', 'ASP', 'ATV', 'UPT']: ## Timesquare ##
        importance += 1
        print('---------------')
        print('Stories - X times')
        stories_x_times(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, Significant_dimensions, meas, df_list_ly, df_list_ty, df_relationship, rename_dim_meas, significance_score, importance, cnxn, cursor)


    importance = 1
    for meas in list(derived_measures_dict.keys()):
    # for meas in ['Markdown %', 'ASP', 'ATV', 'UPT']: ## Timesquare ##
        importance += 1
        print('---------------')
        print('Stories - Rank CY LY')
        stories_rank_cy_ly(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions,  Significant_dimensions, meas, df_list_ly, df_list_ty, df_relationship, rename_dim_meas, significance_score, importance, cnxn, cursor)