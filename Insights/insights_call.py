from Insights.hi_pots import hi_pots
from Insights.movements import movements
from Insights.delta_analysis import delta_analysis
from Insights.rank_analysis import rank_analysis
from Insights.new_entrants import new_entrants
from Insights.trends import trends
from Insights.monthly_anomalies import monthly_anomalies
from Insights.weekly_anomalies import weekly_anomalies
from Insights.outliers import outliers


def insights_call(datamart_id, source_type, source_engine, selected_insights, insights_to_skip, dim_allowed_for_derived_metrics, Significant_dimensions, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, rename_dim_meas, date_columns, dates_filter_dict, outliers_dates, df_list, df_list_ly, df_list_ty, df_list_last12months, df_list_last52weeks, max_month, max_year, max_date, significance_score, df_version_number, cnxn, cursor):
    for dim_table, dim_list in Significant_dimensions.items():
            for dim in dim_list:
                for meas in list(derived_measures_dict.keys()):
                    if dim in dim_allowed_for_derived_metrics[meas]:
                        for insight_name in selected_insights:
                            if insight_name in insights_to_skip:
                                continue

                            if insight_name == 'Hi-Pots':
                                hi_pots(datamart_id, source_type, source_engine, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, rename_dim_meas, dim, meas, date_columns, dates_filter_dict, df_list_ty, dim_table, df_version_number, cnxn, cursor)
                            if meas != 'Stock Cover':
                                if insight_name == 'Movements':     
                                    movements(datamart_id, source_type, source_engine, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, rename_dim_meas, dim, meas, date_columns, dates_filter_dict, df_list_ly, df_list_ty, dim_table, df_version_number, significance_score, cnxn, cursor)
                                elif insight_name == 'Rank Analysis':
                                    rank_analysis(datamart_id, source_type, source_engine, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, rename_dim_meas, df_list_ty, df_list_ly, dim_table, dim, meas, date_columns, dates_filter_dict, df_version_number, cnxn, cursor)
                                elif insight_name == 'Delta Analysis':
                                    delta_analysis(datamart_id, source_type, source_engine, dim, meas, date_columns, dates_filter_dict, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, rename_dim_meas, df_list, df_list_ty, df_list_ly, dim_table, df_version_number, cnxn, cursor)
                                elif insight_name == 'New Entrants':
                                    new_entrants(datamart_id, source_type, source_engine, dim, meas, dim_table, date_columns, dates_filter_dict, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_list_last12months, df_relationship, rename_dim_meas, significance_score, max_month, max_date, df_version_number, cnxn, cursor)

    for insight_name in selected_insights:
        if insight_name == 'Trends':
            trends(datamart_id, source_type, source_engine, dim_allowed_for_derived_metrics, date_columns, dates_filter_dict, Significant_dimensions, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_list_last12months, df_relationship, rename_dim_meas, significance_score, max_month, max_date, df_version_number, cnxn, cursor)
        elif insight_name == 'Monthly Anomalies':
            monthly_anomalies(datamart_id, source_type, source_engine, dim_allowed_for_derived_metrics, date_columns, dates_filter_dict, Significant_dimensions, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_list_last12months, df_relationship, rename_dim_meas, significance_score, max_month, max_date, df_version_number, cnxn, cursor)
        elif insight_name == 'Weekly Anomalies':
            weekly_anomalies(datamart_id, source_type, source_engine, dim_allowed_for_derived_metrics, date_columns, dates_filter_dict, Significant_dimensions, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_list_last52weeks, df_relationship, rename_dim_meas, significance_score, max_month, max_date, df_version_number, cnxn, cursor)
        elif insight_name == 'Outliers':
            outliers(datamart_id, source_type, source_engine, dim_allowed_for_derived_metrics, date_columns, dates_filter_dict, Significant_dimensions, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, df_list_ly, df_list_ty, rename_dim_meas, significance_score, max_year, max_month, outliers_dates, df_version_number, cnxn, cursor)