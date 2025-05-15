from Insights.hi_pots import hi_pots
from Insights.movements import movements
from Insights.delta_analysis import delta_analysis
from Insights.rank_analysis import rank_analysis
from Insights.new_entrants import new_entrants
from Insights.trends import trends
from Insights.monthly_anomalies import monthly_anomalies
from Insights.weekly_anomalies import weekly_anomalies
from Insights.outliers import outliers
import constants


def insights_call():
    source_type = constants.SOURCE_TYPE
    source_engine = constants.SOURCE_ENGINE
    datamart_id = constants.DATAMART_ID
    selected_insights = constants.SELECTED_INSIGHTS
    insights_to_skip = constants.INSIGHTS_TO_SKIP
    dim_allowed_for_derived_metrics = constants.DIM_ALLOWED_FOR_DERIVED_METRICS
    date_columns = constants.DATE_COLUMNS
    dates_filter_dict = constants.DATES_FILTER_DICT
    outliers_dates = constants.OUTLIERS_DATES
    max_date = constants.MAX_DATE
    max_month = constants.MAX_MONTH
    max_year = constants.MAX_YEAR
    derived_measures_dict = constants.DERIVED_MEASURES_DICT
    derived_measures_dict_expanded = constants.DERIVED_MEASURES_DICT_EXPANDED
    df_sql_table_names = constants.DF_SQL_TABLE_NAMES
    df_sql_meas_functions = constants.DF_SQL_MEAS_FUNCTIONS
    Significant_dimensions = constants.SIGNIFICANT_DIMENSIONS
    df_list = constants.DF_LIST
    df_list_ly = constants.DF_LIST_LY
    df_list_ty = constants.DF_LIST_TY
    df_list_last12months = constants.DF_LIST_LAST12MONTHS
    df_list_last52weeks = constants.DF_LIST_LAST52WEEKS
    df_relationship = constants.DF_RELATIONSHIP
    rename_dim_meas = constants.RENAME_DIM_MEAS
    significance_score = constants.SIGNIFICANCE_SCORE
    df_version_number = constants.DF_VERSION_NUMBER
    cnxn = constants.CNXN
    cursor = constants.cursor
    
    
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