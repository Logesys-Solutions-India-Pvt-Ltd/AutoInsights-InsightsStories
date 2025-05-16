from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd


def data_overview_delta(source_type, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict_expanded, derived_measures_dict, df_sql_table_names, df_sql_meas_functions, df_list, df_list_ly, df_list_ty, dim, meas, dim_table, df_relationship, cnxn, cursor):
    split = 10
    is_ratio = False
    DiffValPosOthers, DiffValNegOthers = pd.DataFrame(), pd.DataFrame()

    if source_type == 'xlsx':
        this_year_setting, last_year_setting, all_years_setting = df_list_ty, df_list_ly, df_list
    elif source_type == 'table':
        this_year_setting, last_year_setting, all_years_setting = 'ThisYear', 'LastYear', 'AllYears'

    ThisYearDimVal = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio, is_total=False, is_others=False)
    ThisYearDimVal.replace([np.inf, -np.inf], 0, inplace=True)
    
    LastYearDimVal = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, last_year_setting, is_ratio, is_total=False, is_others=False)
    LastYearDimVal.replace([np.inf, -np.inf], 0, inplace=True)

    df_merge = pd.merge(ThisYearDimVal, LastYearDimVal, left_index = True, right_index = True, how = 'outer', suffixes = ['_ty', '_ly'])
    df_merge.fillna(0, inplace = True)
    df_merge.replace([np.inf, -np.inf], 0, inplace=True)
    
    DiffVal = df_merge[meas + '_ty'] - df_merge[meas + '_ly']
    DiffVal = DiffVal.to_frame()
    DiffVal.columns = [meas]
    DiffVal.sort_values(by=meas,ascending=False,inplace=True)
    
    DiffVal = DiffVal.fillna(0)
    DiffVal.replace([np.inf, -np.inf], 0, inplace=True)
    
    if DiffVal.shape[0] > 10:
        DiffValPos = DiffVal[DiffVal[meas]>0]
        DiffValNeg = DiffVal[DiffVal[meas]<0]
        
        # Only calculate others if rows > split+2 for positive values
        if DiffValPos.shape[0] > split+2:
            DiffValPos, others_count_pos, others_value_pos = df_others(source_type, source_engine, DiffValPos, split, all_years_setting, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, is_ratio=False, is_total=False)
            DiffValPos = DiffValPos.head(split)
            
            if not (others_count_pos == 0):
                DiffValPosOthers = pd.DataFrame({meas: [others_value_pos]}, index=[f"{others_count_pos} others"])
                DiffValPosOthers.index.name = dim
                DiffValPos = pd.concat([DiffValPos, DiffValPosOthers])
        else:
            # Keep all positive rows without creating "Others" row
            pass
            
        
        # Only calculate others if rows > split+2 for negative values
        if DiffValNeg.shape[0] > split+2:
            DiffValNeg, others_count_neg, others_value_neg = df_others(source_type, source_engine, DiffValNeg, split, all_years_setting, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, is_ratio=False, is_total=False)
            DiffValNeg = DiffValNeg.head(split)
            
            if not (others_count_neg == 0):
                DiffValNegOthers = pd.DataFrame({meas: [others_value_neg]}, index=[f"{others_count_neg} others"])
                DiffValNegOthers.index.name = dim
                DiffValNeg = pd.concat([DiffValNeg, DiffValNegOthers])
        else:
            # Keep all negative rows without creating "Others" row
            pass
            
        DiffVal = pd.concat([DiffValPos, DiffValNeg])
    xAxisTitle = dim
    yAxisTitle = meas
    chart_title = 'YTD ' + meas + ' growth by ' + dim
    
    chartSubTitle = 'Overall delta is ' + human_format(ThisYearDimVal[meas].sum() - LastYearDimVal[meas].sum())
    chartFooterTitle = ''

    waterfall = waterfallChart(dim, meas, DiffVal, xAxisTitle, yAxisTitle, chart_title, chartSubTitle, chartFooterTitle)
    section_id = 6
    # engine = azure_sql_database_connect(source_username, source_password, source_server, source_database)
    cnxn, cursor, logesys_engine = sql_connect()
    insert_summary(datamart_id, waterfall, 'Waterfall', 'Waterfall', section_id, dim, meas, cnxn, cursor)