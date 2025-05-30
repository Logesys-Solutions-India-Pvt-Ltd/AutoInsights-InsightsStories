from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd
import numpy as np
import constants


def stories_rank_cy_ly(meas, importance):
    datamart_id = constants.DATAMART_ID
    source_type = constants.SOURCE_TYPE
    source_engine = constants.SOURCE_ENGINE
    date_columns = constants.DATE_COLUMNS
    dates_filter_dict = constants.DATES_FILTER_DICT
    Significant_dimensions = constants.SIGNIFICANT_DIMENSIONS
    rename_dim_meas = constants.RENAME_DIM_MEAS
    derived_measures_dict = constants.DERIVED_MEASURES_DICT
    derived_measures_dict_expanded = constants.DERIVED_MEASURES_DICT_EXPANDED
    df_list_ly = constants.DF_LIST_LY
    df_list_ty = constants.DF_LIST_TY
    df_relationship = constants.DF_RELATIONSHIP
    df_sql_table_names = constants.DF_SQL_TABLE_NAMES
    df_sql_meas_functions = constants.DF_SQL_MEAS_FUNCTIONS
    significance_score = constants.SIGNIFICANCE_SCORE
    df_version_number = constants.DF_VERSION_NUMBER
    dim_allowed_for_derived_metrics = constants.DIM_ALLOWED_FOR_DERIVED_METRICS
    cnxn = constants.CNXN
    cursor = constants.CURSOR
    logesys_engine = constants.LOGESYS_ENGINE
    
    
    row_index = 0
    is_ratio = False
    df_stories_rank = pd.DataFrame(columns=['String','Dimension','Value','LY Rank','TY Rank','Diff Act','Diff Abs','Diff Scaled','RelatedFields'])
    
    if source_type == 'xlsx':
        this_year_setting, last_year_setting = df_list_ty, df_list_ly
    elif source_type == 'table':
        this_year_setting, last_year_setting = 'ThisYear', 'LastYear'
        
    for dim_table, dim_list in Significant_dimensions.items():
        for dim in dim_list:  
            if (meas in ['ATV', 'UPT']) and (dim_table == 'Item_master_Insights'):
                continue
#             ty = ThisYear.groupby([dim])[meas].sum()
            ty = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio, is_total=False, is_others=False)
            ty = ty.sort_values(by=meas, ascending = False)
            ty[meas].fillna(0, inplace = True)
            ty.replace([np.inf, -np.inf], 0, inplace=True)
            ty_rank = ty.rank(ascending = False).astype(int) 

#             ly = LastYear.groupby([dim])[meas].sum()
            ly = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, last_year_setting, is_ratio, is_total=False, is_others=False)
            ly = ly.sort_values(by=meas, ascending = False)
            ly[meas].fillna(0, inplace = True)
            ly.replace([np.inf, -np.inf], 0, inplace=True)
            ly_rank = ly.rank(ascending = False).astype(int)
            
            df_merge = pd.merge(ty_rank, ly_rank, left_index=True, right_index=True)
            df_merge.columns = ['This Year', 'Last Year']
            df_filtered = df_merge.merge(significance_score[dim_table][dim][-5:], left_index = True, right_index = True)
            df_filtered.sort_values(by = 'rank', ascending = False, inplace = True)
            df_filtered['Diff'] = df_filtered['Last Year'] - df_filtered['This Year']
            data = df_filtered[abs(df_filtered['Diff']) >= df_filtered.shape[0]/3]
            
            related_fields, tags = '', ''
            for i in range(len(data)):
                if data['Last Year'][i] < data['This Year'][i]:
                    up_down = 'down'
                    increase_decrease = 'Decrease'
                else:
                    up_down = 'up'
                    increase_decrease = 'Increase'
                related_fields = dim + ' : ' + data.index[i] + ' | ' + 'measure' + ' : ' + meas + ' | ' + 'function' + ' : ' + 'Story Rank CY vs LY '  + increase_decrease
                string = f"{meas} contribution from {dim}: {data.index[i]}{b_tag_close} has moved {up_down} from rank {data['Last Year'][i]}{b_tag_close} Last Year to rank {data['This Year'][i]}{b_tag_close} This Year"
                diff = abs(data['Last Year'][i] - data['This Year'][i]) / df_filtered.shape[0]
                df_stories_rank.loc[row_index] = [string, dim, data.index[i], data['Last Year'][i], data['This Year'][i],(data['Last Year'][i] - data['This Year'][i]), abs(data['Last Year'][i] - data['This Year'][i]),diff,related_fields]
                row_index += 1
                
    df_stories_rank.sort_values(by = 'Diff Scaled', ascending = False, inplace = True)
    df_stories_rank.reset_index(drop=True, inplace = True)
    for i in df_stories_rank.index[0:2] if df_stories_rank.shape[0] > 2 else df_stories_rank.index:
        if df_stories_rank.loc[i]['Diff Act'] > 0:
            svg_type = 'SVG rank increase'
            st_data = ['Rank ' + str(df_stories_rank.loc[i]['LY Rank']) + ' last year', 'Rank ' + str(df_stories_rank.loc[i]['TY Rank']) + ' this year', df_stories_rank.loc[i]['Dimension'], meas + ' contribution']
        else:
            svg_type = 'SVG rank decrease'
            st_data = ['Rank ' + str(df_stories_rank.loc[i]['TY Rank']) + ' this year', 'Rank ' + str(df_stories_rank.loc[i]['LY Rank']) + ' last year', df_stories_rank.loc[i]['Dimension'], meas + ' contribution']
        string = df_stories_rank.loc[i]['String']
        story_data = StoryData(st_data)
        related_fields_list = df_stories_rank.loc[i]['RelatedFields']
        importance += 0.1
        version_num = 0
        insight_code = 'StoryRankCyLy#' + df_stories_rank.loc[i]['Dimension'] + '#' + df_stories_rank.loc[i]['Value'] + '#'+ meas
        string = rename_variables(string, rename_dim_meas)
        tags = rename_variables(tags, rename_dim_meas)
        story_data = rename_variables(story_data, rename_dim_meas)
        insert_insights(datamart_id, string, story_data, 'Rank CY vs LY', svg_type, related_fields_list, importance, tags, 'Story', 'story', cnxn, cursor, insight_code, version_num)
