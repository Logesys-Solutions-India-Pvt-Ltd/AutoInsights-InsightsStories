from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd
import numpy as np
import constants


def stories_avg_cy_ly(meas, importance):    
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
    tags = ''
    
    df_stories_avg_cy_ly = pd.DataFrame(columns=['String','Dimension','LY Value','TY Value','Diff %','Diff Abs','Value'])
    
    if source_type == 'xlsx':
        this_year_setting, last_year_setting = df_list_ty, df_list_ly
    elif source_type == 'table':
        this_year_setting, last_year_setting = 'ThisYear', 'LastYear'
        
    for dim_table, dim_list in Significant_dimensions.items():
        for dim in dim_list:
            if (meas in ['ATV', 'UPT']) and (dim_table == 'Item_master_Insights'):
                continue
            is_ratio = False
        #    ty = round(ThisYear.groupby([dim])[meas].sum(),2).to_frame()
            ty = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio, is_total=False, is_others=False)
            ty = ty[ty[meas] != 0]
            ty = round(ty[meas], 2)

        #    ly = round(LastYear.groupby([dim])[meas].sum(),2).to_frame()
            ly = parent_get_group_data(source_type, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, last_year_setting, is_ratio, is_total=False, is_others=False)
            ly = ly[ly[meas] != 0]
            ly = round(ly[meas], 2)

            df_merge = pd.merge(ty, ly, left_index = True, right_index = True)
            df_merge.columns = ['This Year', 'Last Year']
            df_merge['Diff'] = round(((df_merge['This Year'] - df_merge['Last Year']) * 100)/ df_merge['Last Year'],2)

            df_filtered = df_merge.merge(significance_score[dim_table][dim][-5:], left_index = True, right_index = True)
            df_filtered = df_filtered.sort_values(by = 'Diff', ascending = True)
            df_filtered['abs'] = abs(df_filtered['Diff'])
            df_filtered = df_filtered[df_filtered['abs'] > 30].sort_values(by = 'abs', ascending = False)

            ## Percentage and Units
            is_percentage, meas_units = '', ''
            show_in_percentage_query = f"""
                                SELECT ShowInPercentage, Units 
                                FROM derived_metrics
                                WHERE DatamartId = '{datamart_id}'
                                AND MetricName = '{meas}'
                                """
            
            show_in_percentage_result = query_on_table(show_in_percentage_query, logesys_engine)

            if not show_in_percentage_result.empty:
                show_in_percentage = show_in_percentage_result['ShowInPercentage'][0]
                if show_in_percentage:
                    is_percentage = '%'
                
                meas_units = show_in_percentage_result['Units'][0]

            for i in range(len(df_filtered)):
                if df_filtered.iloc[i]['Diff'] > 0:
                    string = f"YTD {meas} in {dim}: {df_filtered.index[i]}{b_tag_close} has increased {df_filtered.iloc[i]['Diff']}% ({human_format(round(df_filtered.iloc[i]['This Year'] - df_filtered.iloc[i]['Last Year'], 1))}), from {human_format(df_filtered.iloc[i]['Last Year'])}{meas_units}{is_percentage}{b_tag_close} last year to {human_format(df_filtered.iloc[i]['This Year'])}{meas_units}{is_percentage}{b_tag_close} this year"
                else:
                    string = f"YTD {meas} in {dim}: {df_filtered.index[i]}{b_tag_close} has decreased {df_filtered.iloc[i]['Diff']}% ({human_format(round(df_filtered.iloc[i]['This Year'] - df_filtered.iloc[i]['Last Year'], 1))}), from {human_format(df_filtered.iloc[i]['Last Year'])}{meas_units}{is_percentage}{b_tag_close} last year to {human_format(df_filtered.iloc[i]['This Year'])}{meas_units}{is_percentage}{b_tag_close} this year"
                
                df_stories_avg_cy_ly.loc[row_index] = [string, dim, df_filtered['Last Year'].iloc[i], df_filtered['This Year'].iloc[i], df_filtered['Diff'].iloc[i], df_filtered['abs'].iloc[i], df_filtered.index[i]]
                row_index += 1
                
    df_stories_avg_cy_ly.sort_values(by = 'Diff %', ascending = False, inplace = True)
    df_stories_avg_cy_ly.reset_index(drop=True, inplace = True)
    df_stories_avg_cy_ly.drop(df_stories_avg_cy_ly[df_stories_avg_cy_ly['LY Value'] == 0].index, axis = 0, inplace = True)
    for i in df_stories_avg_cy_ly.index[0:2] if df_stories_avg_cy_ly.shape[0] > 2 else df_stories_avg_cy_ly.index:
        # related_fields_list = []
        if df_stories_avg_cy_ly.loc[i]['Diff %'] > 0:
            svg_type = 'SVG avg increase'
            related_fields = df_stories_avg_cy_ly.loc[i]['Dimension'] + ' : ' + df_stories_avg_cy_ly.loc[i]['Value'] + ' | ' + 'measure' + ' : ' + meas + ' | ' + 'function' + ' : ' + 'Story Rank CY vs LY Increase'
        else:
            svg_type = 'SVG avg decrease'
            related_fields = df_stories_avg_cy_ly.loc[i]['Dimension'] + ' : ' + df_stories_avg_cy_ly.loc[i]['Value'] + ' | ' + 'measure' + ' : ' + meas + ' | ' + 'function' + ' : ' + 'Story Rank CY vs LY Decrease'            
        # related_fields_list.append(related_fields)

        

        string = df_stories_avg_cy_ly.loc[i]['String']
        st_data = [meas, '+ ' + str(df_stories_avg_cy_ly.loc[i]['Diff %']) + '%', df_stories_avg_cy_ly.loc[i]['Value'] ]
        story_data = StoryData(st_data)
        importance += 0.1
        # importance = rank of dim_val
        version_num = 0
        insight_code = 'StoryAvgCyLy#' + df_stories_avg_cy_ly.loc[i]['Dimension'] + '#' + df_stories_avg_cy_ly.loc[i]['Value'] + '#'+ meas
        string = rename_variables(string, rename_dim_meas)
        tags = rename_variables(tags, rename_dim_meas)
        story_data = rename_variables(story_data, rename_dim_meas)
        # insert_insights(datamart_id, string, story_data, 'Avg CY vs LY', svg_type, related_fields, importance, tags, 'Story', 'story', cnxn, cursor, insight_code, version_num)
