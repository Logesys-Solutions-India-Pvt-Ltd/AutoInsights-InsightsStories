from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd
import numpy as np


def stories_x_times(sourcetype, source_engine, datamart_id, date_columns, dates_filter_dict, derived_measures_dict, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, Significant_dimensions, meas, df_list_ly, df_list_ty, df_relationship, rename_dim_meas, significance_score, importance, cnxn, cursor):
    row_index = 0
    is_ratio = False
    df_stories_xtimes = pd.DataFrame(columns=['String','X Times','Value','Dimension'])
    
    if sourcetype == 'xlsx':
        this_year_setting, last_year_setting = df_list_ty, df_list_ly
    elif sourcetype == 'table':
        this_year_setting, last_year_setting = 'ThisYear', 'LastYear'
        
    for dim_table, dim_list in Significant_dimensions.items():
        for dim in dim_list:
            if (meas in ['ATV', 'UPT']) and (dim_table == 'Item_master_Insights'):
                continue
            related_fields_list = []
#             data = round(ThisYear.groupby([dim])[meas].mean(),1).to_frame()
#             data.sort_values(by = meas, ascending = False, inplace = True)

            data = parent_get_group_data(sourcetype, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio=False, is_total=False, is_others=False)
            data.sort_values(by = meas, ascending = False, inplace = True)
            data[meas].fillna(0, inplace = True)
            data.replace([np.inf, -np.inf], 0, inplace=True)
            
            if '/' in derived_measures_dict[meas]['Formula']:
                is_ratio = True

            for key, value in derived_measures_dict[meas].items():
                if key != 'Formula': 
                    if 'mean()' in value:
                        is_ratio = True
#             average = round(ThisYear[meas].mean(),2) 
            if is_ratio:
                average = parent_get_group_data(sourcetype, source_engine, '', meas, date_columns, dates_filter_dict, '', derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, this_year_setting, is_ratio = True, is_total = False,is_others=False)
                average = round(average, 2)
            else:
                average = round(data[meas].mean(), 2)
                
            data = pd.merge(data, significance_score[dim_table][dim][-5:], left_index = True, right_index = True)
            for influencer in data.index:
                value = data.loc[influencer][meas] #35%
                actual = round(value / average,2) #1.6 times value
                if(actual >= 1.3):
                    data['Average'] = average
#                     string = 'Average daily '  + meas + ' of ' + b_tag_open + dim + ' : ' + influencer + b_tag_close + ' in this year is ' + b_tag_open + str(human_format(value)) + b_tag_close + ' which is ' + b_tag_open + str(actual) + 'x ' + b_tag_close + 'the overall average ' + b_tag_open + '(' + str(human_format(average)) + ')' + b_tag_close
                    string =  meas + ' of ' + b_tag_open + dim + ' : ' + influencer + b_tag_close + ' in this year is ' + b_tag_open + str(human_format(value)) + b_tag_close + ' which is ' + b_tag_open + str(actual) + 'x ' + b_tag_close + 'the overall average ' + b_tag_open + '(' + str(human_format(average)) + ')' + b_tag_close
                    related_fields = dim + ' : ' + influencer + ' | ' + 'measure' + ' : ' + meas + ' | ' + 'function' + ' : ' + 'Story X times'
                    df_stories_xtimes.loc[row_index] = [string, actual, influencer,dim]
                    row_index += 1
    df_stories_xtimes.sort_values(by = 'X Times', ascending = False, inplace = True)
    df_stories_xtimes.reset_index(drop=True, inplace = True)
    for i in df_stories_xtimes.index[0:2] if df_stories_xtimes.shape[0] > 2 else df_stories_xtimes.index:
        related_fields_list = []
        string = df_stories_xtimes.loc[i]['String']
        related_fields = df_stories_xtimes.loc[i]['Dimension'] + ' : ' + df_stories_xtimes.loc[i]['Value'] + ' | ' + 'measure' + ' : ' + meas + ' | ' + 'function' + ' : ' + 'Story X times'
        related_fields_list.append(related_fields)
        related_fields_list = '#|#'.join(related_fields_list)
        tags = dim + '|' + meas
        st_data = ['Overall Avg', df_stories_xtimes.loc[i]['Value'], str(df_stories_xtimes.loc[i]['X Times']) + 'x', meas]
        story_data = StoryData(st_data)
        importance += 0.1
        version_num = 0
        insight_code = 'StoryXTimes#' + df_stories_xtimes.loc[i]['Dimension'] + '#' + df_stories_xtimes.loc[i]['Value'] + '#'+ meas
        
        string = rename_variables(string, rename_dim_meas)
        tags = rename_variables(tags, rename_dim_meas)
        story_data = rename_variables(story_data, rename_dim_meas)
        
        # insert_insights(datamart_id, string, story_data, 'X Times', 'SVG x times', related_fields_list, importance, tags, 'Story', 'story', cnxn, cursor, insight_code, version_num)         
