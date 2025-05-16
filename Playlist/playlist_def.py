from multiple_tables_csv_excel import *
from FinalCommon import *
from FinalParameters import *
from FinalCharts import *
import pandas as pd
import sys


def playlist(df_insights):
    try:
        tag_list = []
        for i in range(0,df_insights['Tags'].count()):
            unique_set = set(tag for tag in df_insights['Tags'][i].split('|')[:-1] if tag.strip())
            group = list(df_insights['Group'])
            unique_list = list(unique_set)
            tag_list.append(unique_list) 

        df_insights = pd.DataFrame()
        for i in range(0,len(tag_list)):
            df_insights = pd.concat([df_insights, pd.DataFrame({'Tags': tag_list[i], 'Group': group[i]})], ignore_index=True)

        tag_count = df_insights.groupby(['Tags', 'Group']).size().reset_index(name='Count')
        tag_count = tag_count.sort_values(by='Count', ascending=False).reset_index(drop=True)

        remove_list = ['YTD' , 'MTD', 'Rolling 3 Month' ,'Rolling 3 Months' , 'All' , 'Slope','Combo Chart','Contributions', 'Week on Week', 'Last 12 Months', 'Month on Month', 'Others', 'Decreasing', 'Increasing'] #+ list(df.columns)
        tag_dataframe = tag_count[~tag_count['Tags'].isin(remove_list)]
        tag_dataframe = tag_dataframe[~(tag_dataframe['Tags'] == '')].reset_index().drop('index' , axis = 1)

        return tag_dataframe
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        file_name = exc_tb.tb_frame.f_code.co_filename
        line_number = exc_tb.tb_lineno
        error_message = f"Error in playlist: {e} in file '{file_name}' at line {line_number}"
        print(error_message)
        return pd.DataFrame()


def playlist_category(datamart_id, engine_id, sql, category, dim_unique_values_dict, cnxn, cursor, limit = 25):
    tag_list = []
    l = []
    col_list = []
    df_insights = pd.read_sql(sql, cnxn)
    tag_dataframe = playlist(df_insights)
    tag_dataframe['Column'] = np.nan
    
    sql = "SELECT * FROM m_insights_engine_group where EngineId = '" + engine_id + "'"
    group_df = pd.read_sql(sql, cnxn)
    group_df.set_index('GroupId',inplace = True)

    row = tag_dataframe.shape[0]
    
    if row > limit:
        row = limit
    for row in range(row):
        for col in list(dim_unique_values_dict.keys()):
            if (tag_dataframe['Tags'].loc[row] in dim_unique_values_dict[col]):
                tag_dataframe['Column'].loc[row] = col
                col_list.append(col)
    
    for i in range(row):
        if tag_dataframe.loc[i]['Count'] > 2:
            if str(tag_dataframe.loc[i]['Column']) != 'nan':
                pl_string = str(tag_dataframe.loc[i]['Count']) + ' insights about ' + str(tag_dataframe.loc[i]['Column']) + ': ' + tag_dataframe.loc[i]['Tags']
            else:
                pl_string = str(tag_dataframe.loc[i]['Count']) + ' insights about ' + tag_dataframe.loc[i]['Tags']
                
            if len(pl_string) > 43:
                pl_string = pl_string[:40] + '...'
            group = tag_dataframe['Group'].loc[i]
            print(pl_string)
            print(group)
            try:
                groupid = group_df[group_df['GroupName'] == group].index[0]
                playlist_id = uuid.uuid1()
                cursor.execute('''INSERT INTO tt_playlist ([PlayListId]
                            ,[DatamartId]
                            ,[PlayListName]
                            ,[Tags]
                            ,[Category]
                            ,[GroupId]) VALUES (?,?,?,?,?,?)''' , (playlist_id, datamart_id, pl_string, tag_dataframe.loc[i]['Tags'],category, groupid))
                cursor.commit()
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                file_name = exc_tb.tb_frame.f_code.co_filename
                line_number = exc_tb.tb_lineno
                error_message = f"Error in playlist_category: {e} in file '{file_name}' at line {line_number}"
                print(error_message)


def trending(datamart_id, engine_id, sql, dim_unique_values_dict, cnxn, cursor):
    try:
        category = 'Trending'
        limit = 3
        playlist_category(datamart_id, engine_id, sql, category, dim_unique_values_dict, cnxn, cursor)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        file_name = exc_tb.tb_frame.f_code.co_filename
        line_number = exc_tb.tb_lineno
        error_message = f"Error in trending: {e} in file '{file_name}' at line {line_number}"
        print(error_message)


        
