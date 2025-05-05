# FinalPlaylist.py

import pandas as pd
import numpy as np
import uuid

def playlist(df_insights):
    try:
        tag_list = []
        for i in range(0, df_insights['Tags'].count()):
            unique_set = set(tag for tag in df_insights['Tags'][i].split('|')[:-1] if tag.strip())
            group = list(df_insights['Group'])
            unique_list = list(unique_set)
            tag_list.append(unique_list)
        df_insights = pd.DataFrame()
        for i in range(0, len(tag_list)):
            df_insights = df_insights.append(pd.DataFrame({'Tags': tag_list[i], 'Group': group[i]}))
        tag_count = df_insights.groupby(['Tags', 'Group']).size().reset_index(name='Count')
        tag_count = tag_count.sort_values(by='Count', ascending=False).reset_index(drop=True)
        remove_list = ['YTD', 'MTD', 'Rolling 3 Month', 'All', 'Slope', 'Combo Chart', 'Contributions', 'Profit%'] + list(df.columns)
        tag_dataframe = tag_count[~tag_count['Tags'].isin(remove_list)]
        tag_dataframe = tag_dataframe[~(tag_dataframe['Tags'] == '')].reset_index().drop('index', axis=1)
        return tag_dataframe
    except Exception as e:
        print(f'Exception in playlist:{e}')

def playlist_category(sql, category, limit=25):
    tag_list = []
    l = []
    col_list = []
    df_insights = pd.read_sql(sql, cnxn)
    tag_dataframe = playlist(df_insights)
    tag_dataframe['Column'] = np.nan
    sql = "SELECT * FROM m_insights_engine_group where EngineId = '" + engine_id + "'"
    group_df = pd.read_sql(sql, cnxn)
    group_df.set_index('GroupId', inplace=True)
    row = tag_dataframe.shape[0]
    if row > limit:
        row = limit
    for row in range(row):
        for col in Significant_dimensions:
            if tag_dataframe['Tags'].loc[row] in list(df[col].unique()):
                tag_dataframe['Column'].loc[row] = col
                col_list.append(col)
    for i in range(row):
        if tag_dataframe.loc[i]['Count'] > 1:
            pl_string = str(tag_dataframe.loc[i]['Count']) + ' insights about ' + str(tag_dataframe.loc[i]['Column']) + ': ' + tag_dataframe.loc[i]['Tags']
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
          ,[GroupId]) VALUES (?,?,?,?,?,?)''', (playlist_id, datamart_id, pl_string, tag_dataframe.loc[i]['Tags'], category, groupid))
                cursor.commit()
            except Exception as e:
                print(f'Exception in playlist_category:{e}')

def trending(sql):
    try:
        category = 'Trending'
        limit = 3
        playlist_category(sql, category)
    except Exception as e:
        print(f'Exception in trending:{e}')

group_sql = "SELECT DISTINCT [group] FROM tt_insights WHERE datamartid = '" + str(datamart_id) + "'"
groups = pd.read_sql(group_sql, cnxn)

for grp in groups['group']:
    sql = "SELECT * FROM tt_insights where [group] = '" + grp + "' and datamartid = '" + str(datamart_id) + "'"
    trending(sql)