from Playlist.playlist_def import *


def playlist_call(datamart_id, engine_id, source_engine, Significant_dimensions, df_sql_table_names, cnxn, cursor):
    dim_unique_values_dict = {}
    for df_name, cols in Significant_dimensions.items():
        tablename = df_sql_table_names[df_name]
        if len(cols) >= 1:
            for col in cols:
                distinct_dim_val_query = f"""
                                SELECT DISTINCT [{col}] FROM [dbo].[{tablename}]
                                """
                df_unique_values = query_on_table(distinct_dim_val_query, source_engine)
                dim_unique_values_dict[col] = list(df_unique_values[col])
    

    group_sql = "SELECT DISTINCT [group] FROM tt_insights WHERE datamartid = '" + str(datamart_id) + "'"
    groups = pd.read_sql(group_sql, cnxn)

    for grp in groups['group']:
        sql = "SELECT * FROM tt_insights where [group] = '" + grp + "' and datamartid = '" + str(datamart_id) + "'" 
        trending(datamart_id, engine_id, sql, dim_unique_values_dict, cnxn, cursor)