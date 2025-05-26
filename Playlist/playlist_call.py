from Playlist.playlist_def import *
import constants
import logging

logger = logging.getLogger(__name__)


def playlist_call():
    logger.info('Started generation Playlist.')
    datamart_id = constants.DATAMART_ID
    engine_id = constants.ENGINE_ID
    source_engine = constants.SOURCE_ENGINE
    Significant_dimensions = constants.SIGNIFICANT_DIMENSIONS
    df_sql_table_names = constants.DF_SQL_TABLE_NAMES
    cnxn = constants.CNXN
    cursor = constants.CURSOR
    
    
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