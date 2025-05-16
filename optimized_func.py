from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
# from constants import *
import pandas as pd
import calendar


def extract_date_ranges(dates_filter_dict, outliers_dates= None):
    """Extracts relevant date ranges from the provided dictionaries."""
    date_ranges = {
        'ty_start': dates_filter_dict.get('ty_start_date_dict', {}),
        'ty_end': dates_filter_dict.get('ty_end_date_dict', {}),
        'ly_start': dates_filter_dict.get('ly_start_date_dict', {}),
        'ly_end': dates_filter_dict.get('ly_end_date_dict', {}),
        'last12months_start': dates_filter_dict.get('last12months_start_date_dict', {}),
        'last12months_end': dates_filter_dict.get('last12months_end_date_dict', {}),
        'last52weeks_start': dates_filter_dict.get('last52weeks_start_date_dict', {}),
        'last52weeks_end': dates_filter_dict.get('last52weeks_end_date_dict', {}),
    }
    if outliers_dates:
        date_ranges.update({
            'mtd_start': outliers_dates.get('mtd_start_date_dict', {}),
            'mtd_end': outliers_dates.get('mtd_end_date_dict', {}),
            'ytd_start': outliers_dates.get('ytd_start_date_dict', {}),
            'ytd_end': outliers_dates.get('ytd_end_date_dict', {}),
            'month_on_month_start': outliers_dates.get('month_on_month_start_date_dict', {}),
            'month_on_month_end': outliers_dates.get('month_on_month_end_date_dict', {}),
            'week_on_week_start': outliers_dates.get('week_on_week_start_date_dict', {}),
            'week_on_week_end': outliers_dates.get('week_on_week_end_date_dict', {}),
            'r3m_start': outliers_dates.get('r3m_start_date_dict', {}),
            'r3m_end': outliers_dates.get('r3m_end_date_dict', {}),
        })
    return date_ranges


def get_date_period(date_ranges, df_to_use, meas_table_name):
    """Retrieves start and end dates based on df_to_use."""
    if df_to_use == 'ThisYear':
        return date_ranges['ty_start'][meas_table_name], date_ranges['ty_end'][meas_table_name]
    elif df_to_use == 'LastYear':
        return date_ranges['ly_start'][meas_table_name], date_ranges['ly_end'][meas_table_name]
    elif df_to_use == 'AllYears':
        return date_ranges['ly_start'][meas_table_name], date_ranges['ty_end'][meas_table_name]
    elif df_to_use == 'Last12Months':
        return date_ranges['last12months_start'][meas_table_name], date_ranges['last12months_end'][meas_table_name]
    elif df_to_use == 'Last52Weeks':
        return date_ranges['last52weeks_start'][meas_table_name], date_ranges['last52weeks_end'][meas_table_name]
    elif df_to_use == 'ThisPeriodMTD':
        return date_ranges['mtd_start']['CurrentPeriod'], date_ranges['mtd_end']['CurrentPeriod']
    elif df_to_use == 'LastPeriodMTD':
        return date_ranges['mtd_start']['PreviousPeriod'], date_ranges['mtd_end']['PreviousPeriod']
    elif df_to_use == 'ThisPeriodR3M':
        return date_ranges['r3m_start']['CurrentPeriod'], date_ranges['r3m_end']['CurrentPeriod']
    elif df_to_use == 'LastPeriodR3M':
        return date_ranges['r3m_start']['PreviousPeriod'], date_ranges['r3m_end']['PreviousPeriod']
    elif df_to_use == 'ThisPeriodYTD':
        return date_ranges['ytd_start']['CurrentPeriod'], date_ranges['ytd_end']['CurrentPeriod']
    elif df_to_use == 'LastPeriodYTD':
        return date_ranges['ytd_start']['PreviousPeriod'], date_ranges['ytd_end']['PreviousPeriod']
    elif df_to_use == 'ThisPeriodWeekOnWeek':
        return date_ranges['week_on_week_start']['CurrentPeriod'], date_ranges['week_on_week_end']['CurrentPeriod']
    elif df_to_use == 'LastPeriodWeekOnWeek':
        return date_ranges['week_on_week_start']['PreviousPeriod'], date_ranges['week_on_week_end']['PreviousPeriod']
    elif df_to_use == 'ThisPeriodMonthOnMonth':
        return date_ranges['month_on_month_start']['CurrentPeriod'], date_ranges['month_on_month_end']['CurrentPeriod']
    elif df_to_use == 'LastPeriodMonthOnMonth':
        return date_ranges['month_on_month_start']['PreviousPeriod'], date_ranges['month_on_month_end']['PreviousPeriod']
    else:
        return None, None #Handle the default case


def get_outlier_date_range(outlier_val, dates_filter_dict, date_col, df_name):
    """
    Gets the date range string for outlier calculations

    Args:
        outlier_val (str): the outlier value
        dates_filter_dict (Dict): dict containing date ranges
        date_col (str): the date column
        df_name (str): the name of the dataframe
    Returns:
        tuple[str, str]: start and end dates
    """
    if outlier_val == 'ThisPeriodMTD':
        return dates_filter_dict['mtd_start_date_dict']['CurrentPeriod'], dates_filter_dict['mtd_end_date_dict']['CurrentPeriod']
    elif outlier_val == 'LastPeriodMTD':
        return dates_filter_dict['mtd_start_date_dict']['PreviousPeriod'], dates_filter_dict['mtd_end_date_dict']['PreviousPeriod']
    elif outlier_val == 'ThisPeriodYTD':
        return dates_filter_dict['ytd_start_date_dict']['CurrentPeriod'], dates_filter_dict['ytd_end_date_dict']['CurrentPeriod']
    elif outlier_val == 'LastPeriodYTD':
        return dates_filter_dict['ytd_start_date_dict']['PreviousPeriod'], dates_filter_dict['ytd_end_date_dict']['PreviousPeriod']
    elif outlier_val == 'ThisPeriodR3M':
        return dates_filter_dict['r3m_start_date_dict']['CurrentPeriod'], dates_filter_dict['r3m_end_date_dict']['CurrentPeriod']
    elif outlier_val == 'LastPeriodR3M':
        return dates_filter_dict['r3m_start_date_dict']['PreviousPeriod'], dates_filter_dict['r3m_end_date_dict']['PreviousPeriod']
    elif outlier_val == 'ThisPeriodMonthOnMonth':
        return dates_filter_dict['month_on_month_start_date_dict']['CurrentPeriod'], dates_filter_dict['month_on_month_end_date_dict']['CurrentPeriod']
    elif outlier_val == 'LastPeriodMonthOnMonth':
        return dates_filter_dict['month_on_month_start_date_dict']['PreviousPeriod'], dates_filter_dict['month_on_month_end_date_dict']['PreviousPeriod']
    elif outlier_val == 'ThisPeriodWeekOnWeek':
        return dates_filter_dict['week_on_week_start_date_dict']['CurrentPeriod'], dates_filter_dict['week_on_week_end_date_dict']['CurrentPeriod']
    elif outlier_val == 'LastPeriodWeekOnWeek':
        return dates_filter_dict['week_on_week_start_date_dict']['PreviousPeriod'], dates_filter_dict['week_on_week_end_date_dict']['PreviousPeriod']
    return None, None

def get_all_data_time_filter(meas_table_name, date_columns, meas_filter, df_sql_table_names):
    """Constructs the WHERE clause for time filtering when df_to_use is 'AllData'."""
    date_column = date_columns.get(meas_table_name)
    if date_column and date_column in meas_filter:
        sourcetable = df_sql_table_names.get(meas_table_name)
        if f"['{date_column}'].max()-timedelta(days=30))" in meas_filter:
            return f"WHERE [{date_column}] >= DATEADD(day, -30, (SELECT MAX([{date_column}]) FROM [dbo].[{sourcetable}]))"
        else:
            return ""
    return ""


def safe_sql_string(value):
    """Escapes single quotes in SQL string values to prevent errors."""
    if isinstance(value, str):
        return value.replace("'", "''")
    return str(value)

def get_relationship_column(df_relationship, table1_name, table2_name, column_name):
    """
    Retrieves the relationship column from the DataFrame.

    Args:
        df_relationship (pd.DataFrame): DataFrame containing the relationship between tables.
        table1_name (str): Name of the first table.
        table2_name (str): Name of the second table.
        column_name (str): 'column 1' or 'column 2' indicating which column to retrieve.

    Returns:
        str: The name of the relationship column, or None if not found.
    """
    condition = (
        ((df_relationship['file 1'] == table1_name) & (df_relationship['file 2'] == table2_name)) |
        ((df_relationship['file 2'] == table1_name) & (df_relationship['file 1'] == table2_name))
    )
    result = df_relationship.loc[condition, column_name]
    return result.iloc[0] if not result.empty else None


# def handle_no_dimension(source_type, source_engine,  meas_table, meas_col, meas_filter, meas_function, df_to_use):
#     """Handles cases with no dimension provided."""
#     formula = ""
#     if source_type == 'xlsx':
#         if meas_filter:
#             formula = f"df_to_use['{meas_table}'][{meas_filter}]['{meas_col}']"
#         else:
#             formula = f"df_to_use['{meas_table}']['{meas_col}']"
#         if meas_function:
#             formula = f"{meas_function}({formula})"
#         try:
#             result = eval(formula)
#             return pd.DataFrame({meas_col: [result]}), formula
#         except Exception as e:
#             print(f"Error in _handle_no_dimension: {e}")
#             return pd.DataFrame({meas_col: [None]}), formula
#     elif source_type == 'table':
#         query = f"SELECT {meas_function}([{meas_col}]) FROM [dbo].[{meas_table}] {meas_filter if meas_filter else ''}"
#         try:
#             df = pd.read_sql_query(query, source_engine)
#             return df, query
#         except Exception as e:
#             print(f"Error executing SQL query: {query}. Error: {e}")
#             return pd.DataFrame(), query # important to return query

#     return pd.DataFrame(), formula  # Default empty return, should not be reached.

def handle_no_dimension(source_type, source_engine, meas_table_name, meas_col, meas_filter, meas_function, 
                        df_to_use, date_columns, date_ranges, df_sql_table_names, df_sql_meas_functions):
    if source_type == 'table':
        sourcetable = df_sql_table_names[meas_table_name]
        meas_operation = df_sql_meas_functions[meas_function]
        where_clause = ""
        if df_to_use != 'AllData':
            start_period, end_period = get_date_period(date_ranges, df_to_use, meas_table_name)
            date_column = date_columns.get(meas_table_name)
            if start_period and end_period and date_column:
                where_clause = f"WHERE [{date_column}] BETWEEN CONVERT(DATETIME, '{start_period}', 105) AND CONVERT(DATETIME, '{end_period}', 105)"
        else:
            where_clause = get_all_data_time_filter(meas_table_name, date_columns, meas_filter, df_sql_table_names)
        if meas_filter:
            where_clause += f" AND {meas_filter}" if where_clause else f"WHERE {meas_filter}"
        query = f"SELECT {meas_function}([{meas_col}]) FROM [dbo].[{sourcetable}] {where_clause}"
        try:
            df = query_on_table(query, source_engine)
            return df, query
        except Exception as e:
            print(f"Error executing SQL query: {query}. Error: {e}")
            return pd.DataFrame(), query
        

# def process_same_table_groupby(source_type, source_engine, meas_table_name, dim_col, date_columns, meas_filter, df_sql_table_names,
#                                df_sql_meas_functions, meas_col, meas_function, date_ranges, df_to_use, key, 
#                                is_others, others_filter=None, extra_groupby_col=None):
    
#     """Handles grouping when the measure and dimension are in the same table."""
#     date_column = date_columns.get(meas_table_name)
#     table_name = f"[dbo].[{df_sql_table_names[meas_table_name]}]"
#     formula = ""
#     if source_type == 'xlsx':
#          if extra_groupby_col:
#             formula = f"df_to_use['{meas_table_name}'].groupby(['{extra_groupby_col}','{dim_col}'])['{meas_col}'].{meas_function}()"
#          else:
#             formula = f"df_to_use['{meas_table_name}'].groupby(['{dim_col}'])['{meas_col}'].{meas_function}()"
#          return eval(formula), formula
#     elif source_type == 'table':
#         select_clause = f"{dim_col}, {meas_function}([{meas_col}]) as {key}"
#         groupby_clause = f"GROUP BY {dim_col}"
#         where_clause = f"WHERE 1=1"  # Start with a neutral condition

#         if meas_filter:
#             where_clause += f" AND {meas_filter}"
#         if extra_groupby_col:
#             select_clause = f"{extra_groupby_col}, " + select_clause
#             groupby_clause = f"{extra_groupby_col}, " + groupby_clause

#         query = f"SELECT {select_clause} FROM {table_name} {where_clause} {groupby_clause}"
#         try:
#             df = query_on_table(query, source_engine)
#             return df, query
#         except Exception as e:
#             print(e)
#             return pd.DataFrame(), query

#     return pd.DataFrame(), formula


def process_same_table_groupby(source_type, source_engine, meas_table_name, dim_col, date_columns, meas_filter, df_sql_table_names,
                              df_sql_meas_functions, meas_col, meas_function, date_ranges, df_to_use, key, 
                              is_others, others_filter=None, extra_groupby_col=None):
    if source_type == 'table':
        table_name = f"[dbo].[{df_sql_table_names[meas_table_name]}]"
        meas_operation = df_sql_meas_functions.get(meas_function, meas_function)
        where_clause = ""
        if df_to_use != 'AllData':
            start_period, end_period = get_date_period(date_ranges, df_to_use, meas_table_name)
            date_column = date_columns.get(meas_table_name)
            if start_period and end_period and date_column:
                where_clause = f"WHERE [{date_column}] BETWEEN CONVERT(DATETIME, '{start_period}', 105) AND CONVERT(DATETIME, '{end_period}', 105)"
        else:
            where_clause = get_all_data_time_filter(meas_table_name, date_columns, meas_filter, df_sql_table_names)
        if others_filter and is_others:
            quoted_values = [safe_sql_string(val) for val in others_filter]
            others_filter_clause = f"AND [{dim_col}] IN ({', '.join(quoted_values)})"
            where_clause += f" {others_filter_clause}" if where_clause else f"WHERE {others_filter_clause[4:]}"
        if meas_filter:
            where_clause += f" AND {meas_filter}" if where_clause else f"WHERE {meas_filter}"
        groupby_clause = f"GROUP BY [{dim_col}]" if not extra_groupby_col else f"GROUP BY [{extra_groupby_col}], [{dim_col}]"
        select_clause = f"[{dim_col}], {meas_operation}([{meas_col}]) AS [{key}]" if not extra_groupby_col else f"[{extra_groupby_col}], [{dim_col}], {meas_operation}([{meas_col}]) AS [{key}]"
        query = f"SELECT {select_clause} FROM {table_name} {where_clause} {groupby_clause}"
        try:
            df = query_on_table(query, source_engine)
            if not is_others:
                df[key] = df[key].astype(float).round(2)
                df.set_index([extra_groupby_col, dim_col] if extra_groupby_col else dim_col, inplace=True)
                df = df[df.index != '']
            return df, query
        except Exception as e:
            print(e)
            return pd.DataFrame(), query
    


def process_different_table_groupby_xlsx(df_to_use, meas_table_name, dim_table, dim_col, meas_col, df_relationship, 
                                         meas_filter, meas_function, is_others, extra_groupby_col):
    """Handles grouping when measure and dimension are in different Excel tables."""

    relationship_column_meas = get_relationship_column(df_relationship, meas_table_name, dim_table, 'column 1')
    relationship_column_dim = get_relationship_column(df_relationship, meas_table_name, dim_table, 'column 2')
    formula = ""
    if is_others:
        #print(f"df_to_use['{meas_table_name}'].head().to_markdown(numalign='left', stralign='left')")
        #print(f"df_to_use['{dim_table}'].head().to_markdown(numalign='left', stralign='left')")
        if extra_groupby_col:
            formula = f"pd.merge(df_to_use['{meas_table_name}'], df_to_use['{dim_table}'], left_on='{relationship_column_meas}', right_on='{relationship_column_dim}').groupby(['{extra_groupby_col}','{dim_table}.{dim_col}'])['{meas_col}'].{meas_function}()"
        else:
            formula = f"pd.merge(df_to_use['{meas_table_name}'], df_to_use['{dim_table}'], left_on='{relationship_column_meas}', right_on='{relationship_column_dim}').groupby(['{dim_table}.{dim_col}'])['{meas_col}'].{meas_function}()"
    else:
        if extra_groupby_col:
          formula = f"pd.merge(df_to_use['{meas_table_name}'], df_to_use['{dim_table}'], left_on='{relationship_column_meas}', right_on='{relationship_column_dim}').groupby(['{extra_groupby_col}','{dim_col}'])['{meas_col}'].{meas_function}()"
        else:
          formula = f"pd.merge(df_to_use['{meas_table_name}'], df_to_use['{dim_table}'], left_on='{relationship_column_meas}', right_on='{relationship_column_dim}').groupby(['{dim_col}'])['{meas_col}'].{meas_function}()"
    return eval(formula), formula



# def process_different_table_groupby_table(source_engine, meas_table_name, dim_table, dim_col, meas_col, df_relationship, 
#                                           date_columns, meas_filter, meas_function, 
#                                           df_sql_table_names, df_sql_meas_functions, date_ranges, df_to_use,
#                                           key, is_others, others_filter=None, extra_groupby_col=None):
#     """Handles grouping when measure and dimension are in different SQL tables."""

#     relationship_column_meas = get_relationship_column(df_relationship, meas_table_name, dim_table, 'column 1')
#     relationship_column_dim = get_relationship_column(df_relationship, meas_table_name, dim_table, 'column 2')

#     meas_table_name_sql = f"[dbo].[{df_sql_table_names[meas_table_name]}]"
#     dim_table_name_sql = f"[dbo].[{df_sql_table_names[dim_table]}]"
#     alias_meas_table = "t1"
#     alias_dim_table = "t2"

#     select_clause = f"{alias_dim_table}.[{dim_col}], {df_sql_meas_functions.get(meas_function, meas_function)}({alias_meas_table}.[{meas_col}]) as {key}"
#     from_clause = f"FROM {meas_table_name_sql} as {alias_meas_table} INNER JOIN {dim_table_name_sql} as {alias_dim_table} ON {alias_meas_table}.[{relationship_column_meas}] = {alias_dim_table}.[{relationship_column_dim}]"
#     where_clause = "WHERE 1=1"  # Start with a neutral condition
#     groupby_clause = f"GROUP BY {alias_dim_table}.[{dim_col}]"

#     if meas_filter:
#         where_clause += f" AND {alias_meas_table}.{meas_filter}"
#     if extra_groupby_col:
#         select_clause = f"{alias_dim_table}.{extra_groupby_col}, " + select_clause
#         groupby_clause = f"{alias_dim_table}.{extra_groupby_col}, " + groupby_clause

#     query = f"SELECT {select_clause} {from_clause} {where_clause} {groupby_clause}"
#     try:
#         df = query_on_table(query, source_engine)
#         return df, query
#     except Exception as e:
#         print(e)
#         return pd.DataFrame(), query


def process_different_table_groupby_table(source_engine, meas_table_name, dim_table, dim_col, meas_col, df_relationship, 
                                         date_columns, meas_filter, meas_function, df_sql_table_names, df_sql_meas_functions, 
                                         date_ranges, df_to_use, key, is_others, others_filter=None, extra_groupby_col=None):
    meas_sourcetable = f"[dbo].[{df_sql_table_names[meas_table_name]}]"
    dim_sourcetable = f"[dbo].[{df_sql_table_names[dim_table]}]"
    meas_key_col = get_relationship_column(df_relationship, meas_table_name, dim_table, 'column 1')
    dim_key_col = get_relationship_column(df_relationship, meas_table_name, dim_table, 'column 2')
    meas_operation = df_sql_meas_functions.get(meas_function, meas_function)
    alias_meas_table, alias_dim_table = 't1', 't2'
    where_clause = ""
    if df_to_use != 'AllData':
        start_period, end_period = get_date_period(date_ranges, df_to_use, meas_table_name)
        date_column = date_columns.get(meas_table_name)
        if start_period and end_period and date_column:
            where_clause = f"WHERE {alias_meas_table}.[{date_column}] BETWEEN CONVERT(DATETIME, '{start_period}', 105) AND CONVERT(DATETIME, '{end_period}', 105)"
    else:
        where_clause = get_all_data_time_filter(meas_table_name, date_columns, meas_filter, df_sql_table_names)
    if others_filter and is_others:
        quoted_values = [safe_sql_string(val) for val in others_filter]
        others_filter_clause = f"AND {alias_dim_table}.[{dim_col}] IN ({', '.join(quoted_values)})"
        where_clause += f" {others_filter_clause}" if where_clause else f"WHERE {others_filter_clause[4:]}"
    if meas_filter:
        where_clause += f" AND {alias_meas_table}.{meas_filter}" if where_clause else f"WHERE {alias_meas_table}.{meas_filter}"
    groupby_clause = f"GROUP BY {alias_dim_table}.[{dim_col}]" if not extra_groupby_col else f"GROUP BY {alias_meas_table}.[{extra_groupby_col}], {alias_dim_table}.[{dim_col}]"
    select_clause = f"{alias_dim_table}.[{dim_col}], {meas_operation}({alias_meas_table}.[{meas_col}]) AS [{key}]" if not extra_groupby_col else f"{alias_meas_table}.[{extra_groupby_col}], {alias_dim_table}.[{dim_col}], {meas_operation}({alias_meas_table}.[{meas_col}]) AS [{key}]"
    query = f"SELECT {select_clause} FROM {meas_sourcetable} AS {alias_meas_table} INNER JOIN {dim_sourcetable} AS {alias_dim_table} ON {alias_meas_table}.[{meas_key_col}] = {alias_dim_table}.[{dim_key_col}] {where_clause} {groupby_clause}"
    try:
        df = query_on_table(query, source_engine)
        df[key] = pd.to_numeric(df[key], errors='coerce').fillna(0).round(2)
        if not is_others:
            df.set_index([extra_groupby_col, dim_col] if extra_groupby_col else dim_col, inplace=True)
            df = df[df.index != '']
        return df, query
    except Exception as e:
        print(f"Error executing query: {query}. Error: {e}")
        return pd.DataFrame(), query


# def process_other_operation(source_type, df_to_use, meas_table_name, meas_filter, other_operation_column,
#                             source_engine, date_ranges, date_columns, df_sql_table_names):
#     """Handles 'other' operations like counting unique values or rows."""
#     date_column = date_columns.get(meas_table_name)
#     table_name = f"[dbo].[{df_sql_table_names[meas_table_name]}]"
#     formula = ""
#     if source_type == 'xlsx':
#         if meas_filter:
#             formula = f"df_to_use['{meas_table_name}'][{meas_filter}]['{other_operation_column}']"
#         else:
#             formula = f"df_to_use['{meas_table_name}']['{other_operation_column}']"

#         unique_values = df_to_use[meas_table_name][other_operation_column].unique()
#         count_unique = len(unique_values)
#         all_values = df_to_use[meas_table_name][other_operation_column]
#         count_all = len(all_values)
#         return pd.DataFrame({
#             'unique_values': [unique_values],
#             'count_unique': [count_unique],
#             'count_all': [count_all]
#         }), formula
#     elif source_type == 'table':
#         query = f"SELECT COUNT(DISTINCT [{other_operation_column}]) as count_unique, COUNT([{other_operation_column}]) as count_all FROM {table_name} {meas_filter if meas_filter else ''}"
#         try:

#             df = query_on_table(query, source_engine)
#             df['unique_values'] = [pd.read_sql_query(f"SELECT DISTINCT {other_operation_column} from {table_name} {meas_filter if meas_filter else ''}", source_engine)[other_operation_column].tolist()]
#             return df, query
#         except Exception as e:
#             print(e)
#             return pd.DataFrame(), query
#     return pd.DataFrame(), formula


def process_other_operation(source_type, df_to_use, meas_table_name, meas_filter, other_operation_column,
                           source_engine, date_ranges, date_columns, df_sql_table_names):
    if source_type == 'table':
        table_name = f"[dbo].[{df_sql_table_names[meas_table_name]}]"
        date_column = date_columns.get(meas_table_name)
        start_period, end_period = get_date_period(date_ranges, df_to_use, meas_table_name)
        where_clause = f"WHERE [{date_column}] BETWEEN CONVERT(DATETIME, '{start_period}', 105) AND CONVERT(DATETIME, '{end_period}', 105)" if start_period and end_period and date_column else ""
        if meas_filter:
            where_clause += f" AND {meas_filter}" if where_clause else f"WHERE {meas_filter}"
        query = f"""
            WITH UniqueValues AS (
                SELECT DISTINCT [{other_operation_column}] AS unique_values
                FROM {table_name}
                {where_clause}
            )
            SELECT 
                (SELECT STRING_AGG(CAST(unique_values AS NVARCHAR(MAX)), ', ') FROM UniqueValues) AS unique_values, 
                (SELECT COUNT(*) FROM UniqueValues) AS count_unique,
                (SELECT COUNT(*) FROM {table_name} {where_clause}) AS count_rows
        """
        try:
            df = query_on_table(query, source_engine)
            return df, query
        except Exception as e:
            print(f"Error executing query: {query}. Error: {e}")
            return pd.DataFrame(), query


def query_on_table(query, source_engine):
    """
    Executes a SQL query on the specified source engine.  This function
    should handle errors and return a Pandas DataFrame.
    """
    try:
        df = pd.read_sql_query(query, source_engine)
        return df
    except Exception as e:
        print(f"Error executing query: {query}")
        print(e)
        return pd.DataFrame()  # Return an empty DataFrame on error


def get_groupby_data(source_type, source_engine, df_sql_table_names, df_sql_meas_functions, df_to_use, meas_table,
                     meas_col, meas_filter, meas_function, df_relationship, dim_table, dim_col, date_columns,
                     dates_filter_dict, key, is_ratio, is_total, is_others, others_filter=None, 
                     extra_groupby_col=None, other_operation_column=None, other_operation=None, outliers_dates=None):

    date_ranges = extract_date_ranges(dates_filter_dict, outliers_dates)
    meas_table_name = meas_table.split('[')[1].split(']')[0].strip("'") if '[' in meas_table else meas_table
    dim_table_name = dim_table

    if not other_operation:
        if source_type == 'xlsx' or (source_type == 'table' and (is_ratio or is_total or (not dim_table and not dim_col))):
            return handle_no_dimension(source_type, source_engine, meas_table_name, meas_col, meas_filter, meas_function, 
                                       df_to_use, date_columns, date_ranges, df_sql_table_names, df_sql_meas_functions)
                                       
        elif meas_table_name == dim_table:
            return process_same_table_groupby(source_type, source_engine, meas_table_name, dim_col, date_columns, 
                                              meas_filter, df_sql_table_names, df_sql_meas_functions, meas_col,
                                              meas_function, date_ranges, df_to_use, key, 
                                              is_others, others_filter, extra_groupby_col)
        else:
            if source_type == 'xlsx':
                return process_different_table_groupby_xlsx(df_to_use, meas_table_name, dim_table, dim_col, meas_col,
                                                            df_relationship, meas_filter, meas_function, is_others,
                                                            extra_groupby_col)
            elif source_type == 'table':
                return process_different_table_groupby_table(source_engine, meas_table_name, dim_table, dim_col, 
                                                             meas_col, df_relationship, date_columns, meas_filter, 
                                                             meas_function, df_sql_table_names, df_sql_meas_functions, 
                                                             date_ranges, df_to_use, key, is_others, others_filter, 
                                                             extra_groupby_col)
    elif other_operation:
        return process_other_operation(source_type, df_to_use, meas_table_name, meas_filter, other_operation_column,
                                        source_engine, date_ranges, date_columns, df_sql_table_names)
    return pd.DataFrame(), '' # Added to avoid the "Missing return statement" error.


def parent_get_group_data(source_type, source_engine, dim_col, meas, date_columns, dates_filter_dict, dim_table,
                          derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship,
                          df_to_use, is_ratio, is_total, is_others, others_filter=None, extra_groupby_col=None, 
                          other_operation_column=None, other_operation=None, outliers_val=None, outliers_dates=None):
    
    final_formula = derived_measures_dict_expanded[meas]['Formula']
    df_final = pd.DataFrame()
    df_grouped = {}
    
    for key, value in derived_measures_dict_expanded[meas].items():
        if key == 'Formula':
            continue
        table = value['Table']
        column = value['Column']
        filter_condition = value['Filter']
        function = value['Function']
        
        #  dim_table determination outside the loop
        if dim_col in ['Month-Day', 'Week', 'Month', 'Quarter']:
            dim_table = table.split('[')[1].split(']')[0].strip("'")

        df_calculated, formula = get_groupby_data(
                source_type, source_engine, df_sql_table_names, df_sql_meas_functions,
                df_to_use, table, column, filter_condition, function, df_relationship,
                dim_table, dim_col, date_columns, dates_filter_dict, key, is_ratio, is_total, is_others, others_filter,
                extra_groupby_col, other_operation_column, other_operation, outliers_dates
                )
        if other_operation:
            return df_calculated

        # Modify formula for xlsx with outliers
        if source_type == 'xlsx' and outliers_val:
            df_name = formula.split("df_to_use['")[1].split("']")[0]
            date_col = date_columns[df_name]
            date_start, date_end = get_outlier_date_range(outliers_val, dates_filter_dict, date_col, df_name)
            formula = formula.replace(f"df_to_use['{df_name}']", f"df_to_use['{df_name}'][(df_to_use['{df_name}']['{date_col}'] >= '{date_start}') & (df_to_use['{df_name}']['{date_col}'] <= '{date_end}')]")

        df_grouped[key] = df_calculated if source_type != 'table' or is_ratio or is_others else df_calculated
        
        if not is_ratio and not is_others and not is_total:
            if isinstance(df_grouped[key], pd.Series):
                df_grouped[key] = df_grouped[key].to_frame(name=key)
            if df_final.empty:
                df_final = df_grouped[key].rename(columns={meas: key})
            else:
                merge_on = [dim_col] if extra_groupby_col is None else [extra_groupby_col, dim_col]
                df_final = pd.merge(df_final, df_grouped[key], on=merge_on).rename(columns={meas: key})
            final_formula = final_formula.replace(key, f"df_final['{key}']")  # Correct replacement
        elif is_ratio or is_others or is_total:
             final_formula = final_formula.replace(key, str(df_grouped[key]))

    try:
        df_derived_measure = eval(final_formula)
    except ZeroDivisionError:
        df_derived_measure = 0

    if isinstance(df_derived_measure, pd.Series):
        df_derived_measure = df_derived_measure.to_frame(name=meas)

    return df_derived_measure.reset_index(drop=False) if extra_groupby_col else df_derived_measure