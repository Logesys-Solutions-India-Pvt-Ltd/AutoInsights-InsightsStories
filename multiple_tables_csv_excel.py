from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
# from constants import *
import pandas as pd
import calendar


def safe_sql_string(value):
    """Safely quote string values for SQL by doubling apostrophes"""
    return "'" + str(value).replace("'", "''") + "'"

def query_on_table(query, source_engine):
    query = text(query)
    
    with source_engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()  
        column_names = result.keys()
        df_data = pd.DataFrame(rows, columns=column_names)
    return df_data

def rename_variables(text, rename_dict):
    """
    Args:
        text (str): Input text containing variables to be renamed
        rename_dict (dict): Dictionary mapping old names to new names
    """
    # Sort dictionary items by key length in descending order to avoid partial replacements
    sorted_items = sorted(rename_dict.items(), key=lambda x: len(x[0]), reverse=True)
    
    # Replace each old name with its new name
    result = text
    for old_name, new_name in sorted_items:
        result = result.replace(old_name, new_name)
    
    return result


def check_latest_month_completeness(sourcetype, source_engine, date_column, table_to_check, df_sql_table_names, df_list):
    """
    Check if the latest month in the data is complete.
    
    Parameters:
        sourcetype (str): Type of source, either 'table' (SQL) or 'csv'.
        date_column (dict): Dictionary mapping table names to their date columns.
        table_to_check (str): The table or CSV key to check.
        df_list (dict): Dictionary of DataFrames (for CSV case).
        df_sql_table_names (dict): Dictionary mapping table names to SQL table names (for SQL case).
    
    Returns:
        bool: True if the latest month is complete, False otherwise.
    """
    # Get the date column
    date_to_check = date_column[table_to_check]

    if sourcetype == 'table':
        table_name = df_sql_table_names[table_to_check]
        query = f"""
        SELECT MAX([{date_to_check}]) AS LatestDate
        FROM [dbo].[{table_name}];
        """
        df_data = query_on_table(query, source_engine)  
        latest_date = df_data['LatestDate'][0]

        latest_date = pd.to_datetime(latest_date)
        first_day_of_latest_month = datetime(latest_date.year, latest_date.month, 1)
        last_day_of_latest_month = datetime(latest_date.year, latest_date.month, 1) + pd.DateOffset(months=1) - timedelta(days=1)

        query = f"""
            SELECT DISTINCT [{date_to_check}]
            FROM [dbo].[{table_name}]
            WHERE [{date_to_check}] >= '{first_day_of_latest_month.date()}' AND [{date_to_check}] <= '{last_day_of_latest_month.date()}'
        """
        data_for_latest_month_df = query_on_table(query, source_engine)
        present_dates = data_for_latest_month_df[date_to_check].tolist()

    elif sourcetype == 'xlsx':
        df_to_check = df_list[table_to_check]  
        df_to_check[date_to_check] = pd.to_datetime(df_to_check[date_to_check])  
        latest_date = df_to_check[date_to_check].max()  

        # Filter for the latest month
        first_day_of_latest_month = datetime(latest_date.year, latest_date.month, 1)
        last_day_of_latest_month = datetime(latest_date.year, latest_date.month, 1) + pd.DateOffset(months=1) - timedelta(days=1)

        data_for_latest_month_df = df_to_check[
            (df_to_check[date_to_check] >= first_day_of_latest_month) & 
            (df_to_check[date_to_check] <= last_day_of_latest_month)
        ]
        present_dates = data_for_latest_month_df[date_to_check].dt.normalize().unique()
    
    # Generate all days in the latest month
    all_days_in_latest_month = pd.date_range(first_day_of_latest_month, last_day_of_latest_month, freq='D')

    # Ensure both sets are in Timestamp format
    all_days_in_latest_month = pd.to_datetime(all_days_in_latest_month)
    present_dates = pd.to_datetime(present_dates)

    # Convert to sets for comparison
    all_days_set = set(all_days_in_latest_month)
    present_dates_set = set(present_dates)

    missing_dates = all_days_set - present_dates_set

    return not missing_dates


def calculate_periodic_dates_for_outliers(sourcetype, source_engine, date_columns, df_sql_table_names, df_list, df_list_ty, df_list_ly):
    """
    Calculates start and end dates for various time periods (MTD, YTD, Month On Month,
    Week On Week, Rolling 3 Months) for current and previous periods based on the
    data source type.

    Args:
        sourcetype (str): The type of data source ('table' or 'xlsx').
        date_column (dict): A dictionary containing the date column name and the table to check.
                             e.g., {'your_date_column': 'your_table_name'}.
        df_sql_table_names (list): A list of SQL table names (used for 'table' sourcetype).
        df_list (list, optional): A list of DataFrames (used for 'xlsx' sourcetype). Defaults to None.
        df_list_ty (list, optional): A list of 'This Year' DataFrames (used for 'xlsx' sourcetype). Defaults to None.
        df_list_ly (list, optional): A list of 'Last Year' DataFrames (used for 'xlsx' sourcetype). Defaults to None.

    Returns:
        tuple: A tuple containing the following dictionaries:
            - mtd_start_date_dict
            - mtd_end_date_dict
            - ytd_start_date_dict
            - ytd_end_date_dict
            - month_on_month_start_date_dict
            - month_on_month_end_date_dict
            - week_on_week_start_date_dict
            - week_on_week_end_date_dict
            - r3m_start_date_dict
            - r3m_end_date_dict
    """
    mtd_start_date_dict, mtd_end_date_dict = {}, {}
    ytd_start_date_dict, ytd_end_date_dict = {}, {}
    month_on_month_start_date_dict, month_on_month_end_date_dict = {}, {}
    week_on_week_start_date_dict, week_on_week_end_date_dict = {}, {}
    r3m_start_date_dict, r3m_end_date_dict = {}, {}

    table_to_check, date_col = next(iter(date_columns.items()))

    latest_month_completeness = check_latest_month_completeness(sourcetype, source_engine, date_columns, table_to_check, df_sql_table_names, df_list)

    periods_to_calculate = ['MTD', 'YTD', 'Month On Month', 'Week On Week', 'Rolling 3 Months']

    if sourcetype == 'table':
        for val in periods_to_calculate:
            # print(f'val:{val}')
            df_dates = generate_dates_sql(val, date_columns, table_to_check, latest_month_completeness, df_sql_table_names, source_engine)
            if not df_dates.empty:
                current_start = df_dates[(df_dates['PeriodType'] == 'CurrentPeriod')]['PeriodStart'].iloc[0]
                previous_start = df_dates[(df_dates['PeriodType'] == 'PreviousPeriod')]['PeriodStart'].iloc[0]
                current_end = df_dates[(df_dates['PeriodType'] == 'CurrentPeriod')]['PeriodEnd'].iloc[0]
                previous_end = df_dates[(df_dates['PeriodType'] == 'PreviousPeriod')]['PeriodEnd'].iloc[0]

                if val == 'MTD':
                    mtd_start_date_dict['CurrentPeriod'] = current_start
                    mtd_start_date_dict['PreviousPeriod'] = previous_start
                    mtd_end_date_dict['CurrentPeriod'] = current_end
                    mtd_end_date_dict['PreviousPeriod'] = previous_end
                elif val == 'YTD':
                    ytd_start_date_dict['CurrentPeriod'] = current_start
                    ytd_start_date_dict['PreviousPeriod'] = previous_start
                    ytd_end_date_dict['CurrentPeriod'] = current_end
                    ytd_end_date_dict['PreviousPeriod'] = previous_end
                elif val == 'Month On Month':
                    month_on_month_start_date_dict['CurrentPeriod'] = current_start
                    month_on_month_start_date_dict['PreviousPeriod'] = previous_start
                    month_on_month_end_date_dict['CurrentPeriod'] = current_end
                    month_on_month_end_date_dict['PreviousPeriod'] = previous_end
                elif val == 'Week On Week':
                    week_on_week_start_date_dict['CurrentPeriod'] = current_start
                    week_on_week_start_date_dict['PreviousPeriod'] = previous_start
                    week_on_week_end_date_dict['CurrentPeriod'] = current_end
                    week_on_week_end_date_dict['PreviousPeriod'] = previous_end
                elif val == 'Rolling 3 Months':
                    r3m_start_date_dict['CurrentPeriod'] = current_start
                    r3m_start_date_dict['PreviousPeriod'] = previous_start
                    r3m_end_date_dict['CurrentPeriod'] = current_end
                    r3m_end_date_dict['PreviousPeriod'] = previous_end

    elif sourcetype == 'xlsx':
        df_list_copy = df_list.copy()
        df_list_ty_copy = df_list_ty.copy()
        df_list_ly_copy = df_list_ly.copy()
        for val in periods_to_calculate:
            # print(f'val:{val}')
            df_dates = generate_dates_excel(val, date_columns, table_to_check, latest_month_completeness, df_sql_table_names, source_engine, df_list)
            if not df_dates.empty:
                current_start = df_dates[(df_dates['PeriodType'] == 'CurrentPeriod')]['PeriodStart'].iloc[0]
                previous_start = df_dates[(df_dates['PeriodType'] == 'PreviousPeriod')]['PeriodStart'].iloc[0]
                current_end = df_dates[(df_dates['PeriodType'] == 'CurrentPeriod')]['PeriodEnd'].iloc[0]
                previous_end = df_dates[(df_dates['PeriodType'] == 'PreviousPeriod')]['PeriodEnd'].iloc[0]

                if val == 'MTD':
                    mtd_start_date_dict['CurrentPeriod'] = current_start
                    mtd_start_date_dict['PreviousPeriod'] = previous_start
                    mtd_end_date_dict['CurrentPeriod'] = current_end
                    mtd_end_date_dict['PreviousPeriod'] = previous_end
                elif val == 'YTD':
                    ytd_start_date_dict['CurrentPeriod'] = current_start
                    ytd_start_date_dict['PreviousPeriod'] = previous_start
                    ytd_end_date_dict['CurrentPeriod'] = current_end
                    ytd_end_date_dict['PreviousPeriod'] = previous_end
                elif val == 'Month On Month':
                    month_on_month_start_date_dict['CurrentPeriod'] = current_start
                    month_on_month_start_date_dict['PreviousPeriod'] = previous_start
                    month_on_month_end_date_dict['CurrentPeriod'] = current_end
                    month_on_month_end_date_dict['PreviousPeriod'] = previous_end
                elif val == 'Week On Week':
                    week_on_week_start_date_dict['CurrentPeriod'] = current_start
                    week_on_week_start_date_dict['PreviousPeriod'] = previous_start
                    week_on_week_end_date_dict['CurrentPeriod'] = current_end
                    week_on_week_end_date_dict['PreviousPeriod'] = previous_end
                elif val == 'Rolling 3 Months':
                    r3m_start_date_dict['CurrentPeriod'] = current_start
                    r3m_start_date_dict['PreviousPeriod'] = previous_start
                    r3m_end_date_dict['CurrentPeriod'] = current_end
                    r3m_end_date_dict['PreviousPeriod'] = previous_end

    return {
        'mtd_start_date_dict': mtd_start_date_dict,
        'mtd_end_date_dict': mtd_end_date_dict,
        'ytd_start_date_dict': ytd_start_date_dict,
        'ytd_end_date_dict': ytd_end_date_dict,
        'month_on_month_start_date_dict': month_on_month_start_date_dict,
        'month_on_month_end_date_dict': month_on_month_end_date_dict,
        'week_on_week_start_date_dict': week_on_week_start_date_dict,
        'week_on_week_end_date_dict': week_on_week_end_date_dict,
        'r3m_start_date_dict': r3m_start_date_dict,
        'r3m_end_date_dict': r3m_end_date_dict
    }
def calculate_month_dates(max_date):
    last_day_of_month = calendar.monthrange(max_date.year, max_date.month)[1]

    if max_date.day == last_day_of_month:
        end_date = max_date
        prev_year_last_day = calendar.monthrange(max_date.year - 1, max_date.month)[1]
        start_date = max_date.replace(year=max_date.year - 1, day=prev_year_last_day)
    else:
        end_date = max_date.replace(day=1) - timedelta(days=1)
        start_date = max_date - timedelta(days=365)
        start_date = start_date.replace(day=1)

    return start_date, end_date


### NEW WEEKLY ANOMALIES WITH OPT CUT OFF
def calculate_week_dates(max_date):
#     max_date = datetime.strptime(max_date_str, "Timestamp('%Y-%m-%d 00:00:00')")
    days_until_sunday = 6 - max_date.weekday()

    if days_until_sunday == 0:
        days_until_sunday = 7

    last_day = max_date + timedelta(days=days_until_sunday)
    last_day = last_day.day

    if max_date.day == last_day:
        end_date = max_date
        start_date = max_date - timedelta(days=181)
    else:
        end_date = max_date - timedelta(days=7 - days_until_sunday)
        start_date = max_date - timedelta(days=181 - days_until_sunday)

    start_date_str = start_date
    end_date_str = end_date
    return start_date_str, end_date_str


def generate_dates_excel(period, date_column, table_to_check, complete_month_param, df_sql_table_names, df_list):
#     print(f'table_to_check:{table_to_check}')
    
    # Dictionary to store results for each dataframe
    date_to_check = date_column[table_to_check]
    
#     for df_name, df in df_list.items():
    # Get the latest date from the date column
    latest_date = df_list[table_to_check][date_to_check].max()

    # Convert to datetime if not already
    if not isinstance(latest_date, datetime):
        latest_date = pd.to_datetime(latest_date)

    # Calculate date ranges based on the period
    if period == 'Month On Month':
        if complete_month_param:
            current_period_start = latest_date.replace(day=1)
            current_period_end = (latest_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            previous_period_start = (latest_date.replace(day=1) - timedelta(days=32)).replace(day=1)
            previous_period_end = (latest_date.replace(day=1) - timedelta(days=1)).replace(day=1) + timedelta(days=32) - timedelta(days=1)
        else:
            current_period_start = latest_date.replace(day=1)
            current_period_end = latest_date
            previous_period_start = (latest_date.replace(day=1) - timedelta(days=32)).replace(day=1)
            previous_period_end = latest_date.replace(day=1) - timedelta(days=1)

    elif period == 'Rolling 3 Months':
        if complete_month_param:
            current_period_start = (latest_date.replace(day=1) - timedelta(days=60)).replace(day=1)
            current_period_end = (latest_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            previous_period_start = (latest_date.replace(day=1) - timedelta(days=150)).replace(day=1)
            previous_period_end = (latest_date.replace(day=1) - timedelta(days=90)).replace(day=1) + timedelta(days=32) - timedelta(days=1)
        else:
            current_period_start = latest_date - timedelta(days=90)
            current_period_end = latest_date
            previous_period_start = latest_date - timedelta(days=180)
            previous_period_end = latest_date - timedelta(days=91)

    elif period == 'MTD':
        if complete_month_param:
            current_period_start = latest_date.replace(day=1)
            current_period_end = (latest_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            previous_period_start = (latest_date.replace(day=1) - timedelta(days=365)).replace(day=1)
            previous_period_end = (latest_date.replace(day=1) - timedelta(days=365)).replace(day=1) + timedelta(days=32) - timedelta(days=1)
        else:
            current_period_start = latest_date.replace(day=1)
            current_period_end = latest_date
            previous_period_start = latest_date.replace(day=1) - timedelta(days=30)
            previous_period_end = latest_date - timedelta(days=1)

    elif period == 'Week On Week':
        current_period_start = latest_date - timedelta(days=6)
        current_period_end = latest_date
        previous_period_start = latest_date - timedelta(days=13)
        previous_period_end = latest_date - timedelta(days=7)

    elif period == 'YTD':
        current_period_start = latest_date.replace(month=1, day=1)
        current_period_end = latest_date
        previous_period_start = latest_date.replace(year=latest_date.year - 1, month=1, day=1)
        previous_period_end = latest_date.replace(year=latest_date.year - 1, month=12, day=31)

    else:
        raise ValueError("Invalid period type provided.")
        
        
    # Format dates to 'dd-MM-yyyy HH:mm:ss'
    current_period_start = current_period_start.strftime('%d-%m-%Y %H:%M:%S')
    current_period_end = current_period_end.strftime('%d-%m-%Y %H:%M:%S')
    previous_period_start = previous_period_start.strftime('%d-%m-%Y %H:%M:%S')
    previous_period_end = previous_period_end.strftime('%d-%m-%Y %H:%M:%S')

    # Create a DataFrame with the results
    date_results = pd.DataFrame({
        'PeriodStart': [current_period_start, previous_period_start],
        'PeriodEnd': [current_period_end, previous_period_end],
        'PeriodType': ['CurrentPeriod', 'PreviousPeriod']
    })
    
    return date_results


def generate_dates_sql(period, date_column, table_to_check, complete_month_param, df_sql_table_names, source_engine):
    """
    Generate date ranges for different period types with option for complete or incomplete months.
    
    Args:
        period (str): The type of period ('Month on Month', 'Rolling 3 Months', 'MTD', 'Week on Week', 'YTD')
        date_column (str): Name of the date column in the source table
        sourcetablename (str): Name of the source table
        complete_month (bool): If True, uses complete months for calculations, otherwise uses partial months
    
    Returns:
        DataFrame: Contains PeriodStart, PeriodEnd, and PeriodType for current and previous periods
    """
    
    complete_month = 1 if complete_month_param else 0
    # Get the date column
    date_to_check = date_column[table_to_check]
    table_name = df_sql_table_names[table_to_check]
    
    query = f"""
    DECLARE @FilterType NVARCHAR(20) = '{period}'; 

    WITH LatestDate AS (
        SELECT CAST(MAX([{date_to_check}]) AS DATE) AS LatestDate
        FROM [dbo].[{table_name}]
    ),
    DateRanges AS (
        SELECT 
            CASE 
                WHEN @FilterType = 'Month On Month' THEN 
                    CASE WHEN {complete_month} = 1 
                        THEN DATEADD(DAY, 1 - DAY(LatestDate), LatestDate)
                        ELSE DATEADD(DAY, 1, EOMONTH(LatestDate, -2))
                    END
                WHEN @FilterType = 'Rolling 3 Months' THEN 
                    CASE WHEN {complete_month} = 1 
                        THEN DATEADD(MONTH, -2, DATEADD(DAY, 1 - DAY(LatestDate), LatestDate))
                        ELSE DATEADD(DAY, 1, EOMONTH(LatestDate, -4))
                    END
                WHEN @FilterType = 'MTD' THEN DATEADD(DAY, 1 - DAY(LatestDate), LatestDate)
                WHEN @FilterType = 'Week On Week' THEN DATEADD(DAY, -6, LatestDate)
                WHEN @FilterType = 'YTD' THEN DATEFROMPARTS(YEAR(LatestDate), 1, 1)
            END AS CurrentPeriodStart,
            CASE 
                WHEN @FilterType = 'Month On Month' THEN 
                    CASE WHEN {complete_month} = 1 
                        THEN EOMONTH(LatestDate, 0)
                        ELSE EOMONTH(LatestDate, -1)
                    END
                WHEN @FilterType = 'Rolling 3 Months' THEN 
                    CASE WHEN {complete_month} = 1 
                        THEN EOMONTH(LatestDate, 0)
                        ELSE EOMONTH(LatestDate, -1)
                    END
                WHEN @FilterType = 'MTD' THEN 
                    CASE WHEN {complete_month} = 1 
                        THEN EOMONTH(LatestDate, 0)
                        ELSE LatestDate
                    END
                WHEN @FilterType = 'Week On Week' THEN LatestDate
                WHEN @FilterType = 'YTD' THEN LatestDate
            END AS CurrentPeriodEnd,
            CASE 
                WHEN @FilterType = 'Month On Month' THEN 
                    CASE WHEN {complete_month} = 1 
                        THEN DATEADD(MONTH, -1, DATEADD(DAY, 1 - DAY(LatestDate), LatestDate))
                        ELSE DATEADD(DAY, 1, EOMONTH(LatestDate, -3))
                    END
                WHEN @FilterType = 'Rolling 3 Months' THEN 
                    CASE WHEN {complete_month} = 1 
                        THEN DATEADD(MONTH, -5, DATEADD(DAY, 1 - DAY(LatestDate), LatestDate))
                        ELSE DATEADD(DAY, 1, EOMONTH(LatestDate, -7))
                    END
                WHEN @FilterType = 'MTD' THEN 
                    CASE WHEN {complete_month} = 1 
                        THEN DATEADD(YEAR, -1, DATEADD(DAY, 1 - DAY(LatestDate), LatestDate))
                        ELSE DATEADD(MONTH, -1, DATEADD(DAY, 1 - DAY(LatestDate), LatestDate))
                    END
                WHEN @FilterType = 'Week On Week' THEN DATEADD(DAY, -13, LatestDate)
                WHEN @FilterType = 'YTD' THEN DATEFROMPARTS(YEAR(LatestDate) - 1, 1, 1)
            END AS PreviousPeriodStart,
            CASE 
                WHEN @FilterType = 'Month On Month' THEN 
                    CASE WHEN {complete_month} = 1 
                        THEN EOMONTH(LatestDate, -1)
                        ELSE EOMONTH(LatestDate, -2)
                    END
                WHEN @FilterType = 'Rolling 3 Months' THEN 
                    CASE WHEN {complete_month} = 1 
                        THEN EOMONTH(LatestDate, -3)
                        ELSE EOMONTH(LatestDate, -4)
                    END
                WHEN @FilterType = 'MTD' THEN 
                    CASE WHEN {complete_month} = 1 
                        THEN DATEADD(YEAR, -1, EOMONTH(LatestDate, 0))
                        ELSE DATEADD(MONTH, -1, LatestDate)
                    END
                WHEN @FilterType = 'Week On Week' THEN DATEADD(DAY, -7, LatestDate)
                WHEN @FilterType = 'YTD' THEN DATEFROMPARTS(YEAR(LatestDate) - 1, MONTH(LatestDate), DAY(LatestDate))
            END AS PreviousPeriodEnd
        FROM LatestDate
    )
    SELECT 
        FORMAT(CurrentPeriodStart, 'dd-MM-yyyy HH:mm:ss') AS PeriodStart,
        FORMAT(CurrentPeriodEnd, 'dd-MM-yyyy HH:mm:ss') AS PeriodEnd,
        'CurrentPeriod' AS PeriodType 
    FROM DateRanges
    UNION ALL
    SELECT 
        FORMAT(PreviousPeriodStart, 'dd-MM-yyyy HH:mm:ss') AS PeriodStart,
        FORMAT(PreviousPeriodEnd, 'dd-MM-yyyy HH:mm:ss') AS PeriodEnd,
        'PreviousPeriod' 
    FROM DateRanges;
    """
    
    df_data = query_on_table(query, source_engine)
    return df_data


def df_others(sourcetype, source_engine, df_data, split, df_to_use, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, is_ratio, is_total):
    # Check if others processing is necessary
    if df_data.shape[0] <= split + 1:
        return df_data, 0, 0

    # Split data into main and "others"
    others_filter = df_data.index[split:]
    others_value = 0
#     df_main = df_data.head(split)
    
    # Filter the "others" from the main dataset
    if sourcetype == 'xlsx':
        df_to_use_others = df_to_use.copy()
        df_to_use_others[dim_table] = df_to_use[dim_table][df_to_use[dim_table][dim].isin(others_filter)]

        others_value = parent_get_group_data(sourcetype, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, df_to_use_others, is_ratio, is_total, is_others=True, others_filter=others_filter, outliers_val=None)

    else:
        others_filter = others_filter.tolist()
        df_to_use_others = df_to_use
        others_value = parent_get_group_data(sourcetype, source_engine, dim, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, df_to_use_others, is_ratio, is_total, is_others=True, others_filter=others_filter, outliers_val=None)

    
    # Calculate aggregated "others" value
#     others_value = parent_get_group_data(sourcetype, dim, meas, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_to_use_others, is_ratio, is_total, is_others=True, others_filter=others_filter)

#     # Create a DataFrame for the "others" value
# #     others_label = f"{len(others_filter)} Others"
# #     df_others_value = pd.DataFrame({meas: [others_value]}, index=[others_label])
# #     df_others_value.index.name = dim

#     # Concatenate main and "others" data
# #     return pd.concat([df_main, df_others_value])
    return df_data, len(others_filter), others_value


def get_groupby_data(sourcetype, source_engine, df_sql_table_names, df_sql_meas_functions, df_to_use, meas_table, meas_col, meas_filter, meas_function, df_relationship, dim_table, dim_col, date_columns, dates_filter_dict, key, is_ratio, is_total, is_others, others_filter, extra_groupby_col, other_operation_column, other_operation, outliers_dates=None):
    """
    extra_groupby_col: is used if we have done .groupby(['Col A', 'dim']). 'Col A' is passed in 'extra_groupby_col'
    """
    ty_start_date_dict = dates_filter_dict['ty_start_date_dict']
    ty_end_date_dict = dates_filter_dict['ty_end_date_dict']
    ly_start_date_dict = dates_filter_dict['ly_start_date_dict']
    ly_end_date_dict = dates_filter_dict['ly_end_date_dict']
    ly_end_date_dict = dates_filter_dict['ly_end_date_dict']
    last12months_start_date_dict = dates_filter_dict['last12months_start_date_dict']
    last12months_end_date_dict = dates_filter_dict['last12months_end_date_dict']
    last52weeks_start_date_dict = dates_filter_dict['last52weeks_start_date_dict']
    last52weeks_end_date_dict = dates_filter_dict['last52weeks_end_date_dict']

    if outliers_dates is not None:
        mtd_start_date_dict = outliers_dates['mtd_start_date_dict']
        mtd_end_date_dict = outliers_dates['mtd_end_date_dict']
        ytd_start_date_dict = outliers_dates['ytd_start_date_dict']
        ytd_end_date_dict = outliers_dates['ytd_end_date_dict']
        month_on_month_start_date_dict = outliers_dates['month_on_month_start_date_dict']
        month_on_month_end_date_dict = outliers_dates['month_on_month_end_date_dict']
        week_on_week_start_date_dict = outliers_dates['week_on_week_start_date_dict']
        week_on_week_end_date_dict = outliers_dates['week_on_week_end_date_dict']
        r3m_start_date_dict = outliers_dates['r3m_start_date_dict']
        r3m_end_date_dict = outliers_dates['r3m_end_date_dict']

    # Handle ratio or empty dimension case
    meas_table_name = meas_table.split('[')[1].split(']')[0].strip("'")
    # meas_table_name = meas_table
    dim_table_name = dim_table
    if not other_operation:    
        if sourcetype == 'xlsx':
            if is_ratio or is_total or (not dim_table and not dim_col):
                filter_clause = f"[{meas_filter}]" if meas_filter else ""
                meas_formula = f"{meas_table}{filter_clause}['{meas_col}']{f'.{meas_function}' if meas_function else ''}"
                meas_formula = meas_formula.replace("df_list", "df_to_use")

                return {}, meas_formula
        elif sourcetype == 'table':
            if is_ratio or is_total or (not dim_table and not dim_col):
                if meas_table_name not in ty_start_date_dict.keys():
                    df_to_use = 'AllData'
                    
                if df_to_use == 'ThisYear': 
                    start_period = ty_start_date_dict[meas_table_name]
                    end_period = ty_end_date_dict[meas_table_name]
                elif df_to_use == 'LastYear': 
                    start_period = ly_start_date_dict[meas_table_name]
                    end_period = ly_end_date_dict[meas_table_name]
                elif df_to_use == 'AllYears': 
                    start_period = ly_start_date_dict[meas_table_name]
                    end_period = ty_end_date_dict[meas_table_name]
                elif df_to_use == 'Last12Months':
                    start_period = last12months_start_date_dict[meas_table_name]
                    end_period = last12months_end_date_dict[meas_table_name]
                elif df_to_use == 'Last52Weeks':
                    start_period = last52weeks_start_date_dict[meas_table_name]
                    end_period = last52weeks_end_date_dict[meas_table_name]
                elif df_to_use == 'ThisPeriodMTD':
                    start_period = mtd_start_date_dict['CurrentPeriod']
                    end_period = mtd_end_date_dict['CurrentPeriod']
                elif df_to_use == 'LastPeriodMTD':
                    start_period = mtd_start_date_dict['PreviousPeriod']
                    end_period = mtd_end_date_dict['PreviousPeriod']
                elif df_to_use == 'ThisPeriodR3M':
                    start_period = r3m_start_date_dict['CurrentPeriod']
                    end_period = r3m_end_date_dict['CurrentPeriod']
                elif df_to_use == 'LastPeriodR3M':
                    start_period = r3m_start_date_dict['PreviousPeriod']
                    end_period = r3m_end_date_dict['PreviousPeriod']
                elif df_to_use == 'ThisPeriodYTD':
                    start_period = ytd_start_date_dict['CurrentPeriod']
                    end_period = ytd_end_date_dict['CurrentPeriod']
                elif df_to_use == 'LastPeriodYTD':
                    start_period = ytd_start_date_dict['PreviousPeriod']
                    end_period = ytd_end_date_dict['PreviousPeriod']
                elif df_to_use == 'ThisPeriodWeekOnWeek':
                    start_period = week_on_week_start_date_dict['CurrentPeriod']
                    end_period = week_on_week_end_date_dict['CurrentPeriod']
                elif df_to_use == 'LastPeriodWeekOnWeek':
                    start_period = week_on_week_start_date_dict['PreviousPeriod']
                    end_period = week_on_week_end_date_dict['PreviousPeriod']
                elif df_to_use == 'ThisPeriodMonthOnMonth':
                    start_period = month_on_month_start_date_dict['CurrentPeriod']
                    end_period = month_on_month_end_date_dict['CurrentPeriod']
                elif df_to_use == 'LastPeriodMonthOnMonth':
                    start_period = month_on_month_start_date_dict['PreviousPeriod']
                    end_period = month_on_month_end_date_dict['PreviousPeriod']
                    
                if df_to_use != 'AllData':
                    date_meas_table = date_columns[meas_table_name]
                sourcetable = df_sql_table_names[meas_table_name]
                meas_operation = df_sql_meas_functions[meas_function]

                groupby_clause = ""
                select_clause = f"{meas_operation}([{meas_col}]) AS [{key}]"
                if df_to_use != 'AllData':
                    where_time_filter_clause = f"WHERE [{date_meas_table}] BETWEEN CONVERT(DATETIME, '{start_period}', 105) AND CONVERT(DATETIME, '{end_period}', 105)"
                else:
                    # if meas_table_name in date_columns.keys() and any(col in meas_filter for col in date_columns[meas_table_name]):
                    if meas_table_name in date_columns.keys() and date_columns[meas_table_name] in meas_filter:
                        if f"['{date_columns[meas_table_name]}'].max()-timedelta(days=30))" in meas_filter:
                            where_time_filter_clause = f"WHERE [{date_columns[meas_table_name]}] >= DATEADD(day, -30, (SELECT MAX([{date_columns[meas_table_name]}]) FROM [dbo].[{sourcetable}]))"
                    else:
                        where_time_filter_clause = ""
                query = f"""
                        SELECT {select_clause}
                        FROM [dbo].[{sourcetable}]
                        {where_time_filter_clause}
                        """
#                 print(f'query:\n{query}')
                df_data = query_on_table(query, source_engine)
                return df_data, ''

        # Same table logic
        if meas_table_name == dim_table_name:
            if sourcetype == 'table':                
                if meas_table_name not in ty_start_date_dict.keys() or date_columns[meas_table_name] in meas_filter:
                    df_to_use = 'AllData'
                
                # Calculate start_period, end_period to be used in WHERE clause to filter the date_column
                if df_to_use == 'ThisYear': 
                    start_period = ty_start_date_dict[meas_table_name]
                    end_period = ty_end_date_dict[meas_table_name]
                elif df_to_use == 'LastYear': 
                    start_period = ly_start_date_dict[meas_table_name]
                    end_period = ly_end_date_dict[meas_table_name]
                elif df_to_use == 'AllYears': 
                    start_period = ly_start_date_dict[meas_table_name]
                    end_period = ty_end_date_dict[meas_table_name]
                elif df_to_use == 'Last12Months':
                    start_period = last12months_start_date_dict[meas_table_name]
                    end_period = last12months_end_date_dict[meas_table_name]
                elif df_to_use == 'Last52Weeks':
                    start_period = last52weeks_start_date_dict[meas_table_name]
                    end_period = last52weeks_end_date_dict[meas_table_name]
                elif df_to_use == 'ThisPeriodMTD':
                    start_period = mtd_start_date_dict['CurrentPeriod']
                    end_period = mtd_end_date_dict['CurrentPeriod']
                elif df_to_use == 'LastPeriodMTD':
                    start_period = mtd_start_date_dict['PreviousPeriod']
                    end_period = mtd_end_date_dict['PreviousPeriod']
                elif df_to_use == 'ThisPeriodR3M':
                    start_period = r3m_start_date_dict['CurrentPeriod']
                    end_period = r3m_end_date_dict['CurrentPeriod']
                elif df_to_use == 'LastPeriodR3M':
                    start_period = r3m_start_date_dict['PreviousPeriod']
                    end_period = r3m_end_date_dict['PreviousPeriod']
                elif df_to_use == 'ThisPeriodYTD':
                    start_period = ytd_start_date_dict['CurrentPeriod']
                    end_period = ytd_end_date_dict['CurrentPeriod']
                elif df_to_use == 'LastPeriodYTD':
                    start_period = ytd_start_date_dict['PreviousPeriod']
                    end_period = ytd_end_date_dict['PreviousPeriod']
                elif df_to_use == 'ThisPeriodWeekOnWeek':
                    start_period = week_on_week_start_date_dict['CurrentPeriod']
                    end_period = week_on_week_end_date_dict['CurrentPeriod']
                elif df_to_use == 'LastPeriodWeekOnWeek':
                    start_period = week_on_week_start_date_dict['PreviousPeriod']
                    end_period = week_on_week_end_date_dict['PreviousPeriod']
                elif df_to_use == 'ThisPeriodMonthOnMonth':
                    start_period = month_on_month_start_date_dict['CurrentPeriod']
                    end_period = month_on_month_end_date_dict['CurrentPeriod']
                elif df_to_use == 'LastPeriodMonthOnMonth':
                    start_period = month_on_month_start_date_dict['PreviousPeriod']
                    end_period = month_on_month_end_date_dict['PreviousPeriod']

                
                if df_to_use != 'AllData':
                    date_meas_table = date_columns[meas_table_name]
                sourcetable = df_sql_table_names[meas_table_name]
                meas_operation = df_sql_meas_functions[meas_function]
                
                if df_to_use != 'AllData':
                    where_time_filter_clause = f"WHERE [{date_meas_table}] BETWEEN CONVERT(DATETIME, '{start_period}', 105) AND CONVERT(DATETIME, '{end_period}', 105)"
                else:
                    if meas_table_name in date_columns.keys() and date_columns[meas_table_name] in meas_filter:
                        if f"['{date_columns[meas_table_name]}'].max()-timedelta(days=30))" in meas_filter:
                            where_time_filter_clause = f"WHERE [{date_columns[meas_table_name]}] >= DATEADD(day, -30, (SELECT MAX([{date_columns[meas_table_name]}]) FROM [dbo].[{sourcetable}]))"
                    else:
                        where_time_filter_clause = ""
                if others_filter:
#                     quoted_values = [f"'{str(val)}'" for val in others_filter]
                    quoted_values = [safe_sql_string(val) for val in others_filter]
                    others_filter_clause = f"AND [{dim_col}] IN ({', '.join(quoted_values)})"        
                    groupby_clause = ""
                    select_clause = f"{meas_operation}([{meas_col}]) AS [{key}]"
                else:
#                     extra_groupby_col: is used if we have done .groupby(['Col A', 'dim']). 'Col A' is passed in 'extra_groupby_col'
                    others_filter_clause = ""
                    group_by_columns = [dim_col] if extra_groupby_col is None else [extra_groupby_col, dim_col]
                    groupby_clause = f"GROUP BY {', '.join([f'[{col}]' for col in group_by_columns])}"
                    select_columns = [dim_col] if extra_groupby_col is None else [extra_groupby_col, dim_col]
                    select_clause = ', '.join([f'[{col}]' for col in select_columns]) + f", {meas_operation}([{meas_col}]) AS [{key}]"
                
                query = f"""
                        SELECT {select_clause}
                        FROM [dbo].[{sourcetable}] 
                        {where_time_filter_clause}
                        {others_filter_clause}
                        {groupby_clause}
                        """
                
                df_data = query_on_table(query, source_engine)

                if is_others:
                    return df_data, ''
                if not is_others:
                    df_data[key] = df_data[key].astype(float).round(2)
                    # Below if-else condition is done to ensure 'Year-Month' and 'dim' are indices if extra_groupby_col exists
                    if extra_groupby_col:
                        df_data.set_index([extra_groupby_col, dim_col], inplace=True)
                    else:
                        df_data.set_index(dim_col, inplace=True)
                        df_data = df_data[df_data.index != '']
#                     df_data = df_data.rename(index={'': 'Blank'})   
                    
                    return df_data, ''
            else:       
                group_by_columns = [dim_col] if extra_groupby_col is None else [extra_groupby_col, dim_col]
                group_by_clause = f".groupby({group_by_columns})" if not is_others else ""
                filter_clause = f"[{meas_filter}]" if meas_filter else ""
#                 meas_formula = f"df_to_use['{meas_table_name}']{filter_clause}{group_by_clause}['{meas_col}'].{meas_function}"
                meas_formula = f"df_to_use['{meas_table_name}']{filter_clause}{group_by_clause}['{meas_col}']{f'.{meas_function}' if meas_function else ''}"
                meas_formula = meas_formula.replace("df_list", "df_to_use")
                return {}, meas_formula

        # Different table logic
        meas_key_col = df_relationship.loc[
                    ((df_relationship['file 1'] == meas_table_name) & (df_relationship['file 2'] == dim_table_name)) |
                    ((df_relationship['file 2'] == meas_table_name) & (df_relationship['file 1'] == dim_table_name)),
                    'column 1'
                ].iloc[0]
        
        dim_key_col = df_relationship.loc[
                    ((df_relationship['file 1'] == meas_table_name) & (df_relationship['file 2'] == dim_table_name)) |
                    ((df_relationship['file 2'] == meas_table_name) & (df_relationship['file 1'] == dim_table_name)),
                    'column 2'
                ].iloc[0]        
        
        if sourcetype == 'xlsx':
            eval_meas_table = df_to_use[meas_table_name]
            eval_dim_table = df_to_use[dim_table_name]

            df_calculated = pd.merge(
                eval_meas_table[[meas_col, meas_key_col] + ([extra_groupby_col] if extra_groupby_col else [])],
                eval_dim_table[[dim_key_col, dim_col]],
                left_on=meas_key_col, right_on=dim_key_col, how='inner'
            ).drop([meas_key_col, dim_key_col], axis=1)
    
            
            group_by_columns = [dim_col] if extra_groupby_col is None else [extra_groupby_col, dim_col]
            group_by_clause = f".groupby({group_by_columns})" if not is_others else ""
            filter_clause = f"[{meas_filter}]" if meas_filter else ""
            final_formula = f"df_calculated{filter_clause}{group_by_clause}['{meas_col}']{f'.{meas_function}' if meas_function else ''}"
#             final_formula = f"df_calculated{filter_clause}{group_by_clause}['{meas_col}'].{meas_function}"
            return df_calculated, final_formula

        elif sourcetype == 'table':
            # if meas_table_name not in ty_start_date_dict.keys() or date_columns[meas_table_name] in meas_filter:
            if meas_table_name not in ty_start_date_dict.keys() or date_columns[meas_table_name] in meas_filter:
                df_to_use = 'AllData'
                    
                    
            if df_to_use == 'ThisYear': 
                start_period = ty_start_date_dict[meas_table_name]
                end_period = ty_end_date_dict[meas_table_name]
            elif df_to_use == 'LastYear': 
                start_period = ly_start_date_dict[meas_table_name]
                end_period = ly_end_date_dict[meas_table_name]
            elif df_to_use == 'AllYears': 
                start_period = ly_start_date_dict[meas_table_name]
                end_period = ty_end_date_dict[meas_table_name]
            elif df_to_use == 'Last12Months':
                start_period = last12months_start_date_dict[meas_table_name]
                end_period = last12months_end_date_dict[meas_table_name]
            elif df_to_use == 'Last52Weeks':
                start_period = last52weeks_start_date_dict[meas_table_name]
                end_period = last52weeks_end_date_dict[meas_table_name]
            elif df_to_use == 'ThisPeriodMTD':
                start_period = mtd_start_date_dict['CurrentPeriod']
                end_period = mtd_end_date_dict['CurrentPeriod']
            elif df_to_use == 'LastPeriodMTD':
                start_period = mtd_start_date_dict['PreviousPeriod']
                end_period = mtd_end_date_dict['PreviousPeriod']
            elif df_to_use == 'ThisPeriodR3M':
                start_period = r3m_start_date_dict['CurrentPeriod']
                end_period = r3m_end_date_dict['CurrentPeriod']
            elif df_to_use == 'LastPeriodR3M':
                start_period = r3m_start_date_dict['PreviousPeriod']
                end_period = r3m_end_date_dict['PreviousPeriod']
            elif df_to_use == 'ThisPeriodYTD':
                start_period = ytd_start_date_dict['CurrentPeriod']
                end_period = ytd_end_date_dict['CurrentPeriod']
            elif df_to_use == 'LastPeriodYTD':
                start_period = ytd_start_date_dict['PreviousPeriod']
                end_period = ytd_end_date_dict['PreviousPeriod']
            elif df_to_use == 'ThisPeriodWeekOnWeek':
                start_period = week_on_week_start_date_dict['CurrentPeriod']
                end_period = week_on_week_end_date_dict['CurrentPeriod']
            elif df_to_use == 'LastPeriodWeekOnWeek':
                start_period = week_on_week_start_date_dict['PreviousPeriod']
                end_period = week_on_week_end_date_dict['PreviousPeriod']
            elif df_to_use == 'ThisPeriodMonthOnMonth':
                start_period = month_on_month_start_date_dict['CurrentPeriod']
                end_period = month_on_month_end_date_dict['CurrentPeriod']
            elif df_to_use == 'LastPeriodMonthOnMonth':
                start_period = month_on_month_start_date_dict['PreviousPeriod']
                end_period = month_on_month_end_date_dict['PreviousPeriod']
                     
            if df_to_use != 'AllData':
                date_meas_table = date_columns[meas_table_name]
            meas_sourcetable = df_sql_table_names[meas_table_name]
            dim_sourcetable = df_sql_table_names[dim_table_name]
            meas_operation = df_sql_meas_functions[meas_function]
            
            if df_to_use != 'AllData':
                where_time_filter_clause = f"WHERE t1.[{date_meas_table}] BETWEEN CONVERT(DATETIME, '{start_period}', 105) AND CONVERT(DATETIME, '{end_period}', 105)"
            else:                
                if meas_table_name in date_columns.keys() and date_columns[meas_table_name] in meas_filter:
                    if f"['{date_columns[meas_table_name]}'].max()-timedelta(days=30))" in meas_filter:
                        where_time_filter_clause = f"WHERE [{date_columns[meas_table_name]}] >= DATEADD(day, -30, (SELECT MAX([{date_columns[meas_table_name]}]) FROM [dbo].[{meas_sourcetable}]))"
                else:
                    where_time_filter_clause = ""
                
                
            if is_others:
#                 quoted_values = [f"'{str(val)}'" for val in others_filter]
                quoted_values = [safe_sql_string(val) for val in others_filter]
                others_filter_clause = f"AND [{dim_col}] IN ({', '.join(quoted_values)})"
                groupby_clause = ""
                select_clause = f"{meas_operation}([{meas_col}]) AS [{key}]"
            else:
                others_filter_clause = ""
                group_by_columns = [f"t2.[{dim_col}]"]  # Default: Group by dimension column from dim_sourcetable
                select_columns = [f"t2.[{dim_col}]"]    # Default: Select dimension column from dim_sourcetable

                if extra_groupby_col:
                    group_by_columns.insert(0, f"t1.[{extra_groupby_col}]")  # Add column from meas_sourcetable
                    select_columns.insert(0, f"t1.[{extra_groupby_col}]")    # Add column from meas_sourcetable

                groupby_clause = f"GROUP BY {', '.join(group_by_columns)}"
                select_clause = ', '.join(select_columns) + f", {meas_operation}(t1.[{meas_col}]) AS [{key}]"


            query = f"""
                    SELECT {select_clause}
                    FROM [dbo].[{meas_sourcetable}] t1 
                    INNER JOIN [dbo].[{dim_sourcetable}] t2
                    ON t1.[{meas_key_col}] = t2.[{dim_key_col}]
                    {where_time_filter_clause}
                    {others_filter_clause}
                    {groupby_clause}
                    """
#             print(f'query:\n{query}')
            df_data = query_on_table(query, source_engine)
            df_data[key] = pd.to_numeric(df_data[key], errors='coerce').fillna(0)
#             print(f'df_data in diff meas and dim table:\n{df_data}')
            if is_others:
                return df_data, ''
            if not is_others:
                # Below if-else condition is done to ensure 'Year-Month' and 'dim' are indices if extra_groupby_col exists
                if extra_groupby_col:
                    df_data.set_index([extra_groupby_col, dim_col], inplace=True)
                else:
                    df_data.set_index(dim_col, inplace=True)
                df_data[key] = df_data[key].astype(float).round(2)
                df_data = df_data[df_data.index != '']
#                 df_data = df_data.rename(index={'': 'Blank'})

    #         df_data.index = df_data.index.map(lambda x: 'Blank' if x is None else x)
            return df_data, {}
    
    # If True, we calculate count of rows, unique values and count of unique values for mentioned column
    elif other_operation:
        if sourcetype == 'xlsx':
            eval_meas_table = df_to_use[meas_table_name]
            if meas_filter:
                eval_meas_table = eval_meas_table[eval(meas_filter)]
            # Calculate unique values, count of unique values, and count of rows
            unique_values = eval_meas_table[other_operation_column].unique()
            count_unique = eval_meas_table[other_operation_column].nunique()
            count_rows = len(eval_meas_table)

            result_df = pd.DataFrame({
                'unique_values': [unique_values],
                'count_unique': [count_unique],
                'count_rows': [count_rows]
            })
            return result_df, ''
        elif sourcetype == 'table':
            if df_to_use == 'ThisYear':
                start_period = dates_filter_dict['ty_start_date_dict'][meas_table_name]
                end_period = dates_filter_dict['ty_end_date_dict'][meas_table_name]
            elif df_to_use == 'LastYear':
                start_period = dates_filter_dict['ly_start_date_dict'][meas_table_name]
                end_period = dates_filter_dict['ly_end_date_dict'][meas_table_name]
            elif df_to_use == 'AllYears':
                start_period = dates_filter_dict['ly_start_date_dict'][meas_table_name]
                end_period = dates_filter_dict['ty_end_date_dict'][meas_table_name]
            elif df_to_use == 'Last12Months':
                start_period = dates_filter_dict['last12months_start_date_dict'][meas_table_name]
                end_period = dates_filter_dict['last12months_end_date_dict'][meas_table_name]
            elif df_to_use == 'Last52Weeks':
                start_period = dates_filter_dict['last52weeks_start_date_dict'][meas_table_name]
                end_period = dates_filter_dict['last52weeks_end_date_dict'][meas_table_name]
            elif df_to_use == 'ThisPeriodMTD':
                start_period = dates_filter_dict['mtd_start_date_dict']['CurrentPeriod']
                end_period = dates_filter_dict['mtd_end_date_dict']['CurrentPeriod']
            elif df_to_use == 'LastPeriodMTD':
                start_period = dates_filter_dict['mtd_start_date_dict']['PreviousPeriod']
                end_period = dates_filter_dict['mtd_end_date_dict']['PreviousPeriod']
            elif df_to_use == 'ThisPeriodR3M':
                start_period = dates_filter_dict['r3m_start_date_dict']['CurrentPeriod']
                end_period = dates_filter_dict['r3m_end_date_dict']['CurrentPeriod']
            elif df_to_use == 'LastPeriodR3M':
                start_period = dates_filter_dict['r3m_start_date_dict']['PreviousPeriod']
                end_period = dates_filter_dict['r3m_end_date_dict']['PreviousPeriod']
            elif df_to_use == 'ThisPeriodYTD':
                start_period = dates_filter_dict['ytd_start_date_dict']['CurrentPeriod']
                end_period = dates_filter_dict['ytd_end_date_dict']['CurrentPeriod']
            elif df_to_use == 'LastPeriodYTD':
                start_period = dates_filter_dict['ytd_start_date_dict']['PreviousPeriod']
                end_period = dates_filter_dict['ytd_end_date_dict']['PreviousPeriod']
            elif df_to_use == 'ThisPeriodWeekOnWeek':
                start_period = dates_filter_dict['week_on_week_start_date_dict']['CurrentPeriod']
                end_period = dates_filter_dict['week_on_week_end_date_dict']['CurrentPeriod']
            elif df_to_use == 'LastPeriodWeekOnWeek':
                start_period = dates_filter_dict['week_on_week_start_date_dict']['PreviousPeriod']
                end_period = dates_filter_dict['week_on_week_end_date_dict']['PreviousPeriod']
            elif df_to_use == 'ThisPeriodMonthOnMonth':
                start_period = dates_filter_dict['month_on_month_start_date_dict']['CurrentPeriod']
                end_period = dates_filter_dict['month_on_month_end_date_dict']['CurrentPeriod']
            elif df_to_use == 'LastPeriodMonthOnMonth':
                start_period = dates_filter_dict['month_on_month_start_date_dict']['PreviousPeriod']
                end_period = dates_filter_dict['month_on_month_end_date_dict']['PreviousPeriod']
            date_meas_table = date_columns[meas_table_name]
            sourcetable = df_sql_table_names[meas_table_name]

            # Build SQL query for unique values, count of unique values, and count of rows
            query = f"""
                    WITH UniqueValues AS (
                    SELECT DISTINCT [{other_operation_column}] AS unique_values
                    FROM [dbo].[{sourcetable}]
                    WHERE [{date_meas_table}] 
                        BETWEEN CONVERT(DATETIME, '{start_period}', 105) 
                        AND CONVERT(DATETIME, '{end_period}', 105)
                        {f"AND {meas_filter}" if meas_filter else ""}
                )
                SELECT 
                    (SELECT STRING_AGG(CAST(unique_values AS NVARCHAR(MAX)), ', ') FROM UniqueValues) AS unique_values, 
                    (SELECT COUNT(*) FROM UniqueValues) AS count_unique,
                    (SELECT COUNT(*) FROM [dbo].[{sourcetable}] 
                        WHERE [{date_meas_table}] 
                        BETWEEN CONVERT(DATETIME, '{start_period}', 105) 
                        AND CONVERT(DATETIME, '{end_period}', 105)
                        {f"AND {meas_filter}" if meas_filter else ""}
                    ) AS count_rows;"""
    
            df_data = query_on_table(query, source_engine)
#             df_data[key] = pd.to_numeric(df_data[key], errors='coerce').fillna(0)
#             print(f'df_data in get_groupby others:\n{df_data}')
            return df_data, ''


def parent_get_group_data(sourcetype, source_engine, dim_col, meas, date_columns, dates_filter_dict, dim_table, derived_measures_dict_expanded, df_sql_table_names, df_sql_meas_functions, df_relationship, df_to_use, is_ratio, is_total, is_others, others_filter=None, extra_groupby_col=None, other_operation_column=None, other_operation=None, outliers_val=None, outliers_dates=None):
    final_formula = derived_measures_dict_expanded[meas]['Formula']
    df_final = pd.DataFrame()
    df_grouped = {}
    df_calculated = pd.DataFrame()
    formula_in_dict = ''
    
    for key, value in derived_measures_dict_expanded[meas].items():
        if key == 'Formula':
            continue
        
        table = value['Table']
        column = value['Column']
        filter_condition = value['Filter']
        function = value['Function']
        
        if dim_col in ['Month-Day', 'Week', 'Month', 'Quarter']:
            dim_table = table
            # dim_table = table.split('[')[1].split(']')[0].strip("'")
        # If True, we calculate count of rows, unique values and count of unique values for mentioned column
        if other_operation:
            df_calculated, formula = get_groupby_data(sourcetype, source_engine, df_sql_table_names, df_sql_meas_functions,
                df_to_use, table, column, filter_condition, function, df_relationship,
                dim_table, dim_col, date_columns, dates_filter_dict, key, is_ratio, is_total, is_others, others_filter,
                extra_groupby_col, other_operation_column, other_operation, outliers_dates
            )
            return df_calculated
        else:
            df_calculated, formula = get_groupby_data(sourcetype, source_engine, df_sql_table_names, df_sql_meas_functions,
                df_to_use, table, column, filter_condition, function, df_relationship,
                dim_table, dim_col, date_columns, dates_filter_dict, key, is_ratio, is_total, is_others, others_filter,
                extra_groupby_col, other_operation_column, other_operation, outliers_dates
            )
            
        # Here we need to modify the formula to include date filtering based on outliers_val
        if sourcetype == 'xlsx' and outliers_val:
            df_name = formula.split("df_to_use['")[1].split("']")[0]
            date_col = date_columns[df_name]
    
            # Apply date filtering based on outliers_val
            if outliers_val == 'ThisPeriodMTD':
                formula = formula.replace(
                    f"df_to_use['{df_name}']", 
                    f"df_to_use['{df_name}'][(df_to_use['{df_name}']['{date_col}'] >= '{dates_filter_dict['mtd_start_date_dict']['CurrentPeriod']}') & (df_to_use['{df_name}']['{date_col}'] <= '{dates_filter_dict['mtd_end_date_dict']['CurrentPeriod']}')]"
                )
            elif outliers_val == 'LastPeriodMTD':
                formula = formula.replace(
                    f"df_to_use['{df_name}']", 
                    f"df_to_use['{df_name}'][(df_to_use['{df_name}']['{date_col}'] >= '{dates_filter_dict['mtd_start_date_dict']['PreviousPeriod']}') & (df_to_use['{df_name}']['{date_col}'] <= '{dates_filter_dict['mtd_end_date_dict']['PreviousPeriod']}')]"
                )
            elif outliers_val == 'ThisPeriodYTD':
                formula = formula.replace(
                    f"df_to_use['{df_name}']", 
                    f"df_to_use['{df_name}'][(df_to_use['{df_name}']['{date_col}'] >= '{dates_filter_dict['ytd_start_date_dict']['CurrentPeriod']}') & (df_to_use['{df_name}']['{date_col}'] <= '{dates_filter_dict['ytd_end_date_dict']['CurrentPeriod']}')]"
                )
            elif outliers_val == 'LastPeriodYTD':
                formula = formula.replace(
                    f"df_to_use['{df_name}']", 
                    f"df_to_use['{df_name}'][(df_to_use['{df_name}']['{date_col}'] >= '{dates_filter_dict['mtd_start_date_dict']['PreviousPeriod']}') & (df_to_use['{df_name}']['{date_col}'] <= '{dates_filter_dict['mtd_end_date_dict']['PreviousPeriod']}')]"
                )
            elif outliers_val == 'ThisPeriodR3M':    
                formula = formula.replace(
                    f"df_to_use['{df_name}']", 
                    f"df_to_use['{df_name}'][(df_to_use['{df_name}']['{date_col}'] >= '{dates_filter_dict['r3m_start_date_dict']['CurrentPeriod']}') & (df_to_use['{df_name}']['{date_col}'] <= '{dates_filter_dict['r3m_end_date_dict']['CurrentPeriod']}')]"
                )
            elif outliers_val == 'LastPeriodR3M':
                formula = formula.replace(
                    f"df_to_use['{df_name}']", 
                    f"df_to_use['{df_name}'][(df_to_use['{df_name}']['{date_col}'] >= '{dates_filter_dict['r3m_start_date_dict']['PreviousPeriod']}') & (df_to_use['{df_name}']['{date_col}'] <= '{dates_filter_dict['r3m_end_date_dict']['PreviousPeriod']}')]"
                )
            elif outliers_val == 'ThisPeriodMonthOnMonth':    
                formula = formula.replace(
                    f"df_to_use['{df_name}']", 
                    f"df_to_use['{df_name}'][(df_to_use['{df_name}']['{date_col}'] >= '{dates_filter_dict['month_on_month_start_date_dict']['CurrentPeriod']}') & (df_to_use['{df_name}']['{date_col}'] <= '{dates_filter_dict['month_on_month_end_date_dict']['CurrentPeriod']}')]"
                )
            elif outliers_val == 'LastPeriodMonthOnMonth':
                formula = formula.replace(
                    f"df_to_use['{df_name}']", 
                    f"df_to_use['{df_name}'][(df_to_use['{df_name}']['{date_col}'] >= '{dates_filter_dict['month_on_month_start_date_dict']['PreviousPeriod']}') & (df_to_use['{df_name}']['{date_col}'] <= '{dates_filter_dict['month_on_month_start_date_dict']['PreviousPeriod']}')]"
                )
            elif outliers_val == 'ThisPeriodWeekOnWeek':
                formula = formula.replace(
                    f"df_to_use['{df_name}']", 
                    f"df_to_use['{df_name}'][(df_to_use['{df_name}']['{date_col}'] >= '{dates_filter_dict['week_on_week_start_date_dict']['CurrentPeriod']}') & (df_to_use['{df_name}']['{date_col}'] <= '{dates_filter_dict['week_on_week_end_date_dict']['CurrentPeriod']}')]"
                )
            elif outliers_val == 'LastPeriodWeekOnWeek':
                formula = formula.replace(
                    f"df_to_use['{df_name}']", 
                    f"df_to_use['{df_name}'][(df_to_use['{df_name}']['{date_col}'] >= '{dates_filter_dict['week_on_week_start_date_dict']['PreviousPeriod']}') & (df_to_use['{df_name}']['{date_col}'] <= '{dates_filter_dict['week_on_week_start_date_dict']['PreviousPeriod']}')]"
                )
                  
        if sourcetype == 'xlsx':
            df_grouped[key] = eval(formula)
            
        elif sourcetype == 'table' and (is_others or is_ratio): # Since we need a single value and not a column of values
            df_grouped[key] = df_calculated[key][0]
        else:
            df_grouped[key] = df_calculated
            
        if not is_ratio and not is_others and not is_total:
            if isinstance(df_grouped[key], pd.Series): # Converting to a dataframe if it is a series
                df_grouped[key] = df_grouped[key].to_frame(name=key)
            # Executed mostly when it is the first iteration of the 'for' loop i.e first 'key' after 'Formula'
            if df_final.empty: 
                df_final = df_grouped[key].rename(columns={meas: key})
            else: 
                merge_on = [dim_col] if extra_groupby_col is None else [extra_groupby_col, dim_col]
                df_final = pd.merge(df_final, df_grouped[key], on=merge_on).rename(columns={meas: key})
#             print(f'df_final:{df_final}')
            # Replace the key in the formula, with the dataframe containing the calculated values 
            final_formula = final_formula.replace(key, f"df_final['{key}']")

        elif is_ratio or is_others or is_total:
            # Replace the key in the formula with the calculated singular value itself
            final_formula = final_formula.replace(key, str(df_grouped[key]))

    try:
        # Check if final_formula is a string that looks like a DataFrame output
        # (contains a row index and a numeric value)
        if isinstance(final_formula, str) and '\n' in final_formula and any(char.isdigit() for char in final_formula):
            # Try to extract the numeric value from the string representation
            try:
                # Split by lines and get the second line (which contains the value)
                lines = final_formula.strip().split('\n')
                if len(lines) >= 2:
                    # Extract the numeric value
                    value_str = lines[1].split()[-1]  # Get the last part of the second line
                    df_derived_measure = float(value_str)
            except (IndexError, ValueError):
                # If extraction fails, evaluate as normal
                df_derived_measure = eval(final_formula)
        else:
            # Not a DataFrame-like string, evaluate as normal
            df_derived_measure = eval(final_formula)
    except ZeroDivisionError:
        df_derived_measure = 0  # Assign a default value
#     print(f'df_derived_measure:{df_derived_measure}')

    
        
#     print(f'df_grouped:\n{df_grouped}')
    if isinstance(df_derived_measure, pd.Series):
        df_derived_measure = df_derived_measure.to_frame(name=meas)
    if extra_groupby_col:
        return df_derived_measure.reset_index(drop=False)
    else:
        return df_derived_measure
