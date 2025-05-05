start_month = 1
end_month = 12

df_sql_meas_functions = {
    'sum()': 'SUM',
    'mean()': 'AVG'
}

rename_dim_meas = {
    'CRANE_MINS': 'Crane Mins',
    'BERTH_MINS': 'Berth Mins',
    'TEUS': 'TEUs',
    'BERTH_PROD': 'Berth Prod',
    'VESSEL_NAME': 'Vessel Name',
    'SERVICE_ID': 'Service ID'
}

# dim_allowed_for_derived_metrics = {
#      'Markdown %': [dim for dims in Significant_dimensions.values() for dim in dims],
#      'ASP': [dim for dims in Significant_dimensions.values() for dim in dims],
#      'Stock Cover': [dim for dims in Significant_dimensions.values() for dim in dims],
#      'ATV': [dim for dim in Significant_dimensions['df_location_master']
#              if dim in ['Store Name', 'Region', 'Business', 'Mall Name', 'Territory']],
#      'UPT': [dim for dim in Significant_dimensions['df_location_master']
#              if dim in ['Store Name', 'Region', 'Business', 'Mall Name', 'Territory']],
#  }