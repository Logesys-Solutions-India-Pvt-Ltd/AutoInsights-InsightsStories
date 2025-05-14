# FinalCharts.py

from FinalCommon import *

# --- Charts ---
def df_to_json_with_duplicated(data, col1, col2='null'):
    st = '{"' + col1 + '":{'
    for i in range(data.shape[0]):
        st = st + '"' + str(data.index[i]) + '": "' + str(data.iloc[i][col1]) + '",'
    st = st[:-2] + '"}}'
    st1 = ''
    if col2 != 'null':
        st = st[:-1] + ','
        st1 = '"' + col2 + '":{'
        for i in range(data.shape[0]):
            st1 = st1 + '"' + str(data.index[i]) + '": "' + str(data.iloc[i][col2]) + '",'
        st1 = st1[:-2] + '"}}'
    return st + st1

# Note: the below line chart to be removed in future and change LineChartNew to LineChart
def LineChart(data, highlight_columns, non_highlight_columns, xAxisTitle = '' , yAxisTitle = '', chart_title = '', chartSubTitle = '', chartFooterTitle = '', mapping = '', non_highlight_color = '#FEB5B5', highlight_color = '#FF9E00'):
    chartSubTitle = chartSubTitle.replace(r'"','\\"')
    chart_title = chart_title.replace(r'"','\\"')
    chartFooterTitle = chartFooterTitle.replace(r'"','\\"')
    columns = data.columns
    for nh_columns in non_highlight_columns:
        nh_columns = nh_columns.replace(r'"','\\"')
        data[nh_columns] = '{\"data\":' + data[nh_columns].astype(str) + ', \"style\": \"color: ' + non_highlight_color + '\"}'
    for h_columns in highlight_columns:
        h_columns = h_columns.replace(r'"','\\"')
        data[h_columns] = '{\"data\":' + data[h_columns].astype(str) + ',\"highlight\":\"true\" , \"style\": \"color: ' + highlight_color + '\"}'
    if (mapping != ''):
        data.index = data.index.map(mapping)
    ChartData = data_string + data.to_json() + ', '+ config
    for h_columns in highlight_columns:
        h_columns = h_columns.replace(r'"','\\"')
        ChartData = ChartData + h_columns + show_markers + solid_line + ',"color":"' + highlight_color + '"}' + ',"'
    for nh_columns in non_highlight_columns:
        nh_columns = nh_columns.replace(r'"','\\"')
        ChartData = ChartData + nh_columns + no_markers + dashed_line + ',"color":"' + non_highlight_color + '"}' 
    if len(non_highlight_columns) == 0:
        ChartData = ChartData[:-2]
    ChartData = ChartData + '}' + xAxisTitle_start + xAxisTitle + xAxisTitle_end + yAxisTitle_start + yAxisTitle + yAxisTitle_end + chartTitle_start + chart_title + chartTitle_end + chartSubTitle_start + chartSubTitle + chartSubTitle_end + chartFooterTitle_start + chartFooterTitle + chartFooterTitle_end 
    return ChartData


def LineChartNew(data, highlight_columns, non_highlight_columns, xAxisTitle='', yAxisTitle='', chart_title='', chartSubTitle='', chartFooterTitle='', mapping='', highlight_color='#FF9E00', non_highlight_color='#FEB5B5', style=False, config_string='', tooltiptext=False):
    chartSubTitle = chartSubTitle.replace(r'\"', '\\\\\"')
    chart_title = chart_title.replace(r'\"', '\\\\\"')
    chartFooterTitle = chartFooterTitle.replace(r'\"', '\\\\\"')
    if mapping != '':
        data.index = data.index.map(mapping)
    ChartData = data_string + data.to_json() + ', ' + config
    if tooltiptext == False:
        for h_columns in highlight_columns:
            h_columns = h_columns.replace(r'\"', '\\\\\"')
            ChartData = ChartData + h_columns + show_markers + solid_line + solid_color + ',"'
        for nh_columns in non_highlight_columns:
            nh_columns = nh_columns.replace(r'\"', '\\\\\"')
            ChartData = ChartData + nh_columns + no_markers + dashed_line + dashed_color
        if len(non_highlight_columns) == 0:
            ChartData = ChartData[:-2]
    else:
        ChartData = ChartData + config_string[:-2]
    ChartData = ChartData + '}' + xAxisTitle_start + xAxisTitle + xAxisTitle_end + yAxisTitle_start + yAxisTitle + yAxisTitle_end + chartTitle_start + chart_title + chartTitle_end + chartSubTitle_start + chartSubTitle + chartSubTitle_end + chartFooterTitle_start + chartFooterTitle + chartFooterTitle_end
    return ChartData

def ParetoChart(data, condition_columns, non_condition_columns, value='', xAxisTitle='', yAxisTitle='', chart_title='', chartSubTitle='', chartFooterTitle='', mapping='', def_color='#FEE2B5', highlight_color='#FF9E00', yAxisTitleRight=''):
    chartSubTitle = chartSubTitle.replace(r'\"', '\\\\\"')
    chart_title = chart_title.replace(r'\"', '\\\\\"')
    chartFooterTitle = chartFooterTitle.replace(r'\"', '\\\\\"')
    c_columns = condition_columns[0]
    nc_columns = non_condition_columns[0]
    data.loc[data[c_columns] <= value, c_columns + ' %'] = '{\\\"data\\\":' + data[c_columns].astype(str) + ',\\\"highlight\\\":\\\"true\\\" , \\\"style\\\": \\\"color: #FF0000\\\"}'
    data.loc[data[c_columns] > value, c_columns + ' %'] = '{\\\"data\\\":' + data[c_columns].astype(str) + ', \\\"style\\\": \\\"color: #FEB5B5\\\"}'
    data.loc[data[c_columns] <= value, nc_columns] = '{\\\"data\\\":' + data[nc_columns].astype(str) + ',\\\"highlight\\\":\\\"true\\\" , \\\"style\\\": \\\"color: ' + highlight_color + '\\\"}'
    data.loc[data[c_columns] > value, nc_columns] = '{\\\"data\\\":' + data[nc_columns].astype(str) + ', \\\"style\\\": \\\"color: ' + def_color + '\\\"}'
    data.drop(c_columns, inplace=True, axis=1)
    ChartData = data_string + data.to_json() + ', ' + config
    for nc_columns in non_condition_columns:
        ChartData = ChartData + nc_columns + bar_false + ',"'
    for c_columns in condition_columns:
        ChartData = ChartData + c_columns + ' %' + solid_line_true
    ChartData = ChartData + xAxisTitle_start + xAxisTitle + xAxisTitle_end + yAxisTitle_start + yAxisTitle + yAxisTitle_end + yAxisTitleStart_right + yAxisTitleRight + yAxisTitle_end + chartTitle_start + chart_title + chartTitle_end + chartSubTitle_start + chartSubTitle + chartSubTitle_end + chartFooterTitle_start + chartFooterTitle + chartFooterTitle_end
    return ChartData

def TableChart(data):
    chartSubTitle = chartSubTitle.replace(r'\"', '\\\\\"')
    chart_title = chart_title.replace(r'\"', '\\\\\"')
    chartFooterTitle = chartFooterTitle.replace(r'\"', '\\\\\"')
    dict_data = json.loads(data.T.to_json())
    table_data = []
    for key, value in dict_data.items():
        table_data.append(dict_data[key])
    table_data = str(table_data).replace("'", '"')
    ChartData = '{"data": ' + str(table_data) + ',' + '"config": {"columns": ' + str(list(data.columns)).replace("'", '"') + ',"stickedColumn": ' + str(list(data.columns)).replace("'", '"') + ',"allowSorting": true}}'
    return ChartData

def ScatterChart(data, scatter_columns, line_columns, meas='', xAxisTitle='', yAxisTitle='', chart_title='', chartSubTitle='', chartFooterTitle='', mapping='', def_color='#FEE2B5', highlight_color='#FF9E00'):
    chartSubTitle = chartSubTitle.replace(r'\"', '\\\\\"')
    chart_title = chart_title.replace(r'\"', '\\\\\"')
    chartFooterTitle = chartFooterTitle.replace(r'\"', '\\\\\"')
    s_column = scatter_columns[0]
    l_column = line_columns[0]
    data[s_column] = '{\\\\\"data\\\\\" :' + data[s_column].astype(str) + ',\\\\\"style\\\\\":\\\\\"color:' + highlight_color + '\\\\\", \\\\\"tooltipText\\\\\":\\\\\"' + s_column + ': <b>' + data[s_column].astype(str) + '</b><br>' + meas + ': <b>' + data.index.astype(str) + '</b>\\\\\"}'
    data[l_column] = '{\\\\\"data\\\\\" :' + data[l_column].astype(str) + ',\\\\\"style\\\\\":\\\\\"color:' + def_color + '\\\\\", \\\\\"tooltipText\\\\\":\\\\\"' + 'Trend: <b>' + data[l_column].astype(str) + '</b>' + '\\\\\"}'
    data = df_to_json_with_duplicated(data, s_column, l_column)
    ChartData = data_string + data + ', ' + config + l_column + '":{ "type":"line","rightAxis":false' + color + ', "dynamicTooltip": true},"' + s_column + scatter_false + color + dynamicTooltip_true + xAxisTitle_start + xAxisTitle + xAxisTitle_end + yAxisTitle_start + yAxisTitle + yAxisTitle_end + chartTitle_start + chart_title + chartTitle_end + chartSubTitle_start + chartSubTitle + chartSubTitle_end + scattered_true + chartFooterTitle_start + chartFooterTitle + chartFooterTitle_end
    return ChartData


def ComboChart(df, bar_columns , dashed_line_columns, solid_line_columns = [], xAxisTitle = '' , yAxisTitle = '', chart_title = '', chartSubTitle = '', chartFooterTitle = '', yAxisTitleRight = '', scatter_columns = []):
    chartSubTitle = chartSubTitle.replace(r'"','\\"')
    chart_title = chart_title.replace(r'"','\\"')
    chartFooterTitle = chartFooterTitle.replace(r'"','\\"')
    ChartData = data_string + df.to_json() + ', '+ config
    for b_column in bar_columns:
        b_column = b_column.replace(r'"','\\"')
        ChartData = ChartData + b_column + bar_false + ',"'
    for l_column in dashed_line_columns:
        l_column = l_column.replace(r'"','\\"')
        ChartData = ChartData + l_column + dashed_line_false[:-1] + ',"'
    for l_column in solid_line_columns:
        l_column = l_column.replace(r'"','\\"')
        ChartData = ChartData + l_column + solid_line_true[:-1] + ',"'
    for s_column in scatter_columns:
        s_column = s_column.replace(r'"','\\"')
        ChartData = ChartData + s_column + scatter_false + '}' + ',"'
    ChartData = ChartData[:-2] + '}'
    if len(scatter_columns) > 0:
        scatter = scattered_true
    else:
        scatter = ''
    ChartData = ChartData + xAxisTitle_start + xAxisTitle + xAxisTitle_end + yAxisTitle_start + yAxisTitle + yAxisTitle_end + yAxisTitleStart_right + yAxisTitleRight + yAxisTitle_end + chartTitle_start + chart_title + chartTitle_end + chartSubTitle_start + chartSubTitle + chartSubTitle_end + scatter + chartFooterTitle_start + chartFooterTitle + chartFooterTitle_end 
    return ChartData


def waterfallChart(dim, meas, DiffVal, xAxisTitle = '' , yAxisTitle = '', chart_title = '', chartSubTitle = '', chartFooterTitle = '',ty_meas = None, ly_meas = None):
    chartSubTitle = chartSubTitle.replace(r'"','\\"')
    chart_title = chart_title.replace(r'"','\\"')
    chartFooterTitle = chartFooterTitle.replace(r'"','\\"')
    waterfall = pd.DataFrame()
    waterfall =  (DiffVal)

    waterfall = round(pd.DataFrame({meas : ly_meas , meas + '2' : ''} , index=['Last Year']).append(waterfall.append(pd.DataFrame({meas : ty_meas} , index=['This Year']))),2)
    waterfall[meas] = waterfall[meas].cumsum().to_frame()

    waterfall[meas + '2'].iloc[0] = 0
    for i in range(0,waterfall[meas].count()-1):    
        waterfall[meas + '2'].iloc[i+1] = waterfall.iloc[i][meas]
    waterfall[meas].iloc[waterfall[meas].count()-1] = 0
    waterfall = waterfall[[meas + '2',meas]]
    waterfall.columns = [meas , meas + '2']

    interchange1 = waterfall.loc['This Year'][meas+'2']
    interchange2 = waterfall.loc['This Year'][meas]
    waterfall[meas].loc['This Year'] = interchange1
    waterfall[meas+'2'].loc['This Year'] = interchange2

    ly_value = waterfall[meas + '2'].loc['Last Year']
    ty_value = waterfall[meas + '2'].loc['This Year']

    if ly_value < ty_value:
        minimum_value = ly_value/2
    else:
        minimum_value = ty_value/2
#     print(minimum_value)
    if str(minimum_value) == 'nan':
        minimum_value = 0

    waterfall[meas] = waterfall[meas].apply(lambda x: round(x,2))
    waterfall[meas + '2'] = waterfall[meas + '2'].apply(lambda x: round(x,2))

    waterfall[meas + '1'] = waterfall[meas]
    waterfall[meas + '3'] = waterfall[meas + '2']
    waterfall = waterfall[[meas,meas + '1' , meas + '2',meas + '3']]
    waterfall.columns = ['0','1','2','3']

    waterfall = '{"' + meas + '": ' + str(waterfall.T.to_json()) + '}'

    waterfall = data_string + waterfall + ', '+ config + meas + waterfall_false + dynamicTooltip_true + xAxisTitle_start + xAxisTitle + xAxisTitle_end + yAxisTitle_start + yAxisTitle + yAxisTitle_end + chartTitle_start + chart_title + chartTitle_end + chartSubTitle_start + chartSubTitle + chartSubTitle_end +raising_color +  falling_color + min_value + str(minimum_value) + chartFooterTitle_start + chartFooterTitle + chartFooterTitle_end 
    return waterfall


def BarChart(df, bar_columns, xAxisTitle = '' , yAxisTitle = '', chart_title = '', chartSubTitle = '', chartFooterTitle = ''):
    chartSubTitle = chartSubTitle.replace(r'"','\\"')
    chart_title = chart_title.replace(r'"','\\"')
    chartFooterTitle = chartFooterTitle.replace(r'"','\\"')
    # Pls provide as list
    ChartData = data_string + df.to_json() + ', '+ config
    for b_column in bar_columns:
        ChartData = ChartData + b_column + bar_false
#     ChartData = ChartData + '"'   
#     ChartData = ChartData[:-1] 
    ChartData = ChartData + '}'
    ChartData = ChartData + xAxisTitle_start + xAxisTitle + xAxisTitle_end + yAxisTitle_start + yAxisTitle + yAxisTitle_end + chartTitle_start + chart_title + chartTitle_end + chartSubTitle_start + chartSubTitle + chartSubTitle_end + chartFooterTitle_start + chartFooterTitle + chartFooterTitle_end 
    return ChartData