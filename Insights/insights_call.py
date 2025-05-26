from Insights.hi_pots import hi_pots
from Insights.movements import movements
from Insights.delta_analysis import delta_analysis
from Insights.rank_analysis import rank_analysis
from Insights.new_entrants import new_entrants
from Insights.trends import trends
from Insights.monthly_anomalies import monthly_anomalies
from Insights.weekly_anomalies import weekly_anomalies
from Insights.outliers import outliers
import threading
import constants


### Without multithreading ###
def insights_call():
    constants.logger.info('Generating Insights.')
    selected_insights = constants.SELECTED_INSIGHTS
    insights_to_skip = constants.INSIGHTS_TO_SKIP
    dim_allowed_for_derived_metrics = constants.DIM_ALLOWED_FOR_DERIVED_METRICS
    insights_allowed_for_derived_metrics = constants.INSIGHTS_ALLOWED_FOR_DERIVED_METRICS
    derived_measures_dict = constants.DERIVED_MEASURES_DICT
    Significant_dimensions = constants.SIGNIFICANT_DIMENSIONS
    
    for meas in list(derived_measures_dict.keys()):  
        allowed_dims_for_meas = dim_allowed_for_derived_metrics[meas]
        allowed_insights_for_meas = insights_allowed_for_derived_metrics[meas] 

        for dim_table, dim_list in Significant_dimensions.items():  
            for dim in dim_list:  
                if dim in allowed_dims_for_meas:  # Check if the dimension is allowed for the current measure.
                    # print(f'dim:{dim}, meas{meas}')
                    for insight_name in selected_insights:  # Iterate through the selected insights.
                        if insight_name in insights_to_skip:
                            continue
                        
                        if insight_name in allowed_insights_for_meas:
                            if insight_name == 'Hi-Pots':
                                hi_pots(dim_table, dim, meas)  
                            if insight_name == 'Movements':
                                movements(dim_table, dim, meas)
                            elif insight_name == 'Rank Analysis':
                                rank_analysis(dim_table, dim, meas)
                            elif insight_name == 'Delta Analysis':
                                delta_analysis(dim_table, dim, meas)
                            elif insight_name == 'New Entrants':
                                new_entrants(dim_table, dim, meas)


    for insight_name in selected_insights:
        if insight_name == 'Trends':
            trends()
        elif insight_name == 'Monthly Anomalies':
            monthly_anomalies()
        elif insight_name == 'Weekly Anomalies':
            weekly_anomalies()
        elif insight_name == 'Outliers':
            outliers()


### With multithreading
def insights_call_threaded():
    selected_insights = constants.SELECTED_INSIGHTS
    insights_to_skip = constants.INSIGHTS_TO_SKIP
    dim_allowed_for_derived_metrics = constants.DIM_ALLOWED_FOR_DERIVED_METRICS
    insights_allowed_for_derived_metrics = constants.INSIGHTS_ALLOWED_FOR_DERIVED_METRICS
    derived_measures_dict = constants.DERIVED_MEASURES_DICT
    Significant_dimensions = constants.SIGNIFICANT_DIMENSIONS
    threads = []

    # Threading for dimension and measure-based insights
    for dim_table, dim_list in Significant_dimensions.items():
        for dim in dim_list:
            for meas in list(derived_measures_dict.keys()):
                if dim in dim_allowed_for_derived_metrics[meas]:
                    for insight_name in selected_insights:
                        if insight_name in insights_to_skip:
                            continue

                        if insight_name == 'Hi-Pots':
                            thread = threading.Thread(target=hi_pots, args=(dim_table, dim, meas))
                            threads.append(thread)
                            thread.start()
                        if meas != 'Stock Cover':
                            if insight_name == 'Movements':
                                thread = threading.Thread(target=movements, args=(dim_table, dim, meas))
                                threads.append(thread)
                                thread.start()
                            elif insight_name == 'Rank Analysis':
                                thread = threading.Thread(target=rank_analysis, args=(dim_table, dim, meas))
                                threads.append(thread)
                                thread.start()
                            elif insight_name == 'Delta Analysis':
                                thread = threading.Thread(target=delta_analysis, args=(dim_table, dim, meas))
                                threads.append(thread)
                                thread.start()
                            elif insight_name == 'New Entrants':
                                thread = threading.Thread(target=new_entrants, args=(dim_table, dim, meas))
                                threads.append(thread)
                                thread.start()

    # Threading for other insights
    for insight_name in selected_insights:
        if insight_name == 'Trends':
            thread = threading.Thread(target=trends)
            threads.append(thread)
            thread.start()
        elif insight_name == 'Monthly Anomalies':
            thread = threading.Thread(target=monthly_anomalies)
            threads.append(thread)
            thread.start()
        elif insight_name == 'Weekly Anomalies':
            thread = threading.Thread(target=weekly_anomalies)
            threads.append(thread)
            thread.start()
        elif insight_name == 'Outliers':
            thread = threading.Thread(target=outliers)
            threads.append(thread)
            thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    print("All insight generation threads have finished.") # Optional: Confirmation message
