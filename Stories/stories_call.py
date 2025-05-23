from Stories.stories_avg_cy_ly import stories_avg_cy_ly
from Stories.stories_x_times import stories_x_times
from Stories.stories_rank_cy_ly import stories_rank_cy_ly
import constants

################################################## Stories Call ##########################################

def stories_call():
    derived_measures_dict = constants.DERIVED_MEASURES_DICT
    

    print('Generating stories.')
    importance = 1
    for meas in list(derived_measures_dict.keys()):
    # for meas in ['Markdown %', 'ASP', 'ATV', 'UPT']: ## Timesquare ##
        importance += 1
        print('---------------')
        print('Stories - Average CY LY')
        stories_avg_cy_ly(meas, importance)

    importance = 1
    for meas in list(derived_measures_dict.keys()):
    # for meas in ['Markdown %', 'ASP', 'ATV', 'UPT']: ## Timesquare ##
        importance += 1
        print('---------------')
        print('Stories - X times')
        stories_x_times(meas, importance)


    importance = 1
    for meas in list(derived_measures_dict.keys()):
    # for meas in ['Markdown %', 'ASP', 'ATV', 'UPT']: ## Timesquare ##
        importance += 1
        print('---------------')
        print('Stories - Rank CY LY')
        stories_rank_cy_ly(meas, importance)