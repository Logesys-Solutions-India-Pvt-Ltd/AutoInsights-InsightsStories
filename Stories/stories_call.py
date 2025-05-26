from Stories.stories_avg_cy_ly import stories_avg_cy_ly
from Stories.stories_x_times import stories_x_times
from Stories.stories_rank_cy_ly import stories_rank_cy_ly
import constants


def stories_call():
    derived_measures_dict = constants.DERIVED_MEASURES_DICT
    constants.logger.info('Generating stories.')
    importance = 1
    for meas in list(derived_measures_dict.keys()):
    # for meas in ['Markdown %', 'ASP', 'ATV', 'UPT']: ## Timesquare ##
        importance += 1
        constants.logger.info('Stories - Average CY LY')
        stories_avg_cy_ly(meas, importance)

    importance = 1
    for meas in list(derived_measures_dict.keys()):
    # for meas in ['Markdown %', 'ASP', 'ATV', 'UPT']: ## Timesquare ##
        importance += 1
        constants.logger.info('Stories - X times')
        stories_x_times(meas, importance)


    importance = 1
    for meas in list(derived_measures_dict.keys()):
    # for meas in ['Markdown %', 'ASP', 'ATV', 'UPT']: ## Timesquare ##
        importance += 1
        constants.logger.info('Stories - Rank CY LY')
        stories_rank_cy_ly(meas, importance)