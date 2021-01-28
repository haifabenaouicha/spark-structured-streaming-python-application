####################
# Author(s): haifa ben aouicha
####################

from source.hour_indicator import GenericConsumptionPerHour

if __name__ == '__main__':

    hourly_indicator_with_first_threshold = GenericConsumptionPerHour("GENERIC_HOUR_CONSUMPTION_WITH_5000_THRESHOLD",
                                                                      "indicator", None)
    hourly_indicator_with_second_threshold = GenericConsumptionPerHour("GENERIC_HOUR_CONSUMPTION_WITH_4000_THRESHOLD",
                                                                       "second_indicator", None)
    kwargs = hourly_indicator_with_first_threshold.parse_args()
    hourly_indicator_with_first_threshold.execute(**kwargs)
    hourly_indicator_with_second_threshold.execute(**kwargs)

















