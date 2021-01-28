####################
# Author(s): haifa ben aouicha
####################

from pyspark.sql.types import StringType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


class IndicatorHourConsumption(object):
    schema = StructType([
        StructField("data", StructType([
            StructField("indicator", StructType([
                StructField("indicator", StructType([
                    StructField("id", StringType()),
                    StructField("name", StringType()),
                    StructField("link", StructType([
                        StructField("directoryId", StringType()),
                        StructField("companyId", StringType())])),
                    StructField("company", StructType([
                        StructField("id", StringType())])),
                    StructField("directory", StructType([
                        StructField("id", StringType())])),
                    StructField("active", BooleanType()),
                    StructField("lastCheck", StringType()),
                    StructField("root", StructType([
                        StructField("type", StringType()),
                        StructField("name", StringType()),
                        StructField("opts", ArrayType(
                            StructType([StructField("REF", StringType()),
                                        StructField("TYPE", StringType()),
                                        StructField("VALUE", StructType([
                                            StructField("name", StringType()),
                                            StructField("opts", ArrayType(
                                                StructType([StructField("REF", StringType()),
                                                            StructField("TYPE", StringType()),
                                                            StructField("VALUE", StringType())])))
                                        ]))])))])),
                    StructField("type", StringType()),
                    StructField("windowEnd", StringType()),
                    StructField("windowStart", StringType())
                ]))]))]))])

    root = "data.indicator.indicator"

    selected_field = ["{}.id as id".format(root),
                      "{}.name as name".format(root),
                      "{}.link.directoryId as directory_id".format(root),
                      "{}.link.companyId as company_id".format(root),
                      "{}.active as activeState".format(root),
                      "{}.lastCheck as lastCheck".format(root),
                      "{}.root.type as type".format(root),
                      "{}.root.opts[0].VALUE.opts[0].VALUE as dayOfWeek".format(root),
                      "{}.root.opts[1].VALUE.opts[0].VALUE as hoursBegin".format(root),
                      "{}.root.opts[2].VALUE.opts[0].VALUE as hoursEnd".format(root),
                      "{}.root.opts[3].VALUE.opts[0].VALUE as counterid".format(root),
                      "{}.root.opts[4].VALUE.opts[0].VALUE as thresholdField".format(root),
                      "{}.root.opts[5].VALUE.opts[0].VALUE as threshold".format(root),
                      "{}.root.opts[6].VALUE.opts[0].VALUE as siteName".format(root),
                      "{}.type as periodicityType".format(root),
                      "{}.windowStart as windowStart".format(root),
                      "{}.windowEnd as windowEnd".format(root)]