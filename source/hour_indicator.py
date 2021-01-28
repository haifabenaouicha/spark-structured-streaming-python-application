####################
# Author(s): haifa ben aouicha
####################
import argparse

import pyspark.sql.functions as F
from pyhocon import ConfigFactory
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

from entities.abstract import Execute
from entities.base_execute import BaseExecute
from source.schema import IndicatorHourConsumption


class GenericConsumptionPerHour(BaseExecute, Execute):
    def __init__(self, indicator_name, reference,  spark=None):
        super().__init__(spark)
        self.indicator_name = indicator_name
        self.reference = reference

    def parse_args(self):
        self.logger.info("Parsing arguments")
        parser = argparse.ArgumentParser()
        parser.add_argument("conf_path", type=str,
                            help="Path of the params.json")

        args = parser.parse_args()
        args_dict = args.__dict__
        self.logger.info("Arguments retrieved: {}".format(args_dict))
        return args_dict

    def execute(self, conf_path):
        """
        :param conf_path: In which path the software can find the params.json
        :return: Nothing, but save the output data resulting from computation
       """
        self.logger.info("loading configuration from conf file hosted in databricks file system")
        conf = ConfigFactory.parse_file(conf_path)
        self.logger.info("loading json indicator file")

        self.logger.info("read stream indicator files")

        indicator_nested_df = self.spark.readStream.format(
            conf.get_string("streamIndicatorJob.{}.format".format(self.reference)))\
            .option("multiLine","True").schema(IndicatorHourConsumption.schema).load(
            conf.get_string("streamIndicatorJob.{}.input".format(self.reference)))

        self.logger.info("faltten indicator files")

        indicator_flattened_df = indicator_nested_df.selectExpr(IndicatorHourConsumption.selected_field).withColumn(
            "hour_begin", hour("hoursBegin")).withColumn("hour_end", hour("hoursEnd")).drop("hoursBegin", "hoursEnd")
        self.logger.info("add pid from counterid field")
        indicator_with_pid = indicator_flattened_df.withColumn("point_id", json_tuple(indicator_flattened_df.counterid, 'pid'))
        # csv schema of iot data
        csv_schema = StructType([StructField("company_id", StringType()),
                                StructField("date", StringType()), StructField("directory_id", StringType()),
                                StructField("gran", StringType()), StructField("measure_name", StringType()),
                                 StructField("point_id", StringType()), StructField("value", StringType()),
                                 StructField("extractedAt", StringType()), StructField("datetime", StringType())])
        self.logger.info("read stream iot data and add hour and day columns")
        data_df = self.spark.readStream.format(conf.get_string("streamIndicatorJob.data.format"))\
            .option("header", "true")\
            .option("inferSchema", "true").schema(csv_schema)\
            .load(conf.get_string("streamIndicatorJob.data.input")).withColumn("hour", hour("datetime"))\
            .withColumn("day", date_format("date", 'EEEE'))

        self.logger.info("add processing time to iot data")
        data_with_time = data_df.withColumn("processing_time", F.lit(current_timestamp()))

        self.logger.info("add watermark to iot data , window is configurable here")
        indicator_with_watermark = data_with_time.withWatermark("processing_time", "1 second")
        self.logger.info("join indicator dataframe with iot dataframe")
        joined_df = indicator_with_watermark.join(broadcast(indicator_with_pid), ['company_id', 'directory_id',
                                                                                  'point_id'], "inner")
        self.logger.info("filter the resulting dataframe "
                         "based on the hours slot/days slot defined in indocator dataframe")

        filtered_df = joined_df.filter((joined_df.hour >= joined_df.hour_begin) | (joined_df.hour <= joined_df.hour_end)).filter((joined_df.day == "Monday") | (joined_df.day == "Tuesday") | (joined_df.day == "Wednesday")
                    | (joined_df.day == "Tuesday") | (joined_df.day == "Friday"))
        self.logger.info("compute alert based on threshold from indicator dataframe")
        with_alert = filtered_df.withColumn("alert", when(filtered_df.value >= filtered_df.threshold, True)
                                            .otherwise(False)).withColumn("output", F.lit(self.indicator_name))
        self.logger.info("select columns to be written to the output files")
        output = with_alert.filter(with_alert.alert == True).select("output", "datetime", "processing_time",
                                                                    "company_id", "directory_id", "point_id", "value")
        self.logger.info("step8")
        query = output.writeStream.format("parquet").option("checkpointLocation",
                                                            conf.get_string("streamIndicatorJob.{}.checkpoint"
                                                                            .format(self.reference)))\
            .option("path", conf.get_string("streamIndicatorJob.data.output")).outputMode("append")\
            .start().awaitTermination()
        self.logger.info("step9")


