####################
# Author(s): haifa ben aouicha
####################

import logging
import traceback
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import DataFrameReader
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StructType

from utils import utils as uts


class BaseExecute:
    def __init__(self, spark_session=None):
        try:
            # Instantiate logger
            self.logger = uts.create_logger_basic()
            logging.getLogger("py4j").setLevel(logging.ERROR)
            self.logger.info("Initialization of spark & logging done")

            # Instantiate Spark Session
            self.spark = spark_session
            if not self.spark:
                self.spark = SparkSession.builder .config("spark.sql.debug.maxToStringFields", 1000).appName("SparkTest") \
                    .getOrCreate()
        except Exception as e:
            self.logger.error("Initialization of spark & logging couldn't be performed: {}".format(e),
                              traceback.format_exc())
    def spark_reader(self, path: str, data_format: str,
                     key, value, schema: Optional[StructType]=None) -> DataFrame:

        if not schema:
            data_reader = self.spark.read.format(data_format).option(key,value)
        else:
            data_reader = self.spark.read.format(data_format).schema(schema).option(key,value)
        try:
            return data_reader.load(path)
        except Exception as e:
            logging.error("reading  data with spark couldn't be performed as expected: {}".format(e),
                          traceback.format_exc())

    def spark_stream_reader(self, path, data_format, key, value, schema) -> DataFrame:

        data_stream_reader = self.spark.readStream.format(data_format).option(key,value).schema(schema)
        try:
            return data_stream_reader.load(path)

        except Exception as e:
            logging.error("reading stream of data with spark couldn't be performed as expected: {}".format(e),
                          traceback.format_exc())



    def spark_writer(self, df: DataFrame, path, data_format, key, value,
                     partitions: Optional[list] = None):
        try:
            if not partitions:
                df.write.format(data_format).option(key, value)
            else:
                df.write.format(data_format).option(key, value).partitionBy(partitions)
        except Exception as e:
            logging.error("writing data with spark couldn't be performed as expected: {}".format(e),
                              traceback.format_exc())

    def spark_stream_writer(self, df: DataFrame, path, data_format, key, value,
                            partitions: Optional[list] = None):

        if not partitions:
           dt = df.coalesce(1).writeStream.format(data_format).option(key, value)
        else:
            dt = df.coalesce(1).writeStream.format(data_format).option(key, value).partitionBy(partitions)
        try:
            return dt.save(path)
        except Exception as e:
            logging.error("writing stream data with spark couldn't be performed as expected: {}".format(e),
                              traceback.format_exc())

