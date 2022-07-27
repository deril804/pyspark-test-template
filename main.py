from pyspark.sql import *
from lib.utils import get_spark_app_config


if __name__ == "__main__":

    conf= get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()








