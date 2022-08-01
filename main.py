from pyspark.sql import *
from lib.utils import get_spark_app_config
from functions.transformations import *
from dateutil import parser
import pandas



if __name__ == "__main__":

    conf= get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    df = read_data(spark,sys.argv[1],"json")
    cleaned_df = clean_bronze_data(df)
    date_change_df = transform_ts_ms(cleaned_df)
    date_change_df.show(1)



