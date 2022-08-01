import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from lib.utils import get_spark_app_config
from functions.transformations import *
from dateutil import parser

@pytest.fixture(scope="session")
def spark_session():
    # conf= get_spark_app_config()
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    return spark

@pytest.mark.usefixtures("spark_session")
def test_transform_tsms(spark_session):

    timestamp_value = "2022-07-27 22:09:18.791513"
    test_timestamp_value = "2022-07-14 15:22:10.062"
    parsed_value = parser.parse(timestamp_value)
    test_parsed_value = parser.parse(test_timestamp_value)

    data = [(106,'ganapathi','d','mysql','ef_fulfillment','customer',1657792330062,parsed_value)]

    schema = StructType([
        StructField("cus_id",LongType(),True),
        StructField("cus_name",StringType(),True),
        StructField("cdc_operation",StringType(),True),
        StructField("cdc_source_connector", StringType(), True),
        StructField("cdc_source_db", StringType(), True),
        StructField("cdc_source_table", StringType(), True),
        StructField("cdc_ts_ms", LongType(), True),
        StructField("bronze_ts", TimestampType(), True)
        ])

    df = spark_session.createDataFrame(data=data,schema=schema)

    transformed_ts_df = transform_ts_ms(df)
    # value = transformed_ts_df.first().cdc_source_db
    # transformed_ts_df.first().cdc_ts_ms
    assert  transformed_ts_df.toPandas().to_dict('list')['cdc_ts_ms'][0] == test_parsed_value



