from pyspark.sql.types import *
from pyspark.sql.functions import *

typeConverter = {
    "int32" : LongType(),
    "int64" : LongType(),
    "int16" : LongType(),
    "double" : DoubleType(),
    "string" : StringType(),
    "array" : ArrayType(StringType()),
    "boolean" : BooleanType()
}

def read_data(spark,file_path,type):
    df = spark.read.format(type).schema(getschema(file_path,spark)).load(file_path)
    return df

def getschema(file_path,spark):
    schema = [row.asDict(recursive=True) for row in
              spark.read.json(file_path).limit(1).select("schema").distinct().collect()]
    payload_col = []
    for column_details in schema[0]["schema"]["fields"]:
        column_name = column_details["field"].replace(" ", "_")
        if type(column_details["fields"]) == list:
            nested_col_list = []
            for nested_col in column_details["fields"]:
                nested_col_list.append(StructField(nested_col["field"].replace(" ", "_"),
                                                   typeConverter[nested_col["type"]], True))
            payload_col.append(StructField(column_name, StructType(nested_col_list), True))
        else:
            payload_col.append(StructField(column_name, typeConverter[column_details["type"]], True))
        pay_load_schema = StructType(payload_col)
        json_schema = StructType([StructField("payload", pay_load_schema, True), StructField("schema", StringType(),
                                                                                             True),
                                  StructField("year", LongType(), True), StructField("month", LongType(), True),
                                  StructField("day", LongType(), True), StructField("hour", LongType(), True)])
    return json_schema

def clean_bronze_data(df):
    source_view = df.select("payload.*","year","month","day","hour")
    payload_df = source_view.withColumn("cdc_data",
                                        when(col("op") == "d", col("before"))
                                        .otherwise((col("after"))))
    final_df = payload_df.select("cdc_data.*",col("op").alias("cdc_operation"),
                                 col("source.connector").alias("cdc_source_connector"),
                                 col("source.db").alias("cdc_source_db"), col("source.table").alias("cdc_source_table"),
                                 col("ts_ms").alias("cdc_ts_ms"),current_timestamp().alias("bronze_ts"))
    return final_df

def transform_ts_ms(df):
    transformed_df = df.withColumn("cdc_ts_ms",(col("cdc_ts_ms")/1000).cast("timestamp"))
    return transformed_df
