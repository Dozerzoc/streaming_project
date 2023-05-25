import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, lit
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, DoubleType

# Create a SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('spark-batch-processing') \
    .config("spark.jars.packages",
            "org.apache.spark:spark-avro_2.12:3.2.3") \
    .config("spark.es.nodes", "localhost") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.driver.memory", "15g") \
    .getOrCreate()

# Read the input DataFrame
stream_schema = StructType([
    StructField('hotel_id', LongType(), True),
    StructField('with_children', IntegerType(), True),
    StructField('cnt_erroneous_data', IntegerType(), True),
    StructField('cnt_short_stay', IntegerType(), True),
    StructField('cnt_standard_stay', IntegerType(), True),
    StructField('cnt_standard_extended_stay', IntegerType(), True),
    StructField('cnt_long_stay', IntegerType(), True),
    StructField('most_popular_stay_type', StringType(), False)

])

df = spark \
    .readStream \
    .format("avro") \
    .schema(stream_schema) \
    .load('../datasets/hotels_aggregated')

df = df.withColumn("current_timestamp", lit(str(datetime.datetime.now())).cast("timestamp"))
df_with_watermark = df.withWatermark("current_timestamp", "10 seconds")
df_with_window = df_with_watermark.withColumn("window", window(col("current_timestamp"), "1 minute", "30 seconds"))
query = df_with_window \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .start()

query.awaitTermination(20)
query.processAllAvailable()
query.stop()
spark.stop()
