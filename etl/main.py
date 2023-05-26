import datetime
from elasticsearch import Elasticsearch
import findspark
from etl.functions import add_stay_duration, aggregate_by_hotel_id, \
    get_most_popular_stay_type, write_to_avro, join_initial, es_create_index_if_not_exists
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, window, lit
from pyspark.sql.types import *

findspark.init()

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('spark-batch-processing') \
    .config("spark.jars.packages",
            "org.apache.spark:spark-avro_2.12:3.2.3") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.driver.memory", "15g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df1 = spark \
    .read \
    .format("avro") \
    .load('../datasets/expedia') \
    .filter(col("srch_ci")
            .like(f"%2016%")) \
    .withColumnRenamed('id', 'e_id')


df2 = spark \
    .read \
    .parquet('../datasets/hotel-weather')

filtered = join_initial(df1, df2).filter(col('avg_tmpr_c') > 0)
stay_duration_added = add_stay_duration(filtered)
aggregated_df = aggregate_by_hotel_id(stay_duration_added)
final_batch_df = get_most_popular_stay_type(aggregated_df).drop('with_children')
final_batch_df.write.mode('overwrite').parquet("../datasets/initial_state")

stream_schema = StructType([
    StructField('id', LongType(), True),
    StructField('date_time', StringType(), True),
    StructField('site_name', IntegerType(), True),
    StructField('posa_continent', IntegerType(), True),
    StructField('user_location_country', IntegerType(), True),
    StructField('user_location_region', IntegerType(), True),
    StructField('user_location_city', IntegerType(), True),
    StructField('orig_destination_distance', DoubleType(), True),
    StructField('user_id', IntegerType(), True),
    StructField('is_mobile', IntegerType(), True),
    StructField('is_package', IntegerType(), True),
    StructField('channel', IntegerType(), True),
    StructField('srch_ci', StringType(), True),
    StructField('srch_co', StringType(), True),
    StructField('srch_adults_cnt', IntegerType(), True),
    StructField('srch_children_cnt', IntegerType(), True),
    StructField('srch_rm_cnt', IntegerType(), True),
    StructField('srch_destination_id', IntegerType(), True),
    StructField('srch_destination_type_id', IntegerType(), True),
    StructField('hotel_id', LongType(), True),
])

raw_stream_data = spark \
    .readStream \
    .format("avro") \
    .schema(stream_schema) \
    .load('../datasets/expedia') \
    .filter((~col("srch_ci").like(f"%2016%")) & (col("srch_children_cnt") > 0)) \
    .withColumnRenamed('id', 'e_id')

stream_filtered = join_initial(raw_stream_data, df2).filter(col('avg_tmpr_c') > 0)
stream_stay_duration_added = add_stay_duration(stream_filtered)
aggregated_df_stream = aggregate_by_hotel_id(stream_stay_duration_added)

aggregated_df = aggregated_df.select(*(col(x).alias('2016_' + x) for x in aggregated_df.columns))
aggregated_df = aggregated_df.drop('2016_with_children')
aggregated_df_stream = aggregated_df_stream.select(*(col(x).alias('2017_' + x) for x in aggregated_df_stream.columns))

joined_all_data = aggregated_df_stream.join(broadcast(aggregated_df),
                                            col('2016_hotel_id') == col('2017_hotel_id'),
                                            how='inner') \
    .select(col('2016_hotel_id').alias('hotel_id'),
            (col('2016_Short stay') + col('2017_Short stay')).alias('Short stay'),
            (col('2016_Standard stay') + col('2017_Standard stay')).alias('Standard stay'),
            (col('2016_Standard extended stay') + col('2017_Standard extended stay')).alias('Standard extended stay'),
            (col('2016_Long stay') + col('2017_Long stay')).alias('Long stay'),
            (col('2016_Erroneous data') + col('2017_Erroneous data')).alias('Erroneous data'),
            (col('2017_with_children')).alias('with_children'),
            )

final_stream_df = get_most_popular_stay_type(joined_all_data)

query = final_stream_df \
    .writeStream \
    .foreachBatch(write_to_avro) \
    .outputMode(outputMode='complete') \
    .start()

query.awaitTermination(20)
query.processAllAvailable()
query.stop()


es = Elasticsearch("http://localhost:9200")
index = "elk"
es_create_index_if_not_exists(es, index=index)

windowedCounts = final_stream_df.withWatermark("current_timestamp", "100 milliseconds") \
    .groupBy(window("current_timestamp", "2 microseconds", "1 microseconds"),
             "cnt_erroneous_data",
             "cnt_short_stay",
             "cnt_standard_stay",
             "cnt_standard_extended_stay",
             "cnt_long_stay"
             ) \
    .count()

query2 = windowedCounts \
    .writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("truncate", False) \
    .outputMode("append") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("checkpointLocation", "/tmp") \
    .option("es.resource", "elk") \
    .start()

query2.awaitTermination(20)
query2.processAllAvailable()
query2.stop()

spark.stop()
