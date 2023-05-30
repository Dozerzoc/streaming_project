import findspark
from pyspark.sql import SparkSession

from etl.functions import add_stay_duration, aggregate_by_hotel_id, \
    join_initial, get_most_popular_stay_type_batch, \
    get_most_popular_stay_type_stream, write_to_avro
from pyspark.sql.functions import col, broadcast, date_format
from pyspark.sql.types import *

findspark.init()

spark = SparkSession.builder \
        .master("local[*]") \
        .appName('spark-batch-processing') \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.0,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.0.0,"
                "org.elasticsearch:elasticsearch-hadoop:8.0.0") \
        .config("spark.es.nodes", "localhost") \
        .config("spark.es.port", "9200") \
        .config("spark.es.resource", "/elk") \
        .config("spark.es.nodes.wan.only", "true") \
        .config("spark.es.index.auto.create", "true") \
        .getOrCreate()
#         .config("spark.es.net.http.auth.user", "elastic")\
#         .config("spark.es.net.http.auth.pass", "changeit") \

spark.sparkContext.setLogLevel("WARN")

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
final_batch_df = get_most_popular_stay_type_batch(aggregated_df)
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
    .option("startingOffsets", "earliest")\
    .option("failOnDataLoss", "false") \
    .load('../datasets/expedia') \
    .filter((~col("srch_ci").like(f"%2016%")) & (col("srch_children_cnt") > 0)) \
    .withColumnRenamed('id', 'e_id') \
    .withColumn('date_time', col('date_time').cast('timestamp')) \
    .withWatermark('date_time', '1 hour')

#
stream_filtered = join_initial(raw_stream_data, df2).filter(col('avg_tmpr_c') > 0)
#
stream_stay_duration_added = add_stay_duration(stream_filtered)
aggregated_df_stream = aggregate_by_hotel_id(stream_stay_duration_added)\
    .withColumn('start', date_format(col('date_time.start'), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZZ'))\
    .withColumn('end', date_format(col('date_time.end'), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZZ'))

aggregated_df = aggregated_df.select(*(col(x).alias('2016_' + x) for x in aggregated_df.columns))
aggregated_df_stream = aggregated_df_stream.select(*(col(x).alias('2017_' + x) for x in aggregated_df_stream.columns))

joined_all_data = aggregated_df_stream.join(broadcast(aggregated_df),
                                            col('2016_hotel_id') == col('2017_hotel_id'),
                                            how='inner') \
    .fillna(0)\
    .select(col('2017_name').alias('name'),
            (col('2016_Short stay') + col('2017_Short stay')).alias('Short stay'),
            (col('2016_Standard stay') + col('2017_Standard stay')).alias('Standard stay'),
            (col('2016_Standard extended stay') + col('2017_Standard extended stay')).alias('Standard extended stay'),
            (col('2016_Long stay') + col('2017_Long stay')).alias('Long stay'),
            (col('2016_Erroneous data') + col('2017_Erroneous data')).alias('Erroneous data'),
            (col('2016_with_children') + col('2017_with_children')).alias('with_children'),
            col('2017_start').alias('start'),
            col('2017_end').alias('end')
            )
#
final_stream_df = get_most_popular_stay_type_stream(joined_all_data)
# ---------------------------------------------------------------------
# In case of we want to save the streaming data into filesystem folder
# ---------------------------------------------------------------------
# print('saving...')
query = final_stream_df \
    .writeStream \
    .foreachBatch(write_to_avro) \
    .outputMode(outputMode='complete') \
    .start()
query.awaitTermination(20)
query.processAllAvailable()
query.stop()
# ---------------------------------------------------------------------
# To check the final_stream
# ---------------------------------------------------------------------
print('checking final table...')
query2 = final_stream_df \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .start()
query2.awaitTermination(70)
query2.processAllAvailable()
query2.stop()
# ---------------------------------------------------------------------
# Write to elastic
# ---------------------------------------------------------------------
print('routing final table to elastic...')

query3 = final_stream_df \
    .writeStream \
    .outputMode('append') \
    .format("org.elasticsearch.spark.sql") \
    .option('es.resource', "/elk") \
    .option('checkpointLocation', "../datasets/tmp") \
    .start().awaitTermination(100)


query3.processAllAvailable()
query3.stop()
spark.stop()

