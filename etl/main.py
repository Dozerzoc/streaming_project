import findspark

findspark.init()

from etl.functions import add_stay_duration, aggregate_by_hotel_id, \
    get_most_popular_stay_type, write_to_avro, join_initial, filter_by_temp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, concat, lit
from pyspark.sql.types import *
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('spark-batch-processing') \
    .config("spark.jars.packages",
            "org.apache.spark:spark-avro_2.12:3.2.3") \
    .config("spark.driver.memory", "15g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600)

df1 = spark \
    .read \
    .format("avro") \
    .load('./datasets/expedia') \
    .filter(col("srch_ci")
            .like(f"%2016%")) \
    .withColumnRenamed('id', 'e_id')

df2 = spark \
    .read \
    .parquet('./datasets/hotel-weather/hotel-weather')

raw_join = join_initial(df1, df2)
filtered = filter_by_temp(raw_join)
stay_duration_added = add_stay_duration(filtered)
aggregated_df = aggregate_by_hotel_id(stay_duration_added)
final_batch_df = get_most_popular_stay_type(aggregated_df).drop('with_children')
final_batch_df.write.mode('overwrite').parquet("datasets/initial_state")

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
    .load('./datasets/expedia') \
    .filter((~col("srch_ci").like(f"%2016%")) & (col("srch_children_cnt") > 0)) \
    .withColumnRenamed('id', 'e_id')

stream_join = join_initial(raw_stream_data, df2)
stream_filtered = filter_by_temp(stream_join)
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

query.awaitTermination(50)

query.processAllAvailable()
query.stop()
spark.stop()

spark_elastic = SparkSession.builder \
    .master("local[*]") \
    .appName('spark-to-elastic') \
    .config("spark.jars.packages",
            "org.apache.spark:spark-avro_2.12:3.2.3") \
    .config("spark.es.nodes", "localhost") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.driver.memory", "15g") \
    .getOrCreate()

spark_elastic.conf.set("spark.sql.execution.arrow.enabled", "true")
spark_elastic.sparkContext.setLogLevel("ERROR")
spark_elastic.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600)

new_stream_schema = StructType([
    StructField('hotel_id', StringType(), True),
    StructField('with_children', LongType(), True),
    StructField('current_timestamp', StringType(), True),
    StructField('cnt_erroneous_data', LongType(), True),
    StructField('cnt_short_stay', LongType(), True),
    StructField('cnt_standard_stay', LongType(), True),
    StructField('cnt_standard_extended_stay', LongType(), True),
    StructField('cnt_long_stay', LongType(), True),
    StructField('most_popular_stay_type', StringType(), True)
])

df = spark_elastic.readStream.schema(new_stream_schema).format('avro').load('./datasets/hotels_aggregated')
df3 = spark_elastic.read.parquet('./datasets/hotel-weather/hotel-weather')

df_with_coords = df.join(broadcast(df3), df.hotel_id == df3.id, how='inner').select(
    'hotel_id',
    'with_children',
    'current_timestamp',
    'cnt_erroneous_data',
    'cnt_short_stay',
    'cnt_standard_stay',
    'cnt_standard_extended_stay',
    'cnt_long_stay',
    'most_popular_stay_type',
    'latitude',
    'longitude'
)

df_with_coords = df_with_coords.withColumn("coords", concat(col("latitude"), lit(", "), col("longitude")))
df_with_coords = df_with_coords.drop("latitude", "longitude")


index = 'elk'
mappings = {
    "properties": {
        "id": {"type": "long"},
        "with_children": {"type": "long"},
        "current_timestamp": {"type": "text"},
        "cnt_erroneous_data": {"type": "long"},
        "cnt_short_stay": {"type": "long"},
        "cnt_standard_stay": {"type": "long"},
        "cnt_standard_extended_stay": {"type": "long"},
        "cnt_long_stay": {"type": "long"},
        "most_popular_stay_type": {"type": "text"},
        "coords": {"type": "geo_point"}
    }
}
es = Elasticsearch("http://localhost:9200")
es.indices.create(index=index, mappings=mappings)

query2 = df_with_coords.writeStream \
    .outputMode("append") \
    .queryName("writing_to_es") \
    .format("org.elasticsearch.spark.sql.streaming") \
    .option("checkpointLocation", "/tmp/") \
    .option("es.resource", "elk") \
    .option("es.nodes", "localhost") \
    .start()

query2.stop()
# pandasDF = df_with_coords.toPandas()
#
# bulk_data = []
# for i, row in pandasDF.iterrows():
#     bulk_data.append(
#         {
#             "_index": "elk",
#             "_id": i,
#             "_source": {
#                 "id": row["id"],
#                 "with_children": row["with_children"],
#                 "current_timestamp": row["current_timestamp"],
#                 "cnt_erroneous_data": row["cnt_erroneous_data"],
#                 "cnt_short_stay": row["cnt_short_stay"],
#                 "cnt_standard_stay": row["cnt_standard_stay"],
#                 "cnt_standard_extended_stay": row["cnt_standard_extended_stay"],
#                 "cnt_long_stay": row["cnt_long_stay"],
#                 "most_popular_stay_type": row["most_popular_stay_type"],
#                 "coords": row["coords"],
#             }
#         }
#     )
# bulk(es, bulk_data)

es.indices.refresh(index="elk")
es.cat.count(index="elk", format="json")

spark_elastic.stop()
