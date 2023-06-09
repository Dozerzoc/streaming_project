from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, datediff, col, when, lit, count, greatest, broadcast, current_timestamp, window, \
    date_format
from pyspark.sql.types import *
from operator import itemgetter

def set_condition(dataframe1, dataframe2):
    """
    :param dataframe1:
    :param dataframe2:
    :return: condition to join two dataframes
    """
    return [dataframe1.hotel_id == dataframe2.id, dataframe1.srch_ci == dataframe2.wthr_date]


def join_initial(dataframe1, dataframe2):
    """
    This function joins two dataframes and adds new columns
    :param dataframe1:
    :param dataframe2:
    :return: new dataframe with added columns
    """
    return dataframe1 \
        .join(broadcast(dataframe2), set_condition(dataframe1, dataframe2), how='inner') \
        .select(
        'hotel_id',
        'name',
        'avg_tmpr_c',
        'srch_ci',
        'srch_co',
        col('srch_children_cnt').alias('with_children'),
        'date_time'
    )


def add_stay_duration(df):
    """
    This function adds timestamp, stay duration and customer preferences columns
    where customer preferences are:
    short stay, standard stay, standard extended stay, long stay
    :param df:
    :return: dataframe with filtered data
    """
    return df \
        .withColumn("stay_duration", (datediff(col('srch_co'), col('srch_ci')))) \
        .withColumn("customer_preferences",
                    when(col("stay_duration") == 1, "Short stay")
                    .when(col("stay_duration").between(2, 7), "Standard stay")
                    .when(col("stay_duration").between(8, 14), "Standard extended stay")
                    .when(col("stay_duration").between(15, 29), "Long stay")
                    .otherwise("Erroneous data"))


def aggregate_by_hotel_id(df):
    """
    This function aggregates counts of customer preferences and children
    where columns of new dataframe are:
    hotel_id, short stay, standard stay, standard extended stay, long stay, erroneous data, with children
    :param df:
    :return: new dataframe with aggregated data
    """
    return df \
        .groupBy(window('date_time', '10 seconds').alias('date_time'), 'hotel_id', 'name').agg(
        count(when(col('customer_preferences') == 'Short stay', col('hotel_id'))).cast('Integer').alias('Short stay'),
        count(when(col('customer_preferences') == 'Standard stay', col('hotel_id'))).cast('Integer').alias(
            'Standard stay'),
        count(when(col('customer_preferences') == 'Standard extended stay', col('hotel_id'))).cast('Integer').alias(
            'Standard extended stay'),
        count(when(col('customer_preferences') == 'Long stay', col('hotel_id'))).cast('Integer').alias('Long stay'),
        count(when(col('customer_preferences') == 'Erroneous data', col('hotel_id'))).cast('Integer').alias(
            'Erroneous data'),
        count(when(col('with_children') != 0, col('hotel_id'))).cast('Integer').alias('with_children'))


def get_most_popular_stay_type_batch(df):
    """
    This function gets the most_popular_stay_type for each hotel according to the maximum number of customers
    and with_children column to distinguish between hotels with children and without children
    :param df:
    :return: new dataframe with added columns: most_popular_stay_type, with_children
    """
    gre = greatest(df['Short stay'], df['Standard stay'], df['Standard extended stay'], df['Long stay'],
                   df['Erroneous data'])

    return df \
        .withColumn("most_popular_stay_type",
                    when(gre == df['Short stay'], "Short stay") \
                    .when(gre == df['Standard stay'], "Standard stay") \
                    .when(gre == df['Standard extended stay'], "Standard extended stay") \
                    .when(gre == df['Long stay'], "Long stay") \
                    .otherwise("Erroneous data")) \
        .select('name',
                col('Erroneous data').alias('cnt_erroneous_data'),
                col('Short stay').alias('cnt_short_stay'),
                col('Standard stay').alias('cnt_standard_stay'),
                col('Standard extended stay').alias('cnt_standard_extended_stay'),
                col('Long stay').alias('cnt_long_stay'),
                col('with_children').alias('cnt_with_children'),
                'most_popular_stay_type'
                )

def get_most_popular_stay_type_stream(df):
    """
    This function gets the most_popular_stay_type for each hotel according to the maximum number of customers
    and with_children column to distinguish between hotels with children and without children
    :param df:
    :return: new dataframe with added columns: most_popular_stay_type, with_children
    """
    gre = greatest(df['Short stay'], df['Standard stay'], df['Standard extended stay'], df['Long stay'],
                   df['Erroneous data'])

    return df \
        .withColumn("most_popular_stay_type",
                    when(gre == df['Short stay'], "Short stay") \
                    .when(gre == df['Standard stay'], "Standard stay") \
                    .when(gre == df['Standard extended stay'], "Standard extended stay") \
                    .when(gre == df['Long stay'], "Long stay") \
                    .otherwise("Erroneous data")) \
        .select('name',
                col('Erroneous data').alias('cnt_erroneous_data'),
                col('Short stay').alias('cnt_short_stay'),
                col('Standard stay').alias('cnt_standard_stay'),
                col('Standard extended stay').alias('cnt_standard_extended_stay'),
                col('Long stay').alias('cnt_long_stay'),
                col('with_children').alias('cnt_with_children'),
                'most_popular_stay_type',
                'start',
                'end'
                )


def write_to_avro(df, epoch_id):
    """
    This function will be called on each microbatch with epoch_id of a microbatch
    which is used for deduplication and transactional guarantees of the data
    :param df:
    :param epoch_id: this is the microbatch id that is provided by Spark foreachBatch
    :return: microbatch data is written to avro format
    """
    df.write \
        .format("avro") \
        .mode("overwrite") \
        .save("../datasets/hotels_aggregated")


maxcol_schema = StructType([StructField('maxval', IntegerType()), StructField('most_popular_stay_type', StringType())])
maxcol = udf(lambda row: max(row, key=itemgetter(0)), maxcol_schema)
