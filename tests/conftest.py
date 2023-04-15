from datetime import datetime
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from etl.functions import add_stay_duration, aggregate_by_hotel_id, get_most_popular_stay_type

class SparkETLTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession\
                     .builder\
                     .master("local")\
                     .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.3")\
                     .appName("Unit-tests")\
                     .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_add_stay_duration(self):
        # 1. Prepare an input data frame that mimics our source data.
        input_schema = StructType([
            StructField('hotel_id', LongType(), True),
            StructField('name', StringType(), True),
            StructField('avg_tmpr_c', DoubleType(), True),
            StructField('srch_ci', StringType(), True),
            StructField('srch_co', StringType(), True),
            StructField('with_children', IntegerType(), True)
        ])
        input_data = [(1829656068102, "Piazza Quattro", 12.1, "2016-10-06", "2016-10-09", 1),
                      (1838246002689, "13 Rue Fran ois", 7.1, "2016-10-23", "2016-10-24", 2),
                      (3126736191488, "10 Place De La", 9.4, "2016-10-10", "2016-10-20", 3),
                      (60129542145, "611 State Route", 7.8, "2016-09-25", "2016-10-16", 4),
                      (601295421440, 'Route De La Corniche', 15.3, "2016-10-06", "2016-10-05", 5)
                      ]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
        # 2. Prepare an expected data frame which is the output that we expect.
        expected_schema = StructType([
            StructField('hotel_id', LongType(), True),
            StructField('name', StringType(), True),
            StructField('avg_tmpr_c', DoubleType(), True),
            StructField('srch_ci', StringType(), True),
            StructField('srch_co', StringType(), True),
            StructField('with_children', IntegerType(), True),
            StructField('current_timestamp', StringType(), True),
            StructField('stay_duration', IntegerType(), True),
            StructField('customer_preferences', StringType(), True)
        ])

        expected_data = [(1829656068102, "Piazza Quattro", 12.1, "2016-10-06", "2016-10-09", 1,
                          str(datetime.now().replace(second=0, microsecond=0)), 3, 'Standard stay'),
                         (1838246002689, "13 Rue Fran ois", 7.1, "2016-10-23", "2016-10-24", 2,
                          str(datetime.now().replace(second=0, microsecond=0)), 1, 'Short stay'),
                         (3126736191488, "10 Place De La", 9.4, "2016-10-10", "2016-10-20", 3,
                          str(datetime.now().replace(second=0, microsecond=0)), 10, 'Standard extended stay'),
                         (60129542145, "611 State Route", 7.8, "2016-09-25", "2016-10-16", 4,
                          str(datetime.now().replace(second=0, microsecond=0)), 21, 'Long stay'),
                         (601295421440, 'Route De La Corniche', 15.3, "2016-10-06", "2016-10-05", 5,
                          str(datetime.now().replace(second=0, microsecond=0)), -1, 'Erroneous data')
                         ]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        # 3. Apply our transformation to the input data frame
        transformed_df = add_stay_duration(input_df)

        # 4. Assert the output of the transformation to the expected data frame.
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, expected_df.schema.fields)]
        fields2 = [*map(field_list, transformed_df.schema.fields)]
        # Compare schema of transformed_df and expected_df
        res = set(fields1) == set(fields2)

        # assert
        self.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

    def test_aggregate_by_hotel_id(self):
        input_schema = StructType([
            StructField('hotel_id', LongType(), True),
            StructField('name', StringType(), True),
            StructField('avg_tmpr_c', DoubleType(), True),
            StructField('srch_ci', StringType(), True),
            StructField('srch_co', StringType(), True),
            StructField('with_children', IntegerType(), True),
            StructField('current_timestamp', StringType(), True),
            StructField('stay_duration', IntegerType(), True),
            StructField('customer_preferences', StringType(), True)
        ])
        input_data = [(1829656068102, "Piazza Quattro", 12.1, "2016-10-06", "2016-10-09", 1,
                       str(datetime.now().replace(second=0, microsecond=0)), 3, 'Standard stay'),
                      (1829656068102, "Piazza Quattro", 7.1, "2016-10-23", "2016-10-24", 2,
                       str(datetime.now().replace(second=0, microsecond=0)), 1, 'Short stay'),
                      (1829656068102, "Piazza Quattro", 9.4, "2016-10-10", "2016-10-13", 3,
                       str(datetime.now().replace(second=0, microsecond=0)), 4, 'Standard stay'),
                      (60129542145, "611 State Route", 7.8, "2016-09-25", "2016-09-26", 4,
                       str(datetime.now().replace(second=0, microsecond=0)), 1, 'Short stay'),
                      (60129542145, '611 State Route', 15.3, "2016-09-27", "2016-10-28", 0,
                       str(datetime.now().replace(second=0, microsecond=0)), 31, 'Short stay')
                      ]
        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        # 2. Prepare an expected data frame which is the output that we expect.
        expected_schema = StructType([
            StructField('hotel_id', LongType(), True),
            StructField('Short stay', IntegerType(), False),
            StructField('Standard stay', IntegerType(), False),
            StructField('Standard extended stay', IntegerType(), False),
            StructField('Long stay', IntegerType(), False),
            StructField('Erroneous data', IntegerType(), False),
            StructField('with_children', IntegerType(), False)
        ])

        expected_data = [(1829656068102, 1, 2, 0, 0, 0, 3),
                         (60129542145, 2, 0, 0, 0, 0, 1)
                         ]
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        # 3. Apply our transformation to the input data frame
        transformed_df = aggregate_by_hotel_id(input_df)
        # 4. Assert the output of the transformation to the expected data frame.
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, expected_df.schema.fields)]
        fields2 = [*map(field_list, transformed_df.schema.fields)]
        # Compare schema of transformed_df and expected_df
        res = set(fields1) == set(fields2)

        # assert
        self.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

    def test_get_most_popular_stay_type(self):
        input_schema = StructType([
            StructField('hotel_id', LongType(), True),
            StructField('Short stay', LongType(), True),
            StructField('Standard stay', LongType(), True),
            StructField('Standard extended stay', LongType(), True),
            StructField('Long stay', LongType(), True),
            StructField('Erroneous data', LongType(), True),
            StructField('with_children', LongType(), True)
        ])

        input_data = [(1829656068102, 1, 2, 0, 0, 0, 3),
                      (60129542145, 2, 0, 0, 0, 0, 1)
                      ]
        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        # 2. Prepare an expected data frame which is the output that we expect.
        expected_schema = StructType([
            StructField('hotel_id', LongType(), True),
            StructField('with_children', LongType(), True),
            StructField('current_timestamp', StringType(), True),
            StructField('cnt_erroneous_data', LongType(), True),
            StructField('cnt_short_stay', LongType(), True),
            StructField('cnt_standard_stay', LongType(), True),
            StructField('cnt_standard_extended_stay', LongType(), True),
            StructField('cnt_long_stay', LongType(), True),
            StructField('most_popular_stay_type', StringType(), False)
        ])

        expected_data = [(1829656068102, 3, str(datetime.now().replace(second=0, microsecond=0)), 0, 1, 2, 0, 0, 'Standard stay'),
                         (60129542145, 1, str(datetime.now().replace(second=0, microsecond=0)), 0, 2, 0, 0, 0, 'Short stay')
                         ]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        # 3. Apply our transformation to the input data frame
        transformed_df = get_most_popular_stay_type(input_df)

        # 4. Assert the output of the transformation to the expected data frame.
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, expected_df.schema.fields)]
        fields2 = [*map(field_list, transformed_df.schema.fields)]
        expected_df.show()
        transformed_df.show()
        expected_df.printSchema()
        transformed_df.printSchema()
        print(fields1)
        print(fields2)
        # Compare schema of transformed_df and expected_df
        res = set(fields1) == set(fields2)

        # assert
        self.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))


if __name__ == '__main__':
    unittest.main()
