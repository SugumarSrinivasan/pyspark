# importing the required libraries
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

# creating the spark session
spark = SparkSession. \
        builder. \
        config('spark.ui.port', '0'). \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        master('yarn'). \
        getOrCreate()

from pyspark.sql.types import *

customer_schema = StructType[(
StructField("customer_id",LongType()),
StructField("fullname",StructType([StructField("firstname",StringType()),StructField("lastname",StringType())])),
StructField("city",StringType()),
)]

customer_df = spark.read \
.format("json") \
.schema(customer_schema) \
.load("/public/trendytech/datasets/customer_nested/*")

customer_df.show()
customer_df.printSchema()

spark.stop()