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

# Method-1: Specify the dateformat while loading the data into dataframe

orders_schema = 'order_id long, order_date date, customer_id long, order_status string'

orders_df = spark.read \
.format("csv") \
.schema("orders_schema") \
.option("dateFormat", 'MM-dd-yyyy') \
.load("/public/trendytech/datasets/orders_samples2.csv")

# Method-2: First load the date type data as a string into dataframe, then convert to later part

from pyspark.sql.functions import to_date

orders_schema = 'order_id long, order_date string, customer_id long, order_status string'

orders_df = spark.read \
.format("csv") \
.schema("orders_schema") \
.load("/public/trendytech/datasets/orders_samples2.csv")

# creating a new column in the dataframe
transformed_df = orders_df.withColumn("order_date_new", to_date("order_date", 'MM-dd-yyyy'))
transformed_df.show()
transformed_df.printSchema()

# updating the existing column in the dataframe
transformed_df = orders_df.withColumn("order_date", to_date("order_date", 'MM-dd-yyyy'))
transformed_df.show()
transformed_df.printSchema()
