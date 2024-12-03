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

# permissive mode (default mode)

orders_schema = 'order_id long, order_date string, customer_id long, order_status string'

orders_df = spark.read \
.format("csv") \
.schema("orders_schema") \
.load("/public/trendytech/datasets/orders_samples3.csv")

orders_df.show()
orders_df.printSchema()

# failfast mode

orders_schema = 'order_id long, order_date string, customer_id long, order_status string'

orders_df = spark.read \
.format("csv") \
.schema("orders_schema") \
.option("mode", "failfast") \
.load("/public/trendytech/datasets/orders_samples3.csv")

orders_df.printSchema()
orders_df.show()

# dropmalformed mode

orders_schema = 'order_id long, order_date string, customer_id long, order_status string'

orders_df = spark.read \
.format("csv") \
.schema("orders_schema") \
.option("mode", "dropmalformed") \
.load("/public/trendytech/datasets/orders_samples3.csv")

orders_df.printSchema()
orders_df.show()

