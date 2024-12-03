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

# spark.read()
order_schema = 'order_id long, order_date string, customer_id int, order_status string'
orders_df = spark.read \
.format("csv") \
.schema(order_schema) \
.load("/public/trendytech/datasets/orders_samples3.csv")

orders_df.show()

# spark.sql()
closed_orders_df = spark.sql("select * from orders where order_status = 'CLOSED'")
closed_orders_df.show()

# spark.table()
orders_table_df = spark.table("itv011877_retail.orders_ext")
orders_table_df.show()

# spark.range()
range_df = spark.range(5)
range_df.show()

range_df = spark.range(0,8)
range_df.show()

range_df = spark.range(0,8,2)
range_df.show()

# spark.createDataFrame()
order_list = [
(1,'2023-07-25 00:00:00.0',11599,'CLOSED')
(2,'2023-07-25 00:00:00.0',256,'PENDING_PAYMENT')
(3,'2023-07-25 00:00:00.0',12111,'COMPLETE')
]

orders_df = spark.createDataFrame(order_list)
orders_df.show()