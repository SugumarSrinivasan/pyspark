from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession. \
builder. \
config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
enableHiveSupport(). \
master('yarn'). \
getOrCreate()

orders_schema = 'order_id long, order_date date, customer_id long, order_status string'

orders_df = spark.read \
.format("csv") \
.schema(orders_schema) \
.load("/public/trendytech/orders/orders_1gb.csv")

orders_df.show()

# when using select("*") it shows all the columns
orders_df.select("*").show()

# Selecting specific columns
orders_df.select("order_id", "order_date").show()

# to remove the ambiguity issue while joining two dataframes with the same column name
orders_df.select(orders_df.order_date).show()

# Array notation
orders_df.select(orders_df['order_date']).show()

# column object notation with programmatic style
from pyspark.sql.functions import *

orders_df.select(column('customer_id')).show()

orders_df.select(col('customer_id')).show()

orders_df.select("*").where(col('order_status').like('PENDING%')).show()
orders_df.select("*").where(column('order_status').like('PENDING%')).show()

# column expression
orders_df.select(expr("customer_id +1 as new_cust_id")).show()

# same with sql style
orders_df.select("*").where("order_status like 'PENDING%'").show()