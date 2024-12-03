from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession. \
builder. \
config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
enableHiveSupport(). \
master('yarn'). \
getOrCreate()

orders_df = spark.read \
.format('csv') \
.option("header", "true") \
.option("inferSchema", "true") \
.load("/public/trendytech/datasets/order_data.csv")

orders_df.show()

from pyspark.sql.functions import *

# Using Programming way
summary_df = orders_df \
.groupBy("country","invoiceno") \
.agg(sum("quantity").alias("total_quantity"), sum(expr("quantity * unitprice")).alias("invoice_value")) \
.sort("invoiceno")

summary_df.show()

# Using Column Expression
summary_df1 = orders_df \
.groupBy("country","invoiceno") \
.agg(expr("sum(quantity) as total_quantity"), expr("sum(quanitity * unitprice) as invoice_value")) \
.sort("invoiceno")

summary_df1.show()

# Using SQL style
orders_df.createOrReplaceTempView("orders")
spark.sql("""select country, invoiceno, 
          sum(quantity) as total_quantity, 
          sum(quantity * unitprice) as invoice_value 
          from orders 
          group by country,invoiceno 
          order by invoiceno""")

