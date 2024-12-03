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

orders_df = spark.read \
.format("csv") \
.option("header", "true") \
.option("inferSchema", "true") \
.load("/public/trendytech/orders_wh/*")

# check whether the data is loaded or not
orders_df.show(truncate = False)

# check the schema of the dataframe
orders_df.printSchema()

# creating the temprorary table
orders_df.createOrReplaceTempView("orders")

# 1. Find top 15 customers who placed most number of orders.

# dataframe way
result = orders_df.grouBy("customer_id").count().sort("count", ascending = False).limit(15)
result.show()

# spark SQL way
result = spark.sql("select customer_id, count(*) as count from orders group by customer_id order by count desc limit 15")
result.show()

# 2. Find the number of orders under each order status.

# dataframe way
results = orders_df.groupBy("order_status").count()
results.show()

# spark SQL way
results = spark.sql("select order_status, count(order_id) as count from orders group by order_status")
results.show()

# 3. Number of active customers(who placed atleast one order).

# dataframe way
results = orders_df.select("customer_id").distinct().count()
print(results)

# spark SQL way
results = spark.sql("select count(distinct(customer_id)) as active_customers from orders")
results.show()

# 4. Customers with most number of closed orders.

# dataframe way
results = orders_df.filter("order_status = 'CLOSED'").groupBy("customer_id").count().sort("count", ascending = False)
results.show()

# spark SQL way
results = spark.sql("select customer_id, count(*) as count from orders where order_status = 'CLOSED' group by customer_id order by count desc")
results.show()
