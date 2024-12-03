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

orders_rdd = spark.sparkContext.textFile("/public/trendytech/retail_db/orders/*")
orders_rdd.take(5)

# count the orders under each status 
mapped_rdd = orders_rdd.map(lambda x: (x.split(",")[3],1))
reduced_rdd = mapped_rdd.reduceByKey(lambda x,y: x+y)
reduced_rdd.collect()
reduced_sorted = reduced_rdd.sortBy(lambda x: x[1], False)
reduced_sorted.collect()

# find the premium customers (Top 10 who placed the most number of orders)
customers_mapped = orders_rdd.map(lambda x: (x.split(",")[2],1))
customers_reduced = customers_mapped.reduceByKey(lambda x,y : x+y)
customers_sorted = customers_reduced.sortBy(lambda x:x[1], False)
customers_sorted.take(10)

# distinct count of customers who placed atleast one order
distinct_customers = orders_rdd.map(lambda x: x.split(",")[2]).distinct()
distinct_customers.count()
orders_rdd.count()

# which customer has the maximum number of closed orders
filtered_orders = orders_rdd.filter(lambda x: (x.split(",")[3] == "CLOSED"))
filtered_orders.take(10)
filtered_mapped = filtered_orders.map(lambda x: (x.split(",")[2],1))
filtered_mapped.take(10)
filtered_aggregated = filtered_mapped.reduceByKey(lambda x,y : x+y)
filtered_sorted = filtered_aggregated.sortBy(lambda x : x[1], False)
filtered_sorted.take(10)

spark.stop()