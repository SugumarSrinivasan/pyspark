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

# we need to find top 10 customers who have spent the most amount(premium customers)

orders_base = spark.sparkContext.textFile("/public/trendytech/retail_db/orders/*")
order_items_base = spark.sparkContext.textFile("/public/trendytech/retail_db/order_items/*")

order_items_mapped = order_items_base.map(lambda x: (int(x.split(",")[1]), float(x.split(",")[4])))
orders_mapped = orders_base.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[2])))

join_rdd = order_items_mapped.join(orders_mapped)
join_mapped = join_rdd.map(lambda x: (x[1][1], x[1][0]))
result = join_mapped.reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], ascending=False)
result.take(10)