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

orders_base = spark.sparkContext.textFile("/public/trendytech/retail_db/orders/*")
order_items_base = spark.sparkContext.textFile("/public/trendytech/retail_db/order_items/*")
customers_base = spark.sparkContext.textFile("/public/trendytech/retail_db/customers/*")

orders_map = orders_base.map(lambda x: (int(x.split(",")[2]), x.split(",")[3]))
orders_filter = orders_map.filter(lambda x: x[1] == 'CLOSED')

customers_map = customers_base.map(lambda x: (int(x.split(",")[0]), x.split(",")[7]))
join_rdd = orders_filter.join(customers_map)
join_map = join_rdd.map(lambda x: (x[1][1],1))
join_reduced = join_map.reduceByKey(lambda x,y: x+y)
result = join_reduced.sortBy(lambda x: x[1], ascending=False)

result.take(1)

spark.stop()