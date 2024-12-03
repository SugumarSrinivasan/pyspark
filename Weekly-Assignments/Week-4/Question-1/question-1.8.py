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

order_items_map = order_items_base.map(lambda x: (int(x.split(",")[1]), float(x.split(",")[4])))

orders_map = orders_base.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[2])))

join_rdd = order_items_map.join(orders_map)

join_map = join_rdd.map(lambda x: (x[1][1],x[1][0]))

customers_map = customers_base.map(lambda x: (int(x.split(",")[0]), x.split(",")[7]))

result_rdd = join_map.join(customers_map)

result_rdd1 =result_rdd.map(lambda x: (x[1][1], x[1][0])).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], ascending=False)

result_rdd1.collect()

spark.stop()