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

orders_base = spark.sparkContext.textFile("/public/trendytech/orders/orders_1gb.csv")

# to get the number of partitions
orders_base.getNumPartitions()

# when you wish to increase or decrease number of partitions
# repartitions will be increasing or decreasing the number of partitions
repartitioned_orders = orders_base.repartition(15)
repartitioned_orders.getNumPartitions()

orders_base.getNumPartitions()
new_repartitioned_rdd = orders_base.repartition(4)
new_repartitioned_rdd.getNumPartitions()

# coalesce is only decrease the number of partitions, it will not increase it.
coalesce_rdd = orders_base.coalesce(5)
coalesce_rdd.getNumPartitions()