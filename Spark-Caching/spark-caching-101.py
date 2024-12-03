from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession. \
        builder. \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        getOrCreate()

orders_schema = 'order_id long,order_date date,customer_id long,order_status string'

orders_df = spark.read \
.format("csv") \
.schema(orders_schema) \
.load("/public/trendytech/orders/orders_1gb.csv")

orders_df.show()
orders_df.printSchema()

orders_df_cached = orders_df.cache()

# it caches only the 1st partition data
orders_df_cached.show()

# show the first line of the dataframe
orders_df_cached.head()

# show the last 10 lines of the data, it supports only in spark-3.0
orders_df_cached.tail()

# it caches all the data
orders_df_cached.count()

# uncache the data from memory
orders_df_cached.unpersist()

# stop the spark session
spark.stop()