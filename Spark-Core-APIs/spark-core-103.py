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

words = ("big","Data","Is","SUPER","Interesting","BIG","data","IS","A","Trending","technology")

words_rdd = spark.sparkContext.parallelize(words)

# how will you check that how many partitions your rdd has...
words_rdd.getNumPartitions()

spark.sparkContext.defaultParallelism

orders_rdd = spark.sparkContext.textFile("/public/trendytech/retail_db/orders/*")

orders_rdd.getNumPartitions()

spark.sparkContext.defaultMinPartitions

base_rdd = spark.sparkContext.textFile("/public/trendytech/bigLog.txt")
base_rdd.getNumPartitions()

# There are two ways to count the values of each unique key
# using map + reduceByKey
orders_rdd = spark.sparkContext.textFile("/public/trendytech/retail_db/orders/*")
mapped_rdd = orders_rdd.map(lambda x: (x.split(",")[3],1))
results = mapped_rdd.reduceByKey(lambda x,y: x+y)
results.collect()

#using countByValue
orders_rdd = spark.sparkContext.textFile("/public/trendytech/retail_db/orders/*")
mapped_rdd = orders_rdd.map(lambda x: (x.split(",")[3]))
mapped_rdd.countByValue()

spark.stop()