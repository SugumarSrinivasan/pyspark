# Q3. Create logic for linkedIn profile views in Apache Spark.

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

base_rdd = spark.sparkContext.textFile("/user/itv011877/data/input/linkedin_views.csv")
mapped_rdd = base_rdd.map(lambda x: (x.split(",")[2],1))
reduced_rdd = mapped_rdd.reduceByKey(lambda x,y: x+y)
reduced_rdd.collect()
spark.stop()