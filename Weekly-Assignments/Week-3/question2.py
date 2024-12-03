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

rdd1 = spark.sparkContext.textFile("/user/itv011877/data/inputfile.txt")
rdd2 = rdd1.flatMap(lambda x : x.split(" "))
rdd3 = rdd2.map(lambda word : (word,1))
rdd4 = rdd3.reduceByKey(lambda x,y : x+y)
rdd4.collect()
rdd4.saveAsTextFile("/user/itv011877/data/newoutput")