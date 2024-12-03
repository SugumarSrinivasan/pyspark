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

# reading the data from hdfs
rdd1 = spark.sparkContext.textFile("/user/itv005857/data/inputfile.txt")

# applying the transformations
rdd2 = rdd1.flatMap(lambda line : line.split(" "))
rdd3 = rdd2.map(lambda word : (word,1))
rdd4 = rdd3.reduceByKey(lambda x,y : x+y)

# display the results
rdd4.collect()

# saving the results to hdfs
rdd4.saveAsTextFile("/user/itv005857/output")