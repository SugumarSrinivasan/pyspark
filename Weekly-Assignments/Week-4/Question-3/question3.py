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

rdd1 = spark.sparkContext.textFile("/public/trendytech/reviews/trendytech-student-reviews.csv")

rdd2 = rdd1.flatMap(lambda x : x.split(" ")).map(lambda x : x.lower())
rdd3 = rdd2.map(lambda x : (x,1)).reduceByKey(lambda x,y: x+y)

boring_words = spark.sparkContext.textFile("/user/itv011877/data/input/boringwords.txt")

broadcast_bw = spark.sparkContext.broadcast(boring_words.collect())

rdd4 = rdd3.filter(lambda x : x[0] not in broadcast_bw.value)
rdd5 = rdd4.reduceByKey(lambda x,y : x+y).sortBy(lambda x : x[1], ascending=False)

rdd5.take(20)

spark.stop()