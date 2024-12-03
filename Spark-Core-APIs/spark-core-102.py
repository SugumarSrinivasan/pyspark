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

# building the logic using sample data
words = ("big","Data","Is","SUPER","Interesting","BIG","data","IS","A","Trending","technology")

# parallelize() function will be helpful for us to create and RDD from local list.
words_rdd = spark.sparkContext.parallelize(words)
words_normalized = words_rdd.map(lambda x: x.lower())

words_normalized.collect()

mapped_words = words_normalized.map(lambda x: (x,1))
aggregated_result = mapped_words.reduceByKey(lambda x,y: x+y)
aggregated_result.collect()

# writing the above code in single line
spark.sparkContext.parallelize(words).map(lambda x: x.lower()).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).collect()

# chaining multiple functions together in functional programming

result = spark. \
sparkContext. \
parallelize(words). \
map(lambda x: x.lower()). \
map(lambda x: (x,1)). \
reduceByKey(lambda x,y: x+y)

# as a good practice, always call the action in a separate line, do not combine with the transformations.
result.collect()

spark.stop()