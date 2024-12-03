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

# reduce is an action and not a transformation.
my_list = [1,4,6,8,9,10,12]
base_rdd = spark.sparkContext.parallelize(my_list)
base_rdd.reduce(lambda x,y: x+y)

spark.stop()