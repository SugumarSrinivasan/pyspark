# creating the spark session with yarn managed spark cluster:

# importing the required libraries
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession. \
        builder. \
        appName("Spark Session Demo"). \
        config('spark.ui.port', '0'). \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        master('yarn'). \
        getOrCreate()

print(spark)

spark.stop()

#creating the local spark session:

# importing the required libraries
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession. \
        builder.    \
        appName("Spark Session Demo").  \
        config("spark.ui.port", '0').   \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse").   \
        enableHiveSupport().    \
        master('local[*]'). \
        getOrCreate()

spark.stop()