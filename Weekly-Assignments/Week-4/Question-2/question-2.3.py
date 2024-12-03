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

cases_base = spark.sparkContext.textFile("/public/trendytech/covid19/cases/covid_dataset_cases.csv")

cases_map = cases_base.map(lambda x: (x.split(",")[1], int(x.split(",")[11])))
reduce_rdd = cases_map.reduceByKey(lambda x,y: x+y)
result = reduce_rdd.sortBy(lambda x: x[1], ascending=False)

result.take(15)

spark.stop()