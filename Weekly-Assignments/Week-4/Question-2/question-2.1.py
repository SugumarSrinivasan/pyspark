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

cases_map = cases_base.map(lambda x: (x.split(",")[1], int(x.split(",")[2])))
cases_reduce = cases_map.reduceByKey(lambda x,y: x+y)
result = cases_reduce.sortBy(lambda x: x[1], ascending=False)

result.take(10)

spark.stop()