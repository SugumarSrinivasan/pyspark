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
states_base = spark.sparkContext.textFile("/public/trendytech/covid19/states/covid_dataset_states.csv")

states_mapped = states_base.map(lambda x: (x.split(",")[0], (x.split(",")[5], int(x.split(",")[8]))))
cases_mapped = cases_base.map(lambda x: (x.split(",")[1], int(x.split(",")[28])))
total_cases = cases_mapped.reduceByKey(lambda x,y: x+y)
join_rdd = total_cases.join(states_mapped)
join_sorted = join_rdd.sortBy(lambda x: x[1][0], ascending=False)

join_sorted.take(15)

spark.stop()