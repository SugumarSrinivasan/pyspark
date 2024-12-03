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

cases_map = cases_base.map(lambda x: int(x.split(",")[7]))
result = cases_map.reduce(lambda x,y: x+y)
print("Number of Patients in ICU Currently:" +" "+str(result))
spark.stop()