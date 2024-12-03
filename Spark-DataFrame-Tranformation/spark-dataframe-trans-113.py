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

mylist = [
(1,"kapil",34),
(1,"kapil",34),
(1,"Sathish",26),
(2,"Sathish",26)
]

person_df = spark.createDataFrame(mylist).toDF("id","name","age")
person_df.show()

# distinct:
new_df = person_df.distinct()
new_df.show()

person_df.select("id").distinct().show()

# drop duplicates:
new_df = person_df.dropDuplicates()
new_df.show()

new_df = person_df.dropDuplicates(["name","age"])
new_df.show()

new_df = person_df.dropDuplicates(["id"])
new_df.show()

spark.stop()