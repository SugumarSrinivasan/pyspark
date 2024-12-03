from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession. \
        builder. \
        config("spark.ui.port", '0'). \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        getOrCreate()

mylist = [
("Spring",12.3),
("Summer",10.5),
("Autumn",8.2),
("Winter",15.1)
]

df = spark.createDataFrame(mylist).toDF("season","windspeed")
df.printSchema()

df.show()

spark.stop()