# creating multiple spark sessions in a application:

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
        master('yarn'). \
        getOrCreate()

sparkSession2 = spark.newSession()

print(spark)
print(sparkSession2)

orders_df = spark.read \
.format("csv") \
.option("header","true") \
.option("inferSchema","true") \
.load("/public/trendytech/orders_wh/*")

orders_df.show()

# create a local tempView:

orders_df.createOrReplaceTempView("orders")

spark.sql("show tables").show()

sparkSession2.sql("show tables").show()

# create a global tempview:

orders_df.createOrReplaceGlobalTempView("orders_global_table")

spark.sql("select * from global_temp.orders_global_table").show()

sparkSession2.sql("select * from global_temp.orders_global_table").show()

spark.stop()
sparkSession2.stop()