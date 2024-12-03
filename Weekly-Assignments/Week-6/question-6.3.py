from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession. \
        builder. \
        config("spark.ui.port", '0'). \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        getOrCreate()

train_df = spark.read.option("header","true").option("inferSchema","true").csv("/public/trendytech/datasets/train.csv")

# a) Drop the columns passenger_name and age from the dataset
df1 = train_df.drop("passenger_name","age")
df1.printSchema()
df1.show()

# b) Count the number of rows after removing duplicates of columns train_number and ticket_number
count = df1.dropDuplicates(["train_number","ticket_number"]).count()
print("Number of rows afer removing duplicates:" +" "+str(count))

# c) Count the number of unique train names
df2 = df1.select("train_name").distinct()
unique_train = df2.count()
print("Count of unique Train Names:"+" "+str(unique_train))

spark.stop()