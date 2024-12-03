from pyspark.sql import SparkSession 
import getpass

username = getpass.getuser()
spark = SparkSession. \
builder. \
config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
enableHiveSupport(). \
master('yarn'). \
getOrCreate()

logs_data = [("DEBUG","2014-6-22 21:30:49"),
("WARN","2013-12-6 17:54:15"),
("DEBUG","2017-1-12 10:47:02"),
("DEBUG","2016-6-25 11:06:42"),
("ERROR","2015-6-28 19:25:05"),
("DEBUG","2012-6-24 01:06:37"),
("INFO","2014-12-9 09:53:54"),
("DEBUG","2015-11-8 19:20:08"),
("INFO","2017-12-21 18:34:18")]

log_df = spark.createDataframe(logs_data).toDF('loglevel','logtime')
log_df.show()
log_df.printSchema()

# coverting string to timestamp
from pyspark.sql.functions import *
new_log_df = log_df.withColumn("logtime",toTimestamp("logtime"))
new_log_df.show()
new_log_df.printSchema()

new_log_df.createOraReplaceTempView("serverlogs")
spark.sql("select * from serverlogs").show()

# extracting the month as 'January, February'
spark.sql("select loglevel, date_format(logtime, 'MMMM') as months from serverlogs")

# extracting the month as 'Jan, Feb, Mar'
spark.sql("select loglevel, date_format(logtime, 'MMM') as months from serverlogs")

# extracting the month as '01, 02, 03'
spark.sql("select loglevel, date_format(logtime, 'MM') as months from serverlogs")

# extracting the month as '1, 2,'
spark.sql("select loglevel, date_format(logtime, 'M') as months from serverlogs")


spark.sql("""select loglevel, date_format(logtime, 'MMMM') as month,
          count(*) as total_occurence from serverlogs
          group by loglevel, month
          """).show()

# loading the actual data

log_schema = 'loglevel string, logtime timestamp'
log_df = spark.read \
.format("csv") \
.schema(log_schema) \
.load("/public/trendytech/datasets/logdata1m.csv")

new_log_df.createOraReplaceTempView("serverlogs")
spark.sql("select * from serverlogs").show()

# extracting the month as 'January, February'
spark.sql("select loglevel, date_format(logtime, 'MMMM') as months from serverlogs")


spark.sql("""select loglevel, date_format(logtime, 'MMMM') as month,
          from serverlogs
          """).show()

spark.sql("""select loglevel, date_format(logtime, 'MMMM') as month,
          count(*) as total_occurence from serverlogs
          group by loglevel, month
          """).show()

# if you want to order by month
spark.sql("""select loglevel, date_format(logtime, 'MMMM') as month,
          count(*) as total_occurence from serverlogs
          group by loglevel, month
          order by month
          """).show()
# The above query sort based on alpabetical order


spark.sql("""select loglevel, date_format(logtime, 'MMMM') as month,
          date_format(logtime, 'M') as month_num,
          count(*) as total_occurence from serverlogs
          group by loglevel, month, month_num
          order by month_num
          """).show()

spark.sql("""select loglevel, date_format(logtime, 'MMMM') as month,
          int(date_format(logtime, 'M')) as month_num,
          count(*) as total_occurence from serverlogs
          group by loglevel, month, month_num
          order by month_num
          """).show()

spark.sql("""select loglevel, date_format(logtime, 'MMMM') as month,
          date_format(logtime, 'MM') as month_num,
          count(*) as total_occurence from serverlogs
          group by loglevel, month, month_num
          order by month_num
          """).show()

result_df = spark.sql("""select loglevel, date_format(logtime, 'MMMM') as month,
          first(date_format(logtime, 'MM')) as month_num,
          count(*) as total_occurence from serverlogs
          group by loglevel, month
          order by month_num
          """)

final_df = result_df.drop("month_num")
final_df.show()

# pivot table

spark.sql("select loglevel, date_format(logtime, 'MMMM') as month from serverlogs").show()

result_df = spark.sql("select loglevel, date_format(logtime, 'MMMM') as month from serverlogs") \
.groupBy("loglevel") \
.pivot("month") \
.count()

result_df = spark.sql("select loglevel, date_format(logtime, 'MM') as month from serverlogs") \
.groupBy("loglevel") \
.pivot("month") \
.count()

result_df.show()

# Enhancement in pivot table

month_list = ['January',
          'February',
          'March',
          'April',
          'May',
          'June',
          'July',
          'August',
          'September',
          'October',
          'November',
          'December']

result_df = spark.sql("select loglevel, date_format(logtime, 'MM') as month from serverlogs") \
.groupBy("loglevel") \
.pivot("month",month_list) \
.count()