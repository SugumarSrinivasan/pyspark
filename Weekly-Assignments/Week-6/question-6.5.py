from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession. \
        builder. \
        config("spark.ui.port", '0'). \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        getOrCreate()


#3. Add a new column called "duration_of_stay" that represents the number of days a patient stayed in the hospital.
# (hint: The duration should be calculated as the difference between the "discharge_date" and "admission_date" columns.)

from pyspark.sql.functions import to_date, expr, datediff
new_df = renamed_column_df.withColumn("duration_of_stay", datediff(to_date('discharge_date','yyyy-MM-dd'),to_date('admission_date','MM-dd-yyyy')))
new_df.printSchema()
new_df.show()

# 4. Create a new column called "adjusted_total_cost" that calculates the adjusted total cost based on the diagnosis as follows:
# If the diagnosis is "HeartAttack", multiply the hospital_bill by 1.5.
# If the diagnosis is "Appendicitis", multiply the hospital_bill by 1.2.
# For any other diagnosis, keep the hospital_bill as it is.

df1 = new_df.withColumn("adjusted_total_cost", expr("CASE WHEN diagnosis LIKE '%Heart Attack%' THEN hospital_bill * 1.5 WHEN diagnosis LIKE '%Appendicitis%' THEN hospital_bill * 1.2 ELSE hospital_bill END"))
df1.printSchema()
df1.show()

# 5. Select the "patient_id", "diagnosis", "hospital_bill", and "adjusted_total_cost" columns.

result = df1.select("patient_id","diagnosis","hospital_bill","adjusted_total_cost")
result.show()

spark.stop()