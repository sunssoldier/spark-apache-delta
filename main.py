from pyspark.sql import SparkSession
from pyspark.sql.functions import (lit,col)
from delta.tables import *
import shutil

spark = SparkSession \
	.builder \
	.appName("deltaz") \
	.master("local") \
	.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
	.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
	.getOrCreate()


delta_path = "data/delta-table"

# df0 = spark.read.csv('data/data0.csv', header=True, inferSchema=True) \
# 	.withColumn("start",lit(0)) \
# 	.withColumn("end",lit(0))

# df1 = spark.read.csv('data/data1.csv', header=True, inferSchema=True) \
# 	.withColumn("start",lit(1)) \
# 	.withColumn("end",lit(0))

# df2 = spark.read.csv('data/data2.csv', header=True, inferSchema=True) \
# 	.withColumn("start",lit(2)) \
# 	.withColumn("end",lit(1))

# # spark.read.format("csv") \
# # 	.option("header",True) \
# # 	.load('data/data0.csv') \
# # 	.withColumn("start",lit(0)) \
# # 	.withColumn("end",lit(0)) \
# # 	.write.format("delta")\
# # 	.mode("overwrite")\
# # 	.save(delta_path)

# df0.write.format("delta")\
# 	.mode("overwrite")\
# 	.save(delta_path)

# deltaTable = DeltaTable.forPath(spark, delta_path)

# deltaTable.toDF.show()

# deltaTable.alias("oldData") \
#   .merge(
#     df1.alias("newData"),
#     "oldData.key = newData.key") \
#   .whenMatchedUpdate(set = { "start": col("newData.start") }) \
#   .whenNotMatchedInsert(values = { "end": col("newData.end") }) \
#   .execute()

# deltaTable.toDF().show()

# deltaTable.alias("oldData") \
#   .merge(
#     df2.alias("newData"),
#     "oldData.key = newData.key") \
#   .whenMatchedUpdate(set = { "start": col("newData.start") }) \
#   .execute()

# spark.read.format("delta").option("versionAsOf", 0).load(delta_path).show()
# spark.read.format("delta").option("versionAsOf", 1).load(delta_path).show()
# spark.read.format("delta").option("versionAsOf", 2).load(delta_path).show()

# # Apply SCD Type 2 operation using merge
# deltaTable.alias("history").merge(
# 	df1.alias("updates"),
# 	"history.key = updates.key") \
# .whenMatchedUpdate(
# 	condition = "history.current = true AND history.total_amount <> updates.total_amount",
# 	set = {                                      # Set current to false and endDate to source's effective date.
# 		"current": "false",
# 		"endDate": "updates.effectiveDate"}
# ).whenNotMatchedInsert(
# 	values = {
# 		"tpep_dropoff_datetime": "updates.tpep_dropoff_datetime",
# 		"trip_distance": "updates.trip_distance",
# 		"total_amount":"updates.total_amount",
# 		"current":"true",
# 		"effectiveDate": "updates.effectiveDate",  # Set current to true along with the new address and its effective date.
# 		"endDate": "null"}
# ).execute()

# deltaTable.toDF().show()

df0 = spark.read.csv('data/data0.csv', header=True, inferSchema=True) \
	.withColumn("current",lit("true")) \
	.withColumn("effectiveDate",lit(0)) \
	.withColumn("endDate",lit("null"))	

df1 = spark.read.csv('data/data1.csv', header=True, inferSchema=True) \
	.withColumn("current",lit("true")) \
	.withColumn("effectiveDate",lit(1)) \
	.withColumn("endDate",lit("null"))	

df2 = spark.read.csv('data/data2.csv', header=True, inferSchema=True) \
	.withColumn("current",lit("true")) \
	.withColumn("effectiveDate",lit(2)) \
	.withColumn("endDate",lit("null"))	


def create_delta(delta_path = delta_path, df=df0):
	try:
		shutil.rmtree(delta_path)
	except:
		print('Delta path does not exist yet.')

	df0.write.format("delta")\
		.mode("overwrite")\
		.save(delta_path)

	deltaTable = DeltaTable.forPath(spark, "data/delta-table")
	deltaTable.toDF().show()

	return deltaTable

deltaTable = create_delta()

def pit_merge(df, deltaTable=deltaTable):
	deltaTable.alias("history") \
	.merge(
		df.alias("updates"),
		"history.key = updates.key") \
	.whenMatchedUpdate(
		condition = "history.current = true AND history.total_amount <> updates.total_amount",
		set = {                                      # Set current to false and endDate to source's effective date.
			"current": "false",
			"endDate": "updates.effectiveDate"}
	).whenNotMatchedInsert(
		values = {
			"tpep_dropoff_datetime": "updates.tpep_dropoff_datetime",
			"trip_distance": "updates.trip_distance",
			"total_amount":"updates.total_amount",
			"current":"true",
			"effectiveDate": "updates.effectiveDate",  # Set current to true along with the new address and its effective date.
			"endDate": "null"}
	).execute()
    
	deltaTable.toDF().show()


def pit_merge2(df, deltaTable=deltaTable):

	deltaTable.alias("history") \
	.merge(
		df.alias("updates"),
		"history.key = updates.key") \
	.whenMatchedUpdateAll() \
	.whenNotMatchedInsertAll() \
	.execute()
    
	deltaTable.toDF().show()


def print_version(x=3, delta_path=delta_path):
	for i in range(0,x):
		spark.read.format("delta").option("versionAsOf", i).load(delta_path).show()


pit_merge(df1)
pit_merge(df2)
print_version()

deltaTable = create_delta()
pit_merge2(df1)
pit_merge2(df2)
