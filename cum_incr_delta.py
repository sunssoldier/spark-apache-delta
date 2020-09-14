from utils import *

# spark = SparkSession \
# 	.builder \
# 	.appName("deltaz") \
# 	.master("local") \
# 	.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
# 	.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
# 	.getOrCreate()

df0 = spark.read.csv('data/cum_incr/data0.csv', header=True, inferSchema=True) \
	.withColumn("current",lit("true")) \
	.withColumn("effectiveDate",lit(0)) \
	.withColumn("endDate",lit("null"))	

df1 = spark.read.csv('data/cum_incr/data1.csv', header=True, inferSchema=True) \
	.withColumn("current",lit("true")) \
	.withColumn("effectiveDate",lit(1)) \
	.withColumn("endDate",lit("null"))	

df2 = spark.read.csv('data/cum_incr/data2.csv', header=True, inferSchema=True) \
	.withColumn("current",lit("true")) \
	.withColumn("effectiveDate",lit(2)) \
	.withColumn("endDate",lit("null"))	

df3 = spark.read.csv('data/cum_incr/data3.csv', header=True, inferSchema=True) \
	.withColumn("current",lit("true")) \
	.withColumn("effectiveDate",lit(3)) \
	.withColumn("endDate",lit("null"))

# deltaTable.toDF().sort('key').show()

# Method 1
dT = create_delta('data/delta-table1', df0)
pit_merge(df1, dT)
pit_merge(df2, dT)
pit_merge(df3, dT)
print('Merge V1')
print_version(3,'data/delta-table')


# Method 2
dT2 = create_delta('data/delta-table2', df0)

pit_merge2(df1, dT2)
pit_merge2(df2, dT2)
pit_merge2(df3, dT2)
print('Merge V2')
print_version(3, 'data/delta-table2')
