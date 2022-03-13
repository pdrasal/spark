from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

myRange = spark.range(1000).toDF("number")
myRange.show()

myRange.printSchema()

spark.sql("SELECT 1 + 1").show()

# narrow transformations 
#the where statement specifies a narrow dependency, where only one partition contributes to at most one output partition
#A wide dependency (or wide transformation) will have input partitions contributing to many 
#output partitions. You will often hear this referred to as a shuffle whereby
#Spark will exchange partitions across the cluster. With narrow transformations, Spark will
#automatically perform an operation called pipelining, meaning that if we specify multiple filters
#on DataFrames, they’ll all be performed in-memory. The same cannot be said for shuffles. When
#we perform a shuffle, Spark writes the results to disk. 

divisBy2 = myRange.where("number % 2 = 0")

divisBy2.count()

divisBy2.show()

# in Python
flightData2015 = spark.read.option("inferSchema", "true").option("header", "true")\
.csv("Bases/2015-summary.csv")

flightData2015.head(3)

flightData2015.sort("count").explain()

spark.conf.set("spark.sql.shuffle.partitions", "5")

flightData2015.sort("count").take(2)

#With Spark SQL, you can register any DataFrame as a table or view and query it using pure SQL. 
#There is no performance difference between writing SQL queries or writing DataFrame code, 
#they both “compile” to the same underlying plan that we specify in DataFrame code.

#You can make any DataFrame into a table or view with one simple method call:
flightData2015.createOrReplaceTempView("flight_data_2015")

#Now we can query our data in SQL. To do so, we’ll use the spark.sql function

sqlWay = spark.sql(""" SELECT DEST_COUNTRY_NAME, count(1) FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME""")
dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()

sqlWay.explain()
dataFrameWay.explain()

sqlWay.show()
dataFrameWay.show()

spark.sql("SELECT max(count) from flight_data_2015").take(1)

flightData2015.select(max("count")).take(1)

maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")
maxSql.show()

maxSql.explain()

# test
spark.sql("""SELECT * FROM flight_data_2015 LIMIT 5""").show()

