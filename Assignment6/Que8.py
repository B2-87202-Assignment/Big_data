from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
        .appName("ques8")\
        .config("spark.sql.shuffle.partitions","2")\
        .getOrCreate()

df = spark.read\
     .option("inferSchema","True")\
     .option("header","True")\
     .csv("/home/sunbeam/Cdac/BigDataAssignments/data/Fire_Service_Calls_Sample.csv")


# Q1 )
# result = df.select("Call Type").distinct().count()

# print(result)


#Q2)
# result = df.select("Call Type").distinct()

# print(result)


#Q3)


# df.printSchema()

# result =df.na.drop()

# result = df.withColumn("rec",unix_timestamp("Received DtTm","MM/dd/yyyy HH:mm:ss a"))\
#  .withColumn("resp" ,unix_timestamp("Response DtTm","MM/dd/yyyy HH:mm:ss a"))

# res = result.where("((resp-rec)/60)>5").count()

# print(res)


#Q4)
# df = df.select("Call Type").groupBy("Call Type").count().orderBy(desc("count")).limit(10)

# df.show()


#Q5)


# comm_call = df.select("Call Type").groupBy("Call Type").count().orderBy(desc("count")).limit(10)

# joined = df.alias("t1").join(comm_call.alias("t2"),col("t1.Call Type")==col("t2.Call Type"))\
#         .select("t1.Zipcode of Incident","t1.Call Type")
# joined.show()



#Q6) What San Francisco neighborhoods are in the zip codes 94102 and 94103?


# df = df.na.drop()
# df.printSchema()

# result = df.select("Neighborhooods - Analysis Boundaries","City","Zipcode of Incident")\
#         .where((col("City") == "San Francisco") & (col("Zipcode of Incident") == 94102) |(col("Zipcode of Incident") == 94102) )

# result.show()



#Q7. What was the sum of all calls, average, min, and max of the call response times?
# res = df.withColumn("sum",)




#Q8) How many distinct years of data are in the CSV ﬁle?

# res = df.select(year(to_timestamp(col("data_loaded_at"),"MM/dd/yyyy hh:mm:ss a")).alias("yr")).distinct().show()



#Q9)9. What week of the year in 2018 had the most ﬁre calls?

# df = df.filter(year(to_date(col("Call Date"),"MM/dd/yyyy")) == 2018)
# res = df.select(weekday(to_date(col("Call Date"),"MM/dd/yyyy"))\
#         .alias("week")).groupby("week").count().show()




#Q10)What neighborhoods in San Francisco had the worst response time in 2018?


spark.stop()

