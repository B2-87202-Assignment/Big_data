from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
        .appName("q1")\
        .config("spark.sql.shuffle.partitions","2")\
        .getOrCreate()

jdbcurl = "jdbc:mysql://localhost:3306/dbda_db"
jdbcdriver = "com.mysql.cj.jdbc.Driver"
user1 = "root"
passwd = "manager"

#1. Clean NCDC data and write year, temperature and quality data into mysql table.
filep = "/home/sunbeam/Cdac/BigDataAssignments/ncdc"


ncdcdf = spark.read.text(filep)
ncdcdf = ncdcdf.na.drop()
ncdcdf.show()

regexpr = r"^.{15}([0-9]{4}).{68}([-\+][0-9]{4})([0-9]).*"

df = ncdcdf.select(regexp_extract("value",regexpr,1).alias("year").cast("SHORT"),
                 regexp_extract("value",regexpr,2).alias("temp").cast("SHORT"),
                 regexp_extract("value",regexpr,3).alias("rating").cast("BYTE"))

df.show()

df.write\
  .option("driver",jdbcdriver)\
  .option("user",user1)\
  .option("password",passwd)\
  .jdbc(url=jdbcurl,table="ncdc_tb")



spark.stop()