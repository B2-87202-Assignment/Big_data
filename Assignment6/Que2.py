from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
            .appName("Que2")\
            .config("spark.sql.shuffle.partitions", "2")\
            .getOrCreate()

empfile = "/home/tejas/Desktop/BigData/data/emp.csv"
empshcema = "empno INT, ename STRING, job STRING, mgr INT, hire DATE, sal DOUBLE, comm DOUBLE, deptno INT"

df = spark.read\
        .schema(empshcema)\
        .csv(empfile)

result = df.groupBy(["deptno", "job"])\
            .agg(max("sal").alias("max_sal"), min("sal").alias("min_sal"), avg("sal").alias("avg_sal"), sum("sal").alias("tot_sal"))
result.show()
# +------+---------+-------+-------+-------+-------+
# |deptno|      job|max_sal|min_sal|avg_sal|tot_sal|
# +------+---------+-------+-------+-------+-------+
# |    30| SALESMAN| 1600.0| 1250.0| 1400.0| 5600.0|
# |    20|    CLERK| 1100.0|  800.0|  950.0| 1900.0|
# |    20|  MANAGER| 2975.0| 2975.0| 2975.0| 2975.0|
# |    30|  MANAGER| 2850.0| 2850.0| 2850.0| 2850.0|
# |    10|  MANAGER| 2450.0| 2450.0| 2450.0| 2450.0|
# |    20|  ANALYST| 3000.0| 3000.0| 3000.0| 6000.0|
# |    10|PRESIDENT| 5000.0| 5000.0| 5000.0| 5000.0|
# |    30|    CLERK|  950.0|  950.0|  950.0|  950.0|
# |    10|    CLERK| 1300.0| 1300.0| 1300.0| 1300.0|
# +------+---------+-------+-------+-------+-------+

result2 = df.groupBy(["deptno", "job"])\
            .agg(min("sal").alias("min_sal"))
# result2.show()
# +------+---------+-------+
# |deptno|      job|min_sal|
# +------+---------+-------+
# |    30| SALESMAN| 1250.0|
# |    20|    CLERK|  800.0|
# |    20|  MANAGER| 2975.0|
# |    30|  MANAGER| 2850.0|
# |    10|  MANAGER| 2450.0|
# |    20|  ANALYST| 3000.0|
# |    10|PRESIDENT| 5000.0|
# |    30|    CLERK|  950.0|
# |    10|    CLERK| 1300.0|
# +------+---------+-------+

result3 = df.groupBy(["deptno", "job"])\
            .agg(avg("sal").alias("avg_sal"))
# result3.show()
