from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

data="C:\\bigdata1\\drivers\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",",").load(data)

#user defined functions
def func(st):
    if(st=="NY"):
        return "30% off"
    elif(st=="CA"):
        return "40% off"
    elif (st=="OH"):
        return "50% off"
    else:
        return "500/- off"

#by default spark unable to understand python functions. so convert python/scala/java function to UDF (spark able to understand udfs)
uf = udf(func)
ndf = df.withColumn("offer", uf(col("state")))

#using this udf in sql
df.createOrReplaceTempView("tab")
spark.udf.register("offer",uf)#udf is converted int sql function
ndf=spark.sql("select *, offer(state) todayoffer from tab")

ndf.printSchema()
ndf.show(truncate=False)
