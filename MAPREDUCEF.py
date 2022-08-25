from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
#sc = spark.sparkContext
data = "C:\\bigdata1\\drivers\\emailsmay4.txt"
erdd = spark.sparkContext.textFile(data)
res = erdd.filter(lambda x:"@" in x).map(lambda x:x.split(",")).map(lambda x:(x[0],x[-1]))

for i in res.collect():
    print(i)