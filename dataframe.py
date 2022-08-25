from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()

# old approch
data = "C:\\bigdata1\\drivers\\asltab.txt"
ardd= spark.sparkContext.textFile(data)
pro = ardd.filter(lambda x: "age" not in x).map(lambda x:x.split(",")).toDF(["name","age","city"])
pro.createOrReplaceTempView("tab")
result=spark.sql("select * from tab where city='blr' and age<30")
# another way
result = pro.where((col("age")>=30) & (col("city")=="mas"))
result.show()
# above steps mostly used in 2015 after march.. above approch .. rdd ur converting to dataframe
#but  after jan 2016 mostly us dataframe api

# for unstructured data

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "C:\\bigdata1\\drivers\\emailsmay4.txt"
erdd = spark.sparkContext.textFile(data)
res = erdd.filter(lambda x:"@" in x).map(lambda x:x.split(",")).map(lambda x:(x[0],x[-1]))

for i in res.collect():
    print(i)


