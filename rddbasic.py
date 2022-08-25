from pyspark.sql import *
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local").appName("test").getOrCreate()

# 1st way sc.parallelize
#data = [12,32,34,4,54,26]
#drdd = spark.sparkContext.parallelize(data)

#nrdd=spark.sparkContext.parallelize(data)
#pro=nrdd.map(lambda x:x*x).filter(lambda x:x>30)

# 2nd way sc.textFile
#data = "C:\\bigdata1\\drivers\\email.txt"
data = "C:\\bigdata1\\drivers\\asltab.txt"
ardd= spark.sparkContext.textFile(data)
pro = ardd.filter(lambda x: "age" not in x).map(lambda x:x.split(",")).filter(lambda x: int(x[1])>=30)
for i in pro.collect():
    print(i)