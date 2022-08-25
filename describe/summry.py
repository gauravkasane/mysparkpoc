from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

data="C:\\bigdata1\\drivers\\donations.csv"
df=spark.read.format('csv').options(header='true', inferSchema='true').load(data)

df.show()
df.describe("amount").show()
