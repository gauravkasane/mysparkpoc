from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").aappName("test").getOrCreate()
