# Create SparkSession from builder
import pyspark
from pyspark.sql import SparkSession
from pyspark import *
spark = SparkSession.builder.master("local[1]") \
    .appName('joining_dfs_without_union') \
    .getOrCreate()

df = spark.read.csv(r'C:\Users\rahul\Desktop\sample.csv.txt', header=True)
df.show()
df.printSchema()
list_df = df.randomSplit([0.5,0.5])
d={}
for i in list_df:
    y=i.rdd.flatMap(lambda x: x).collect()
    y1=y[1::2]
    y2=y[::2]
    mydict = dict(zip(y1, y2))
    d.update(mydict)
final_d=list(d.items())
final_df=spark.createDataFrame(data=final_d,schema=["1st column name","2nd column name"])
final_df.show()
