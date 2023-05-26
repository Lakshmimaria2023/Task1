import pyspark 
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('Task1').master('local[1]').getOrCreate()

df=spark.read.option('multiline','true').option('header','true').option('inferschema','true').json('AAD.json')
#df.show(5)
df1=df.write.mode('overwrite').option('header','true').csv('AAD.csv')


