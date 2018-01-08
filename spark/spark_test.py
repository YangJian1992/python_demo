import matplotlib as plt
# import bokeh.charts as chrt
from bokeh.io import output_file
from pyspark.context import SparkContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.types as typ
# conf = (SparkConf().setMaster('spark://masterhostname:7077').setAppName('My Analytical Application').set('spark.executor.memory', '2g'))
# sc = SparkContext(conf=conf)
sc = SparkContext('local')
spark = SparkSession(sc)
a = spark.read.text('../README.md')
print(type(a))



# spark is an existing SparkSession
# df = spark.read.csv('D:\\work\\database\\shumei\\shumei_data.csv', sep='\t', header='true', inferSchema='true')
# df.createOrReplaceTempView('df')
# Displays the content of the DataFrame to stdout
# spark.sql('select name from df where name like "杨%"').show()
#  df.describe('idcard').show()

