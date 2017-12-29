import matplotlib as plt
import bokeh.charts as chrt
from bokeh.io import output_file
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.types as typ

sc = SparkContext('local')
spark = SparkSession(sc)


# spark is an existing SparkSession
df = spark.read.csv('D:\\work\\database\\shumei\\shumei_data.csv', sep='\t', header='true', inferSchema='true')
df.createOrReplaceTempView('df')
# Displays the content of the DataFrame to stdout
# spark.sql('select name from df where name like "杨%"').show()
#  df.describe('idcard').show()
plt.bar(df['name', 'max_overdue_days'])

