import sys
import re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('wikipedia_popular_df').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    m=re.search('/pagecounts-(\d{8}-\d{2})',path)
    found=m.group(1)
    return found


def main(inputs, output):
    views_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('view', types.LongType()),
    types.StructField('bytes', types.LongType()),
    ])

    views = spark.read.csv(inputs,sep=' ', schema=views_schema).withColumn("filename",functions.input_file_name())
    views=views.select(path_to_hour(views['filename']).alias('hour'),views['language'],views['title'],views['view'])
    views_all=views.where((views['language']=='en')& (views['title']!='Main_Page')& (views['title'].startswith('Special:')==False)).cache()
    max_view=views_all.groupby(views['hour']).max()
    join=functions.broadcast(views_all.join(max_view,(views_all['view'] == max_view['max(view)']) & (views_all['hour']==max_view['hour'])))
    result=join.select(views_all['hour'],join['title'],join['view'])
    sorted_result=result.orderBy(result['hour'],result['title'])
    sorted_result.write.json(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

