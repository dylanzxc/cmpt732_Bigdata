import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression


cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('regression').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main(keyspace,model_file,input):
    labeled_data_schema = types.StructType([
        types.StructField('name', types.StringType()),
        types.StructField('tripadvisor/google_ratings', types.FloatType()),
        types.StructField('tripadvisor/google_review_count', types.IntegerType()),
    ])
    labeled_data = spark.read.csv(input, schema=labeled_data_schema)
    labeled_data = labeled_data.withColumn('target_popularity', labeled_data['tripadvisor/google_ratings']*labeled_data['tripadvisor/google_review_count'])
    labeled_data = labeled_data.drop('tripadvisor/google_ratings', 'tripadvisor/google_review_count')
    business = spark.read.format("org.apache.spark.sql.cassandra").options(table='business', keyspace=keyspace).load()
    business = business.select('business_id', 'name', 'stars', 'review_count', 'is_open')   
    data = business.join(labeled_data, 'name')
    review = spark.read.format("org.apache.spark.sql.cassandra").options(table='review', keyspace=keyspace).load()
    review = review.select('business_id', 'useful', 'funny', 'cool')
    review = review.groupBy('business_id').avg('useful','funny', 'cool')
    data = data.join(review, 'business_id')
    train, validation = data.randomSplit([0.9, 0.1])
    train = train.cache()
    validation = validation.cache()

    assemble_features = VectorAssembler(inputCols=['stars','review_count','is_open', 'avg(useful)', 'avg(funny)', 'avg(cool)'], outputCol='features')
    # classifier = GBTRegressor(featuresCol='features', labelCol='target_popularity')
    classifier = LinearRegression(featuresCol='features', labelCol='target_popularity', maxIter=10, regParam=0.3, elasticNetParam=0.8)
    pipeline = Pipeline(stages=[ assemble_features, classifier])
    model = pipeline.fit(train)
    predictions = model.transform(validation)
#    predictions.show()

    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='target_popularity', metricName='r2')
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='target_popularity', metricName='rmse')
    r2 = r2_evaluator.evaluate(predictions)
    rmse = rmse_evaluator.evaluate(predictions)
    print('r-square for GBT model: %g' % (r2, ))
    print('root mean square error for GBT model: %g' % (rmse, ))
    model.write().overwrite().save(model_file)


if __name__ == '__main__':
    keyspace = sys.argv[1]
    model_file = sys.argv[2]
    input = sys.argv[3]
    main(keyspace, model_file,input)
