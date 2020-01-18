import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.evaluation import RegressionEvaluator


def main(inputs,model_filew):
    tmax_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.DateType()),
        types.StructField('latitude', types.FloatType()),
        types.StructField('longitude', types.FloatType()),
        types.StructField('elevation', types.FloatType()),
        types.StructField('tmax', types.FloatType()),
    ])
    data = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    # data_process=SQLTransformer(statement="SELECT *, dayofyear(date) AS day_of_year FROM __THIS__ ")
    data_process = SQLTransformer(statement="SELECT today.latitude,today.longitude,today.tmax AS tmax, today.elevation,  \
                                  dayofyear(today.date) AS day_of_year,yesterday.tmax AS yesterday_tmax\
                                  FROM __THIS__ as today \
                                  INNER JOIN __THIS__ as yesterday \
                                  ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station")
    # assemble_features = VectorAssembler(inputCols=['latitude','longitude','elevation', 'day_of_year'], outputCol='features')
    assemble_features = VectorAssembler(inputCols=['latitude','longitude','elevation', 'day_of_year', 'yesterday_tmax'], outputCol='features')
    classifier = GBTRegressor(featuresCol='features', labelCol='tmax')
    # classifier = GeneralizedLinearRegression(featuresCol='features', labelCol='tmax',family='gaussian', link='identity')
    pipeline = Pipeline(stages=[data_process, assemble_features, classifier])
    model = pipeline.fit(train)
    predictions = model.transform(validation)
#    predictions.show()

    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='r2')
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='rmse')
    r2 = r2_evaluator.evaluate(predictions)
    rmse = rmse_evaluator.evaluate(predictions)
    print('r-square for GBT model: %g' % (r2, ))
    print('root mean square error for GBT model: %g' % (rmse, ))
    model.write().overwrite().save(model_file)


if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]
    main(inputs, model_file)

