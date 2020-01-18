from pyspark import SparkConf, SparkContext
import sys
import random
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def get_iterations(number_of_samples):
    iterations=0
    random.seed()
    for i in range(number_of_samples //100 ):
        sum=0.0
        while(sum<1):
            sum+=random.random()
            iterations+=1
    return iterations
    
def main(inputs):
    number_of_samples=int(inputs)
    partitions=100
    #batches=sc.parallelize(range(partitions),numSlices=partitions)
    batches=sc.parallelize ([number_of_samples]*partitions,numSlices=partitions)
    total_iterations=batches.map(get_iterations)
    total_iterations=total_iterations.reduce(lambda a,b:a+b)
    print(total_iterations/number_of_samples)

if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    main(inputs)
