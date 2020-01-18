from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def set_kv_pair(reddit_json):
    value=(1,reddit_json['score'])
    key=reddit_json['subreddit']
    return (key,value)

def add_pairs(a,b):
    return((a[0]+b[0]),(a[1]+b[1]))

def get_avg(pair):
    avg=pair[1][1]/pair[1][0]
    return(pair[0],avg)

def get_relative_avg(comment):
    return ((comment[1][0]['score'])/comment[1][1],comment[1][0]['author'])

def main(inputs, output):
    text = sc.textFile(inputs)
    commentdata=text.map(json.loads).cache()
    kv_pairs=commentdata.map(set_kv_pair)
    sum_pairs=kv_pairs.reduceByKey(add_pairs)
    average=sum_pairs.map(get_avg)
    commentbysub = commentdata.map(lambda c: (c['subreddit'], c))
    comment=commentbysub.join(average)
    relative_avg=comment.map(get_relative_avg)
    relative_avg_sort=relative_avg.sortByKey(ascending= False)
    outdata=relative_avg_sort.map(json.dumps)
    outdata.saveAsTextFile(output)
if __name__ == '__main__':
    conf = SparkConf().setAppName('relative_score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

