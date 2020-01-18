from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def set_comment(reddit_json):
    score=reddit_json['score']
    subreddit=reddit_json['subreddit']
    author=reddit_json['author']
    return {'subreddit':subreddit, 'score':score, 'author':author }

def main(inputs, output):
    text = sc.textFile(inputs)
    reddit_json=text.map(json.loads)
    comment=reddit_json.map(set_comment)
    comment_e=comment.filter(lambda comment:'e' in comment['subreddit']).cache()
    positive=comment_e.filter(lambda comment_e: comment_e['score']>0)
    negative=comment_e.filter(lambda comment_e:comment_e['score']<=0)
    positive.map(json.dumps).saveAsTextFile(output + '/positive')
    negative.map(json.dumps).saveAsTextFile(output + '/negative')

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit_etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
