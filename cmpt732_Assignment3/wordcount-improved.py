from pyspark import SparkConf, SparkContext
import sys
import re, string 
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def words_once(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    for w in wordsep.split(line):
        w=w.lower()
        yield (w, 1)

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs, output):
    text = sc.textFile(inputs).repartition(80)
    words = text.flatMap(words_once)
    non_empty_words=words.filter(lambda kv: kv[0]!="")
    wordcount = non_empty_words.reduceByKey(add)

    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)
    
if __name__ == '__main__':
    conf = SparkConf().setAppName('word_count_improved')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

