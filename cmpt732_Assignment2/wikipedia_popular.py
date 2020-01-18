from pyspark import SparkConf, SparkContext
import sys
import re, string 
from operator import itemgetter
conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
wordsep = re.compile(' +')
inputs = sys.argv[1]
output = sys.argv[2]

def set_tuple(line):
    elements=wordsep.split(line)
    #the integer is in base 10
    elements[3]=int(elements[3],10)
    yield (elements[0],elements[1],elements[2],elements[3],elements[4])

def get_pair(elements):
    return (elements[0],(elements[3],elements[2]))

def get_max(a,b):
    #max(a,b, key=itemgetter(1)) if we want to apply max() on second element
    return max(a,b)

def tab_separated(kv):
    #／t will show in .take() but will be " " when print 
    return "%s\t%s" % (kv[0], kv[1])

def get_key(kv):
    return kv[0]

text = sc.textFile(inputs)
tuples=text.flatMap(set_tuple)
useful_tuples=tuples.filter(lambda elements: elements[1]=='en' and elements[2]!='Main_Page' and elements[2].startswith('Special:')==False)
kv_pairs=useful_tuples.map(get_pair)
max_count=kv_pairs.reduceByKey(get_max)
max_count=max_count.sortBy(get_key)
outdata=max_count.map(tab_separated)
outdata.saveAsTextFile(output)
