from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def set_kv_pair(graph_edges):
    key, value_str = graph_edges.split(':')
    values = value_str.split(' ')
    values.remove('')
    return (key,values)

def get_new_path(element):
    new_paths=[]
    for node in element[1][1]:
        new_path=(node,[element[0],int(element[1][0][1])+1])
        new_paths.append(new_path)
    return new_paths

def get_min_path(a,b):
    if a[1]<b[1]:
        return a
    else:
        return b

def main(inputs, output, source, destination):
    graph_edges=sc.textFile(inputs+'/links-simple-sorted.txt')
    #create a rdd store the graph edges as kv pair
    graph_edges=graph_edges.map(set_kv_pair).cache()
    #create the start point 
    known_paths=sc.parallelize([(source,['-','0'])])

    for i in range(6):
        raw_list=known_paths.join(graph_edges)
        new_paths=raw_list.flatMap(get_new_path)
        known_paths=known_paths.union(new_paths)
        #delete the repeated path and only leep the shortest-length path
        known_paths=known_paths.reduceByKey(get_min_path)
        known_paths.saveAsTextFile(output + '/iter-' + str(i))
        if known_paths.lookup(destination):
            break

    known_paths.cache()
    path=[destination]
    for i in range(known_paths.lookup(destination)[0][1]):
        r=known_paths.lookup(destination)[0][0]
        path.insert(0,r)
        destination=r
    
    finalpath=sc.parallelize(path)
    finalpath.saveAsTextFile(output + '/path')

if __name__ == '__main__':
    conf = SparkConf().setAppName('shortest_path')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = sys.argv[3]
    destination = sys.argv[4]
    main(inputs, output, source, destination)
