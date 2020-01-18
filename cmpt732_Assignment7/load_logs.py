import sys,os,uuid,gzip,re
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement,SimpleStatement
from cassandra import ConsistencyLevel
from datetime import datetime

def disassemble(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    return line_re.split(line)


def main(input_dir, keyspace, table):
    from cassandra.cluster import Cluster
    cluster = Cluster(['199.60.17.32', '199.60.17.65'])
    session = cluster.connect(keyspace)
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    query="INSERT INTO %s(host,id,datetime,path,bytes) VALUES (?,?,?,?,?)"%(table)
    insert_data = session.prepare(query)
    counter=0

    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                server_logs_dis=disassemble(line)
                if len(server_logs_dis)==6:
                    datetime_object=datetime.strptime(server_logs_dis[2],'%d/%b/%Y:%H:%M:%S')
                    batch.add(insert_data, (server_logs_dis[1], uuid.uuid1(), datetime_object,server_logs_dis[3],int(server_logs_dis[4])))
                    counter=counter+1
                if counter==200:
                    session.execute(batch)
                    batch.clear()
                    counter=0


if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(input_dir, keyspace, table)
