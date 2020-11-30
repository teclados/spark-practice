import sys
import re
from operator import add

from pyspark import SparkContext

def map_phase(x):
    x = re.sub('--', ' ', x)
    x = re.sub("'", '', x)
    return re.sub('[?!@#$\'",.;:()]', '', x).lower()

if __name__ == "__main__":
	if len(sys.argv) < 4:
      		print >> sys.stderr, "Usage: wordcount <master> <inputfile> <outputfile>"
          	exit(-1)
	sc = SparkContext(sys.argv[1], "python_wordcount_sorted in bigdataprogrammiing")
	lines = sc.textFile(sys.argv[2],2)
	print(lines.getNumPartitions()) # print the number of partitions
	counts = lines.flatMap(lambda line: line.split("\n"))
        filtered = counts.filter(lambda x: x!='Tokyo')
	reduced = filtered.map(lambda word: (map_phase(word), 1)).reduceByKey(lambda a, b: a + b)
	result = reduced.sortBy(lambda x: x[1])
        result.collect()
        result.saveAsTextFile(sys.argv[3])
