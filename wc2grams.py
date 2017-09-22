import sys
import csv
import re
from operator import add
from pyspark.sql import SparkSession
from pyspark import SparkContext
#Reference: https://stackoverflow.com/questions/35260584/how-to-find-bigram-frequency-on-a-rdd-using-python

reader = csv.reader(open(sys.argv[1]))
dictionary = {}
for row in reader:
	keys = ' '.join(row).split()
	dictionary[keys[0]] = keys[1:]

def word_pairs(line):
	splitLine = line.split(">")
	location = splitLine[0]
	if len(splitLine)==1:
		return ""
	line = re.sub('[^a-zA-Z\\s]', ' ', splitLine[1]).lower().replace('j', 'i').replace('v', 'u')
	words = line.split()
	result = []
	for i in range(len(words)-1):
		for j in range(i, len(words)):
			lemmas1 = []
			lemmas2 = []
			lemmas1 += dictionary.get(words[i], [words[i]])
			lemmas2 += dictionary.get(words[j], [words[j]])
			for lemma1 in lemmas1:
				for lemma2 in lemmas2:
					result.append([lemma1 +" "+ lemma2, location +"." +str(i+1) +">"])
	return result

if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: python_file.py <lemmatizer path> <input> <output>", file=sys.stderr)
		exit(-1)
	sc = SparkContext(appName="lemmatize")
	spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()
	text_file = sc.textFile(sys.argv[2])
	pairs = text_file.flatMap(word_pairs)
	counts = pairs.map(lambda x: (x[0], x[1])).reduceByKey(lambda a, b: a +","+ b).sortByKey()
	output = counts.collect()
	output = [(e, "[" + c +"]" +" count=" + str(len(c.split(",")))) for (e,c) in output]
	sc.parallelize(output).saveAsTextFile(sys.argv[3])
	spark.stop()