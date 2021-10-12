import sys
from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext()

    text_file = sc.textFile(sys.argv[1])
    counts = text_file.flatMap(lambda line: line.split(" ")) \
        .map(lambda w: (w, 1)) \
        .reduceByKey(lambda a, b: a + b)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    sc.stop()
