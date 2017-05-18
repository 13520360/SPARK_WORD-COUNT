#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    # save counts
    lines.saveAsTextFile("/tmp/show-spark/lines")
    """
    text(input) split by SPACE(' ')
    map word into 1 ('The' : 1), then reduce by key and call function add(from operator import add)
    ('The' : 1) ('The' : 1) => ('The' : 2)

    ##################
    help(add)
        Help on built-in function add in module operator:

        add(...)
        add(a, b) -- Same as a + b.

    # example call
    >>> add(1,2)
    3
    ##################
    """
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)


    # save counts
    counts.saveAsTextFile("/tmp/show-spark/counts")

    output = counts.collect()

    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()
