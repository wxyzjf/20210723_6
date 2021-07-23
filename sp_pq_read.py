df = spark.read.text("file:///kahome/spk_py/res/part-00001-d116c669-a3b9-4ee7-91ae-251a6242da31-c000.gz.parquet")
df = spark.read.parquet("file:///kahome/spk_py/res/part-00001-390890a1-f498-4551-a871-aaf3ef136529-c000.snappy.parquet")
df.take(3)














for f in `seq 1000000`; do echo $RANDOM >> test_1m.csv; done;




