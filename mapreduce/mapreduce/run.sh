export MR_OUTPUT=/user/root/output-data

hadoop fs -rm -r $MR_OUTPUT

hadoop jar "$HADOOP_MAPRED_HOME"/hadoop-streaming.jar \
-Dmapred.job.name='avg tips payment' \
-file mapper.py -mapper mapper.py \
-file reducer.py -reducer reducer.py \
-input /user/root/yellow_tripdata_2020-12.csv -output $MR_OUTPUT

hadoop jar "$HADOOP_MAPRED_HOME"/hadoop-streaming.jar \
-Dmapred.job.name='avg tips payment' \
-file mapper.py -mapper mapper.py \
-file reducer.py -reducer reducer.py \
-input s3a://ny-taxi-f/yellow_tripdata_2020-04.csv -output $MR_OUTPUT

hadoop fs -cat /user/root/output-data/* > 12.csv

