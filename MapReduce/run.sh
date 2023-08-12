export MR_OUTPUT=/user/ubuntu/2020/output-data
hadoop fs -rm -r $MR_OUTPUT

hadoop jar "$HADOOP_MAPRED_HOME"/hadoop-streaming.jar \
-Dmapred.job.name='Taxi map-reduce job' \
-Dmapred.reduce.tasks=1 \
-Dmapreduce.map.memory.mb=1024 \
-file ./2020/mapper.py -mapper ./2020/mapper.py \
-file ./2020/reducer.py -reducer ./2020/reducer.py \
-input /user/ubuntu/2020/ -output $MR_OUTPUT

hadoop fs -cat /user/ubuntu/2020/output-data/part-0000* | sort -t, -k2,2 -k1,1 > result.csv
