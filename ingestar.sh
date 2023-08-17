rm -f /home/hadoop/landing/*

wget -P /home/hadoop/landing https://data-engineer-edvai.s3.amazonaws.com/f1/re>
wget -P /home/hadoop/landing https://data-engineer-edvai.s3.amazonaws.com/f1/dr>
wget -P /home/hadoop/landing https://data-engineer-edvai.s3.amazonaws.com/f1/co>

/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/*

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/* /ingest
