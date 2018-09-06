# Purpose
This branch contains the 2.6.0 Apache Beam examples with the pom.xml modified to produce an executable uber jar for usage with the DataflowRunner and GCP I/Os. This tests GCS, Bigquery and Pubsub.

# Useful code links
[Maven shade plugin configuration to create uber jar](https://github.com/lukecwik/incubator-beam/blob/b31b8aa32e15f5e1f98d54561d1952884efe9d7f/pom.xml#L72)

# How to run
Create the uber jar by invoking: 
```
mvn package
```

Create or reuse an existing GCP bucket, a Pubsub topic, and a BigQuery dataset.

Run the Leaderboard example by specifying
```
java -jar target/examples-beam-0.1.jar --runner=DataflowRunner --tempLocation=gs://GCP_BUCKET/leaderboard --project=GCP_PROJECT --topic=projects/GCP_PROJECT/topics/MY_TOPIC --dataset=MY_DATASET --output=gs://GCP_BUCKET/leaderboard/output.txt
```

Goto the Pubsub topic and manually publish a few messages of the format `user name as string,team name as string,score as integer,timestamp as long` (e.g. `lukecwik,google,777,1536191771445`). Then wait for the pipeline (~5 mins) to process your records and check your Bigquery Dataset for output.
