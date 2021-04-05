### Purpose
Run spark job on Yelp dataset resulting in:
- sentinmental analysis model that predicts user rating
- weighted rating of each business where rating is adjusted based on quantity of reviews 
	- i.e a 5 star restaurant with 5 reviews vs a 4 star restaurant with 1k reviews

Design:
Spark jobs are run from AWS EMR cluster and orchestrated through Apache Airflow. That means the Spark job is automated end-to-end.

This project was based off of the architecture suggested in this [startdataengineering.com](https://www.startdataengineering.com/post/how-to-submit-spark-jobs-to-emr-cluster-from-airflow/#clone-repository) post.

### Prereqs
1. Install [Docker](https://docs.docker.com/get-docker/)
2. Install [docker-compose](https://docs.docker.com/compose/install/)
3. AWS account credentials for S3 and EMR
4. Move yelp [dataset](https://www.yelp.com/dataset/download) to S3
4. Create a file `dags/aws_credentials.json` and update login and password
```json
{
	 "login":"<access_key>",
	 "password":"<access_key_secret>"
}
```
5. To run Airflow job, go to http://localhost:8080/

### EMR Learning Lessons
1. When using `s3-dist-cp`, the --src arg MUST be a directory. If you want to only move specific files, you would add an additional regex argument
2. If you want to ssh into the EMR cluster, you need to include this additional argument `{Ec2KeyName: <key>}` inside JOB_FLOW_OVERRIDES 
3. If you run into any issues with your steps, the console provides very poor logging, your best bet is to SSH into the cluster itself
	 1. The steps logging file is located here: `/mnt/var/log/hadoop/steps/`
	2. To figure out the step ID, go to EMR console, and go to `Steps` tab
4. When you ssh into EMR, if you want to locate the files you moved from S3 >> HDFS, you need to run this command: `hadoop fs -ls hdfs:///`
5. Inside pyspark program, I had a hard time figuring out how to reference the HDFS files that were moved earlier. This [stackoverflow](https://stackoverflow.com/a/46165668) answer helped me understand.
