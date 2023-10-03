# Optasia_assignement
feature engineering

-First we run the following command on terminal to get the docker container up
    $ docker compose up
    
-Then we can run some tests for our code in the jupyter environment which is 
more interactive. To access jupyter we open our port localhost:8888.
-There we have to upload from our local files the notebook.ipynb and cvas_data.csv.

-To run our final code which performs the feature engineering we run the 
following command while docker is still up.
    $ docker cp /local_code_directory spark-master:/app
    $ docker exec -it spark-master /bin/bash
  -This will log us in the containers bash
      $ cd /app/local_code_directory
      $ spark-submit --master spark://spark-master:7077 spark_job.py 
  The last command runs the code in spark_job that performs feature
  engineering and saves the results in parquet format.
