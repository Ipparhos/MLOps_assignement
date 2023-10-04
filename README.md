# Optasia_assignement
feature engineering

-First, we run the following command on the terminal to get the docker container up <br />
   &emsp; $ docker compose up
    
-Then we can run some tests for our code in the Jupyter environment which is <br />
more interactive.<br />
-To access Jupyter we open our port localhost:8888. There we have to upload from our local files the notebook.ipynb and cvas_data.csv.

-To run our final code which performs the feature engineering we run the 
following command while docker is still up. <br />
   &emsp; $ docker cp /local_code_directory spark-master:/app <br />
   &emsp; $ docker exec -it spark-master /bin/bash <br />

 -Here you will need to install numpy library to execute spark_job/py
    &emsp; $ pip install numpy
  -This will log us in the container's bash <br />
    &emsp; $ cd /app/local_code_directory <br />
    &emsp; $ spark-submit --master spark://spark-master:7077 spark_job.py  <br />

      
  The last command runs the code in spark_job that performs the feature <br />
  engineering and saves the results in parquet format.
