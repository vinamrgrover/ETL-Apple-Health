# Apple Health ETL Project

## Architecture Diagram

![Apple_Health_ETL drawio](https://user-images.githubusercontent.com/100070155/235325889-724ea75c-7f6d-46ff-8345-99963219603b.png)

## Description
This project involves an **ETL** (Extract, Transform, Load) process to analyze sleep data exported from Apple Health to iCloud in XML format. 

The data is then processed and transformed using **AWS** services, queried through Amazon Athena, and visualized using a **Streamlit** dashboard.

## Project Overview

+ Export Health Data from iPhone to iCloud in **XML** format.
+ Load the data into an **Amazon S3** bucket.
+ Set up an **AWS Lambda** function to process the **XML** data into a **CSV** file and store it in the S3 bucket.
+ Set up another AWS Lambda function to further transform the data using **DuckDB** and **Pandas**. 
+ The data is then redirected to a Lambda Function, which saves the data as **Parquet** files in an S3 Bucket, partitioned by year. 
+ Set up **AWS Glue** Crawlers to crawl the Parquet files stored in the S3 bucket and store the data in the **AWS Glue Data Catalog** table, partitioned by year.
+ Finally, A **Streamlit** dashboard is set up on an **Amazon EC2** instance to display sleep analytics over the years.

## Tech Stack
+ AWS Services : S3, Lambda, Glue, Athena, SNS, EC2
+ Python Libraries : boto3, lxml, s3fs, awswrangler, pandas, duckdb, streamlit
+ Data Processing : DuckDB
+ Analytics and Visualization : Athena, Streamlit

## Prerequisites
The above tech stack and an iCloud Account with Apple Health Data synced regularly from Apple Watch are required. If you don't have an account, you can download my [Health Dataset](https://github.com/vinamrgrover/ETL-Apple-Health/files/11360011/apple_health_export.zip)

## Workflow
1. The Data is Exported from Apple Health to iCloud (Download [here](https://github.com/vinamrgrover/ETL-Apple-Health/files/11360011/apple_health_export.zip))

2. The Data is then transfered locally to an S3 Bucket using AWS CLI. 

<img width="1111" alt="Screenshot 2023-04-30 at 2 11 27 AM" src="https://user-images.githubusercontent.com/100070155/235323410-9d9fa9ec-050d-45a6-9cc4-4f512bd83bce.png">

3. An AWS Lambda Function (*Process_XML*) is set up to Transform the Raw Data (XML) and save it as CSV Files in an S3 Bucket. If the Function's execution fails, its response is redirected to an AWS SNS Topic. 

<img width="857" alt="Screenshot 2023-04-30 at 2 14 19 AM" src="https://user-images.githubusercontent.com/100070155/235323511-6e28d998-7918-42fb-b064-e3fea64d41bd.png">

Processed CSV Files: 

<img width="381" alt="Screenshot 2023-04-30 at 2 19 11 AM" src="https://user-images.githubusercontent.com/100070155/235323694-bbf7ee99-496a-4679-b02d-b46642416276.png">

4. Another Lambda Function (*Transform Health*) is triggered by an Object Put in S3 Bucket, which further transforms the Data using DuckDB and redirects the transformed Data to another Lambda Function (*To_Parquet*), which saves the Data as Parquet Files, partitioned by year. 

<img width="874" alt="Screenshot 2023-04-30 at 2 22 40 AM" src="https://user-images.githubusercontent.com/100070155/235324085-9398c31f-7e58-4ea7-949c-3d394fecb5b4.png">

Transformed Parquet Files:

<img width="491" alt="Screenshot 2023-04-30 at 2 33 05 AM" src="https://user-images.githubusercontent.com/100070155/235324193-c30c6e5f-182a-4cc4-ad35-0ee5e343847d.png">

The Data is partitioned by year: 

<img width="436" alt="Screenshot 2023-04-30 at 2 38 55 AM" src="https://user-images.githubusercontent.com/100070155/235324322-2a0cbf88-9a45-4373-8849-843ce6fc4d20.png">

5. The same Lambda Function (*To_Parquet*), triggers AWS Glue Crawlers to Crawl the Parquet Files stored inside S3 Bucket.
The Crawled data is then stored inside AWS Glue Data Catalog's tables.

<img width="1129" alt="Screenshot 2023-04-30 at 2 44 53 AM" src="https://user-images.githubusercontent.com/100070155/235324498-0f2790ef-a320-4115-b8a7-ee9b6625ecc6.png">

Crawled Data in AWS Glue Data Catalog Tables: 

<img width="715" alt="Screenshot 2023-04-30 at 2 49 02 AM" src="https://user-images.githubusercontent.com/100070155/235324690-fe8d7d57-4f04-4d02-83d3-5b32474f4dda.png">

Querying Data using Athena: 

Example Query: 

```
SELECT
    FROM_UNIXTIME(recorded_on / 1000) AS recorded_on,
    avg_heart_rate, 
    year
FROM heart_data_parquet
WHERE year = '2022';
```

In the above query, I used `WHERE` Clause to filter the data by `year` to avoid scanning other Partitions. 
Also, `FROM_UNIXTIME()` Function is used to convert the Unix Epoch format to `TIMESTAMP`

Output: 

<img width="940" alt="Screenshot 2023-04-30 at 3 09 23 AM" src="https://user-images.githubusercontent.com/100070155/235325295-f76c6216-cb03-4c54-84d1-d8a0f6bf45b8.png">

6. It also triggers to start an EC2 instance and Launch the Streamlit Dashboard app

<img width="1440" alt="Screenshot 2023-04-30 at 3 20 57 AM" src="https://user-images.githubusercontent.com/100070155/235325583-d23f3be2-4b17-4523-9835-f6b1f66cd27d.png">

You can access the app through the following URL : ```http://<your_instance's_public_ip>:8501```

Replace `your_instance's publicc_ip` with your EC2 instance's Public IPV4 Address. 

Configuring Auto Launcher: 

On the EC2 instance, open crontab editor

```crontab -e```

Add the following line to the editor: 

```@reboot /home/ec2-user/.local/bin/streamlit run /home/ec2-user/<path_to_streamlit_app> --server.port 8501```


Replace the `path_to_streamlit_app` with path to your Streamlit App

Now whenever the EC2 instance restarts, the Streamlit app will automatically run on port `8501`

## Streamlit Dashboard

Here's a quick look of the Streamlit Dashboard hosted on EC2: 

https://user-images.githubusercontent.com/100070155/235328411-224b38f5-185e-41cb-8aeb-579d519f4a13.mov

