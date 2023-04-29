import pandas as pd
import duckdb
import boto3
import json

def lambda_handler(event = None, context = None):
    s3 = boto3.client('s3')
    
    list_resp = s3.list_objects(
        Bucket = 'apple-health-lake'
    )
    
    keys = [obj['Key'] for obj in list_resp['Contents']][1:]

    queries = [
        """
            SELECT
                YEAR(CAST(created_at AS DATE)) AS year, 
                CAST(created_at AS DATE) AS recorded_on, 
                ROUND(AVG(value), 2) AS avg_heart_rate
            FROM df
            GROUP BY CAST(created_at AS DATE)
            ORDER BY year; 
        """, 

        """
            SELECT
                YEAR(CAST(created_at AS DATE)) AS year, 
                CAST(created_at AS DATE) AS recorded_on, 
                ROUND(AVG(count), 2) AS avg_resp_rate
            FROM df
            GROUP BY CAST(created_at AS DATE)
            ORDER BY year; 
        """, 

        """
            SELECT
                YEAR(recorded_on) AS year, 
                CAST(recorded_on AS TIMESTAMP) AS recorded_on,
                start_time,  
                end_time
            FROM(
                SELECT
                    CAST(created_at AS DATE) AS recorded_on,
                    MIN(CAST(LEFT(start_date, 19) AS TIMESTAMP)) AS start_time, 
                    MAX(CAST(LEFT(end_date, 19) AS TIMESTAMP)) AS end_time
                FROM df
                GROUP BY CAST(created_at AS DATE)
                ORDER BY recorded_on
            ) tbl; 
        """, 

        """
            SELECT
                YEAR(CAST(created_at AS DATE)) AS year, 
                CAST(created_at AS DATE) AS created_at, 
                SUM(count) AS total_steps
            FROM df
            GROUP BY CAST(created_at AS DATE)
        """
    ]
    
    
    df_json = dict()
    
    for key, query in zip(keys, queries):
        resp = s3.get_object(
            Bucket = 'apple-health-lake',
            Key = key
        ) 

        df = pd.read_csv(resp['Body'])                                  
        df = duckdb.sql(query).df()
        file_name = key.split('/')[-1].split('.')[0] + '.parquet'
        
        df_json[file_name] = df.to_json()
    
    
    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(
        FunctionName = "To_Parquet",
        Payload = json.dumps(df_json)
        )
    
    response_payload = response['Payload'].read().decode('utf-8')
    
    return response_payload
        
