import awswrangler as wr
import pandas as pd 
import boto3

def lambda_handler(event, context):
    for key, val in event.items(): 
        df = pd.read_json(val)
        
        wr.s3.to_parquet(df, path = f's3://apple-health-lake/transformed/{key}', partition_cols = ['year'], dataset = True)
        
    glue = boto3.client('glue')
    glue.start_crawler(Name = 'Heart_Crawler')
    glue.start_crawler(Name = 'Resp_Crawler')
    glue.start_crawler(Name = 'Sleep_Crawler')
    glue.start_crawler(Name = 'Step_Crawler')
    
    ec2 = boto3.client('ec2')
    ec2.start_instances(InstanceIds = ['i-08d4e98fbb21593e8'])
    
    return {
        'StatusCode' : 200 
        }
    
