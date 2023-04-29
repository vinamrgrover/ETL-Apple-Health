import lxml.etree as ET
import boto3
import time  
import csv
import io

def reset_stream():

    s3 = boto3.client('s3')
	
    response = s3.get_object(
        Bucket = 'apple-health-lake', 
        Key = 'apple_health_export/export.xml'
    )
    
    global streaming_body
    
    streaming_body = response['Body']

def gen_categories():
    for event, elmt in ET.iterparse(streaming_body, events = ('start',), tag = 'Record'):
        if elmt.tag == 'Record':
            yield elmt.attrib['type']

def gen_sleep_elmt():
    for event, elmt in ET.iterparse(streaming_body, events = ('start',), tag = 'Record'):
        if elmt.get('type') == 'HKCategoryTypeIdentifierSleepAnalysis':
            yield elmt

def gen_heart_elmt():
    for event, elmt in ET.iterparse(streaming_body, events = ('start',), tag = 'Record'): 
        if elmt.attrib['type'] == 'HKQuantityTypeIdentifierHeartRate':
            yield elmt

def gen_step_elmt():
    for event, elmt in ET.iterparse(file_name, events = ('start', ), tag = 'Record'):
        if elmt.attrib['type'] == 'HKQuantityTypeIdentifierStepCount':
            yield elmt

def gen_resp_rate_elmt():
    for event, elmt in ET.iterparse(file_name, events = ('start', ), tag = 'Record'):
        if elmt.attrib['type'] == 'HKQuantityTypeIdentifierRespiratoryRate':
            yield elmt



def write_csv_to_s3(bucket_name, key, header : tuple, gen):
    
    reset_stream()
    
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    
    writer.writerow(header)
    
    for row in gen:
        writer.writerow(row)

    s3.put_object(
        Bucket = bucket_name,  
        Key = key,  
        Body = buffer.getvalue()
    )



def lambda_handler(event = None, context = None): 
    heart_data = ((e.attrib['creationDate'], e.attrib['value']) for e in gen_heart_elmt())
    sleep_data = ((e.attrib['creationDate'], e.attrib['startDate'], e.attrib['endDate']) for e in gen_sleep_elmt())
    step_data = ((e.attrib['creationDate'], e.attrib['value']) for e in gen_step_elmt())
    resp_data = ((e.attrib['creationDate'], e.attrib['value']) for e in gen_resp_rate_elmt())
    
    start_time = time.time()
    
    write_csv_to_s3('apple-health-lake', 'processed/Heart_Data.csv', ('created_at', 'value'), heart_data)
    write_csv_to_s3('apple-health-lake', 'processed/Sleep_Data.csv', ('created_at', 'start_date', 'end_date'), sleep_data)
    write_csv_to_s3('apple-health-lake', 'processed/Step_Data.csv', ('created_at', 'count'), step_data)
    write_csv_to_s3('apple-health-lake', 'processed/Resp_Data.csv', ('created_at', 'count'), resp_data)
    
    end_time = time.time()
    time_taken = end_time - start_time
    
    return {
        'StatusCode' : 200, 
        'TimeTaken' : round(time_taken, 2)
    }

