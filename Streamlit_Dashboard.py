import pandas as pd 
import boto3
import duckdb
import streamlit as st
import matplotlib.pyplot as plt
from datetime import datetime
import calendar

s3 = boto3.client('s3')
list_resp = s3.list_objects(
    Bucket = 'apple-health-lake'
)
keys = [obj['Key'] for obj in list_resp['Contents']][1:]

resp = s3.get_object(
    Bucket = 'apple-health-lake',
    Key = keys[2]
)
sleep_df = pd.read_csv(resp['Body'])

sleep_df = duckdb.sql("""
SELECT
    YEAR(recorded_on) AS year,
    recorded_on,
    start_time,
    end_time
FROM(
    SELECT
        CAST(created_at AS DATE) AS recorded_on,
        MIN(CAST(LEFT(start_date, 19) AS TIMESTAMP)) AS start_time,
        MAX(CAST(LEFT(end_date, 19) AS TIMESTAMP)) AS end_time
    FROM sleep_df
    GROUP BY CAST(created_at AS DATE)
    ORDER BY recorded_on
) tbl;
""").df()


convert_seconds = lambda time : time.hour * 3600 + time.minute * 60 + time.second
          
def sleep_chart_start(year):  
    sleep_df['start_seconds'] = sleep_df['start_time'].apply(convert_seconds)
    sleep_df_tmp = sleep_df[sleep_df['year'] == int(year)]
    sleep_df_tmp.rename(columns = {'recorded_on' : 'Month'}, inplace = True)

    grouped_df = sleep_df_tmp.groupby(sleep_df_tmp['Month'].dt.month).agg({'start_seconds' : 'mean'})
    grouped_df = grouped_df['start_seconds'] / 3600
    month_nums = pd.Series(grouped_df.index)
    grouped_df.name = 'Sleep Hours'
    grouped_df = grouped_df.reset_index() 
    return grouped_df

def sleep_chart_total(year):
    base_date = pd.to_datetime('1900-01-01')
    sleep_df.rename(columns = {'recorded_on' : 'Month'}, inplace = True)

    sleep_df['total_time'] = sleep_df['end_time'] - sleep_df['start_time']
    sleep_df['total_time'] = base_date + sleep_df['total_time']
    sleep_df['Sleep_Hours'] = sleep_df['total_time'].apply(convert_seconds)

    grouped_df = sleep_df[sleep_df['year'] == int(year)].groupby(sleep_df['Month'].dt.month).agg({'Sleep_Hours' : 'mean'})
    grouped_df['Sleep_Hours'] = grouped_df['Sleep_Hours'] / 3600 

    grouped_df.name = 'Sleep_Hours'
    return grouped_df.reset_index()

def sleep_chart_end(year):  
    sleep_df['end_seconds'] = sleep_df['end_time'].apply(convert_seconds)
    sleep_df_tmp = sleep_df[sleep_df['year'] == int(year)]
    sleep_df_tmp.rename(columns = {'recorded_on' : 'Month'}, inplace = True)

    grouped_df = sleep_df_tmp.groupby(sleep_df_tmp['Month'].dt.month).agg({'end_seconds' : 'mean'})
    grouped_df = grouped_df['end_seconds'] / 3600
    month_nums = pd.Series(grouped_df.index)
    grouped_df.name = 'Sleep Hours'
    grouped_df = grouped_df.reset_index() 
    return grouped_df

c1, c2, c3 = st.columns(3)
month_name = lambda month_num : calendar.month_name[month_num]

st.title("Sleep Analysis Dashboard")

selected_option = st.selectbox('Select Category', ['Total Sleep Time', 'Start Sleep Time', 'End Sleep Time'])
selected_year = st.selectbox('Select Year', ['2022', '2023'])

                                        
if selected_option == 'Total Sleep Time':
    with c1:                                                
        grouped_df = sleep_chart_total(selected_year)
        st.line_chart(grouped_df, x = 'Month', y = 'Sleep_Hours')
        grouped_df['Month'] = grouped_df['Month'].apply(month_name)
    with c2:  
        grouped_df.rename(columns = {'Sleep_Hours' : 'AVG Sleep Hours'}, inplace =  True)
        st.dataframe(grouped_df)
    with c3:
        best_month = grouped_df.max()['Month']
        max_avg = grouped_df.max()['AVG Sleep Hours']
        min_avg = grouped_df.min()['AVG Sleep Hours']
        st.metric(label = 'Best Month', value = best_month)
        st.metric(label = 'Max Average Sleep', value = str(round(max_avg, 2)) + ' Hours')
        st.metric(label = 'Min Average Sleep', value = str(round(min_avg, 2)) + ' Hours')

elif selected_option == 'Start Sleep Time':
    with c1:
        grouped_df = sleep_chart_start(selected_year)
        st.line_chart(grouped_df, x = 'Month', y = 'Sleep Hours')
        grouped_df['Month'] = grouped_df['Month'].apply(month_name)
    with c2:
        grouped_df.rename(columns = {'Sleep Hours' : 'AVG Start Time'}, inplace =  True)
        st.dataframe(grouped_df)

else:
    with c1: 
        grouped_df = sleep_chart_end(selected_year)
        st.line_chart(grouped_df, x = 'Month', y = 'Sleep Hours')
    with c2:
        grouped_df['Month'] = grouped_df['Month'].apply(month_name)
        grouped_df.rename(columns = {'Sleep Hours' : 'AVG End Time'}, inplace =  True)
        st.dataframe(grouped_df)

