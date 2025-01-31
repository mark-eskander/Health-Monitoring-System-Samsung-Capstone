import pandas as pd
import json
import time
from kafka import KafkaProducer
import numpy as np
fitdf=pd.read_csv('/opt/spark-app/dataset.csv')
fitdf = fitdf.dropna(axis=1, thresh=0.3*len(fitdf))
fitdf.head()
fitdf = fitdf.drop('Unnamed: 0', axis=1)
fitdf.head()
fitdf['datee'] = pd.to_datetime(fitdf['date'])
fitdf['hour'] = fitdf['hour'].astype(int)
sorted_df = fitdf.sort_values(by=['datee', 'hour'])
sorted_df.head()
sorted_df = sorted_df[['id','temperature', 'date', 'hour', 'calories', 'distance', 'bpm', 'steps', 'age','gender','bmi','datee']]
sorted_df.reset_index(drop=True, inplace=True)


first_non_nan_calories = sorted_df['calories'].first_valid_index()
first_non_nan_calories
sorted_df['date_hour'] = sorted_df['datee'].astype(str) + ' ' + sorted_df['hour'].astype(str)
sorted_df['date_hour'] = pd.to_datetime(sorted_df['date_hour'])
uniqueDateHour = sorted_df['date_hour'].unique()
sorted_df.head()
truncate_df = sorted_df[sorted_df['date_hour'] >= uniqueDateHour[first_non_nan_calories]]
truncate_df.head()
truncate_df.reset_index(drop=True, inplace=True)
sorted_df = truncate_df
sorted_df.head(10)


BROKER_URL = 'kafka:9092'
TOPIC_NAME = 'my-topic'
producer = KafkaProducer(
    bootstrap_servers=BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode('utf-8') 
)
sorted_df['date_hour'] = sorted_df['datee'].astype(str) + ' ' + sorted_df['hour'].astype(str)
sorted_df['date_hour'] = pd.to_datetime(sorted_df['date_hour'])
uniqueDateHour = sorted_df['date_hour'].unique()
print('Number of unique date_hour: ', len(uniqueDateHour))
for i in range(0, len(uniqueDateHour)):
    # print('Date_hour: ', uniqueDateHour[i])
    subset=sorted_df[sorted_df['date_hour'] == uniqueDateHour[i]]
    subset=subset.drop(['datee', 'date_hour'], axis=1)
    subsett=subset.to_dict(orient='records')
    
    producer.send(TOPIC_NAME, value=subsett)
    print('Message sent: ', subsett)
    time.sleep(5)  # Sleep for 1 second
