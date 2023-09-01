import pandas as pd                         # Python library for data analysis.
import pandas_gbq                           # library that allows you to easily load data from Google BigQuery into pandas DataFrames
import numpy as np                          # library that provides support for mathematical operations on arrays.

from google.cloud import storage, bigquery  # Interact with Google Cloud Storage, Bigquery

def hello_gcs(event,context):               # Event-The event that triggered the function, Context-The execution environment
    
    print(event) 
    print(context)                          # Prints the Event and Context Dictionaries to the Console
    
    bucket_name = event['bucket']
    file_name = event['name']
    print(f"gs://{bucket_name}/{file_name}")
    df = pd.read_csv(f"gs://{bucket_name}/{file_name}") # reads the data from the file into a pandas DataFrame
    
    df.columns = df.columns.str.replace('\W', '_',regex=True)
    df.replace(np.nan,"", inplace=True) #replace all NaN values in the DataFrame with an empty string
    df.columns = df.columns.str.lower()
    if 'attended' in df.columns: #if the attended column exists in DataFrame. If it does,the code converts the column to Bool values.
        df['attended'] = df['attended'].map({'TRUE': True, 'FALSE': False})
    
    df['file_name'] = str(event['name']) #contains the file name of the file that was uploaded to Cloud Storage
    
    pandas_gbq.to_gbq(df, "checkpoint03.checkpoint", project_id="prj-gradient-surya", if_exists='append')
