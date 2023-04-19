# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 09:38:17 2023

@author: hp
"""
import requests
import pandas as pd
import json
import psycopg2 as pg2


def file_to_dataframe(file_path):
    df = pd.read_csv(file_path, delimiter=',')
    
    # Save the DataFrame as a CSV file
    df.to_csv(file_path, index=False)
    print(f"DataFrame saved as {file_path}")

    return df 

def insert_df_to_table(df, table_name, dbname, user, password, host, port):

    conn = pg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()
    cols = ",".join([f'"{i}"' for i in df.columns.tolist()])
    for index, row in df.iterrows():
        values = row.to_dict()
        print(values.values())
        placeholders = ",".join(["%s" for _ in range(len(values))])  # Update
        sql = f'INSERT INTO {table_name} ({cols}) VALUES ({placeholders})'  # Update
        print(sql)
        cur.execute(sql, list(values.values()))
    conn.commit()
    cur.close()
    conn.close()



def read_table_to_df(table_name, dbname, user, password, host, port):
    
    conn = pg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name}")
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=cols)
    cur.close()
    conn.close()
    return df

def send_nginx_request(url, method='GET', headers=None, data=None):
    try:
        response = requests.request(method, url, headers=headers, data=data)
        return response
    except Exception as e:
        return f"Error: {e}"


def get_nginx_response(url):
   
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Failed to retrieve response. HTTP status code: {response.status_code}")
        return None

def process_data(file_path,url, table_name):
    # Read data from URL
    d1 = pd.read_csv(file_path)

    # Convert data to JSON
    d2 = d1.to_json(orient='split', index=False)
    
    d3 = json.loads(d2)
    
    df1 = pd.DataFrame(d3['data'], columns = d3['columns'])
    

    d = {'query': f'select * from {table_name}'}

    # Set headers for POST request
    headers = {'content_type': 'application/json'}

    # Send POST request
    r = requests.post(url, data=json.dumps(d3), headers=headers)

    return r.text







