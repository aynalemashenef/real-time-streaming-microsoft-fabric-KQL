#!/usr/bin/env python
# coding: utf-8

# ## fetching-crypto-live-data-notebook
# 
# null

# In[5]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install azure-servicebus==7.11.4


# In[6]:


import requests
import json
import time
from datetime import datetime
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import pytz

# Replace with your Fabric EventStream connection string
myconnectionstring = "<insert your EventStream SAS key>"

# API URL - Change this to any API you want to use
API_URL = "https://api.binance.us/api/v3/ticker/price?symbols=[%22BTCUSDT%22,%22ETHUSDT%22,%22SOLUSDT%22,%22AVAXUSDT%22]"  # Example: Binance API
# API_URL = "https://api.wheretheiss.at/v1/satellites/25544"  # Example: ISS API

# Function to fetch data from any API
def fetch_api_data():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()  # Raises an error if the request fails
        data = response.json()

        # Convert single object response to a list for consistency
        if isinstance(data, dict):
            return [data]  # Wrap single dictionary in a list

        return data  # Return list as-is
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

# Function to add timestamps in Sydney, Australia time
def add_timestamps(data):
    sydney_tz = pytz.timezone("Australia/Sydney")
    now_sydney = datetime.now(sydney_tz)

    formatted_datetime = now_sydney.strftime("%m/%d/%Y %I:%M:%S %p")  # MM/DD/YYYY HH:MM:SS AM/PM
    date_column = now_sydney.strftime("%d-%m-%Y")  # DD-MM-YYYY
    time_column = now_sydney.strftime("%H:%M:%S")  # HH:MM:SS

    for record in data:
        record["datetime"] = formatted_datetime
        record["date"] = date_column
        record["time"] = time_column

    return data

# Function to send processed data to Microsoft Fabric EventStream
def send_to_eventstream(messages, connection_string):
    # Extract EntityPath from connection string
    entity_path = None
    for param in connection_string.split(';'):
        if param.startswith('EntityPath='):
            entity_path = param.split('=')[1]
            break

    if not entity_path:
        raise ValueError("EntityPath not found in connection string. Please check your connection details.")

    # Ensure data is always a list before sending
    if isinstance(messages, dict):
        messages = [messages]  # Convert single object to a list

    # Establish connection to Fabric EventStream
    servicebus_client = ServiceBusClient.from_connection_string(connection_string)
    try:
        with servicebus_client.get_queue_sender(entity_path) as sender:
            # Convert messages to JSON format
            batch_message = [ServiceBusMessage(json.dumps(msg)) for msg in messages]
            sender.send_messages(batch_message)
            print(f"Successfully sent {len(messages)} records to EventStream.")
    except Exception as e:
        print(f"Error sending messages: {e}")
    finally:
        servicebus_client.close()

# Infinite loop to fetch and send data every 2 seconds
print(f"Starting real-time data streaming from {API_URL} to Fabric...")
while True:
    data = fetch_api_data()  # Fetch data from API
    if data:
        processed_data = add_timestamps(data)  # Add date and time
        send_to_eventstream(processed_data, myconnectionstring)  # Send data to EventStream
        print(f"Sent {len(processed_data)} records at {processed_data[0]['datetime']}")
    time.sleep(2)  # Wait for 2 seconds before fetching new data




