

import json
import time
from azure.eventhub import EventHubProducerClient, EventData
from azure.identity.aio import DefaultAzureCredential
import logging
import pandas as pd

def eh_connection(ehName, ehConnection):
    '''
    Function to connect to Azure Event Hub

    Args:
        ehName (String): Name of the Event Hub
        ehConnection (String): Connection String of the Event Hub

    Return:
        producer (Object): Producer Object for the Event Hub
        e (String): Error Message
    '''
    try:
        connection_str = ehConnection
        eventhub_name = ehName
        producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=eventhub_name)
        print(f">>> Connected Successfully to Event Hub {ehName}")
        logging.info(f">>> Connected Successfully to Event Hub {ehName}")
        return ("Successful", producer)
    
    except Exception as e:
        print(f"<<<ERROR>>> Error while connecting to Event Hub {ehName}, Error Message: {e}")
        logging.info(f"<<<ERROR>>> Error while connecting to Event Hub {ehName}, Error Message: {e}")
        return ("Error", e)

def readCsv(fileName):
    '''
    Function to read CSV

    Args:
        fileName (String): Name of the CSV File
    
    Return:
        df (DataFrame): DataFrame of the CSV
    '''
    df=pd.read_csv(fileName)
    return 'success', df

def ehPublisher(df, producer):
    '''
    Function to publish data to Event Hub

    Args:
        df (DataFrame): DataFrame of the CSV
        producer (Object): Producer Object for the Event Hub

    Return:
        None
    '''
    counter=1        
    for sentiment in df['TweetContent']:    
        data = {
            "TweetContent":sentiment
        }    
        serialized_data=json.dumps(data)
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(serialized_data))
        producer.send_batch(event_data_batch)
        print(f"Published to Azure Event Hub: {counter}")
        print(f"#{counter}. DATA: {data}")
        logging.info(f"Published to Azure Event Hub: {counter}")
        logging.info(f"#{counter}. DATA: {data}")
        if counter >=100:
            exit(0)
        counter=counter+1
        
        #exit(0)

        
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='publishLog.log')
    
    startTime=time.time()
    logging.info(startTime)
    print("*" * 130)
    print("         Initiating Data Extraction from Excel and Sending it to Event Hub (Topic) - Streaming         ")
    print("*" * 130)
    logging.info("*" * 130)
    logging.info("         Initiating Data Extraction from Excel and Sending it to Event Hub (Topic) - Streaming         ")
    logging.info("*" * 130)

    print("Reading Config")
    logging.info("Reading Config")

       
    
    message, df=readCsv(fileName)

    if message=="Error":
        print(">>> Exiting the process due to error in Oracle Connection...")
        logging.info(">>> Exiting the process due to error in Oracle Connection...")
        endTime=time.time()
        elapsedTime=(endTime-startTime)/60
        logging.info(f"End Time: {endTime}")
        logging.info(f"Elapsed Time: {elapsedTime}")
        exit()

    print(f">>> Initiating Event Hub (Topic) {eventhub_name} Connection...")
    logging.info(f">>> Initiating Event Hub (Topic) {eventhub_name} Connection...")
    message, producer=eh_connection(eventhub_name, connection_str)
    
    if message=="Error":
        print(">>> Exiting the process due to error in Event Hub Connection...")
        logging.info(">>> Exiting the process due to error in Event Hub Connection...")
        endTime=time.time()
        elapsedTime=(endTime-startTime)/60
        logging.info(f"End Time: {endTime}")
        logging.info(f"Elapsed Time: {elapsedTime}")
        exit()

    ehPublisher(df, producer)

    endTime=time.time()
    elapsedTime=(endTime-startTime)/60
    print(f"Elapsed Time: {elapsedTime}")
    logging.info(f"End Time: {endTime}")
    logging.info(f"Elapsed Time: {elapsedTime}")
