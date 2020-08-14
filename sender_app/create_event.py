import time 
import asyncio 
import configparser 
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

# config 
config = configparser.ConfigParser()
config.read('../app_config/EVENTHUB_CONFIG.INI')
EVENTHUB_NAME = config['EVENTHUB_ACCOUNT']['EVENTHUB_NAME']
EVENTHUB_CONN_STR = config['EVENTHUB_ACCOUNT']['EVENTHUB_CONN_STR']

async def run():
    '''
        Send events to Azure Data Hubs
    '''
    
    i = 0
    timeout = time.time() + 60*60 #60 minutes from now
    while time.time() < timeout:
        producer = EventHubProducerClient.from_connection_string(EVENTHUB_CONN_STR)
        
        i = i +1
        async with producer:
            event_data_batch = await producer.create_batch()
        
            event_data_batch.add(EventData('{ "sensor": "temperature", "value": "32"}'))
            event_data_batch.add(EventData('{ "sensor": "humidity", "value": "0.5"}'))
        
            await producer.send_batch(event_data_batch)
            print("Event Sent: \"{}\"".format(i))
        
if __name__ == "__main__":
   loop = asyncio.get_event_loop()
   loop.run_until_complete(run())
