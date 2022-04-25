
import json
import time
from time import time, sleep
from confluent_kafka import Producer
import uuid
from datetime import datetime
import os 
import random


KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

# Streaming Data Configurations
frequency = int(input('Frequency of message generation in seconds: '))

producer = Producer({
    'bootstrap.servers': KAFKA_BROKER,
    'socket.timeout.ms': 100,
    'api.version.request': 'false',
    'broker.version.fallback': '0.9.0',
    'message.max.bytes': 1000000000
})

def cron(event_time):
    while True:
        lambda_handler()
        sleep(event_time)


def lambda_handler():
    start_time = int(time() * 1000)

    send_msg_async(data_builder())
    #print(data_builder())

    end_time = int(time() * 1000)
    time_taken = (end_time - start_time) / 1000
    print("Time taken to complete = %s seconds" % (time_taken))





def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))


def send_msg_async(msg):
    print("Sending message")
    try:
        msg_json_str = str({"data": json.dumps(msg)})
        producer.produce(
            KAFKA_TOPIC,
            msg_json_str,
            callback=lambda err, original_msg=msg_json_str: delivery_report(err, original_msg
                                                                            ),
        )
        producer.flush()
    except Exception as ex:
        print("Error : ", ex)



def data_builder():
        dateToday = datetime.now()
        currTimeSuffix = dateToday.strftime("%Y-%m-%d %H:%M:%S")
        CurrencyList = ['INR', 'USD', 'GBP', 'CAD', 'AED', 'JPY']
        return {
                "SaleID":'GKS' + str("%03d" % random.randrange(2, 200)),
                "Product_ID" : 'P' + str("%03d" % random.randrange(1, 30)),
                "QuantitySold" : random.randrange(1, 10),
                "Vendor_ID" : 'GV' + str("%03d" % random.randrange(1, 20)),
                "SaleDate" : currTimeSuffix,
                "Sale_Amount" : '',
                "Currency" : random.choice(CurrencyList)
        }


cron(frequency)