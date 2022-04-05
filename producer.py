
import json
import time
from time import time, sleep
from confluent_kafka import Producer
import uuid
from datetime import datetime
import os 


KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')


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
    return {
            "eventId": str(uuid.uuid4()),
            "eventDate": str(datetime.now()),
                "businessProcess": "PolicyEnrollment",
                "eventClass": "CaseEvent",
                "eventType": "CaseChange",
                "eventStatus": "Success",
                "market": "Broad",
                "sourceSystem": "Intake",
                "sourceState": {
                "3rdParty": "LexisNexis",
                "3rdPartyService": "Risk Classifier"
                }
    }


cron(10)