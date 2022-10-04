import pandas as pd
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
import json




FILE_PATH = "C:/Users/samre/INTERVIEW/Big Data Course/Kafka/Assign1/orders.csv"


API_KEY = 'BMUE2KUHURTCVZCW'
API_SECRET_KEY = 'e5NpHdFYKBk2tWsdb68Ba9jhZG7nyIG8FsVGAIeFDrzOLpzqjKXxex3stfGPxQmw'
BOOTSTRAP_SERVER = 'pkc-lzvrd.us-west4.gcp.confluent.cloud:9092'

SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'

ENDPOINT_SCHEMA_URL  = 'https://psrc-mw731.us-east-2.aws.confluent.cloud'
SCHEMA_REGISTRY_API_KEY = 'XBBOB6YE6BQISWWU'
SCHEMA_REGISTRY_API_SECRET = 'DRmq1oxm6HU2DR3UpFcW35676I8gRlsw1xqwTT5PoDN4MpQhsDbHEgOINOL1hE1C'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf

def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info': f'{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}'

    }


def main(topic):
    schema_registry_conf= schema_config()
    schema_registry_client= SchemaRegistryClient(schema_registry_conf)
    
    schemaStr= schema_registry_client.get_latest_version(topic+"-value").schema.schema_str
    
    
    cols= list(json.loads(schemaStr)['properties'].keys())
    
    json_deserializer= JSONDeserializer(schemaStr)
    
    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    
    totalrecords= 0
    
    output= pd.DataFrame(columns=cols)
    
    while True:
        try:
            
            msg = consumer.poll(0.1)
            if msg is None:
                continue

            row = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            
            dff=pd.DataFrame([row])
            output=pd.concat([output,dff])

            if row is not None:
                print("User record {}: car: {}, partition: {}, offset: {} \n"
                      .format(msg.key(), row, msg.partition(), msg.offset() ))
                totalrecords+=1
        except KeyboardInterrupt:
            print("----------------\n Total records read= ",totalrecords,'\n----------------')
            print("Writing records to file output.csv")
            
            output.to_csv("C:/Users/samre/INTERVIEW/Big Data Course/Kafka/Assign1/output.csv",index=False\
                          ,header=False,mode='a')
            break

    consumer.close()
    

    
main('restaurant-take-away-data')