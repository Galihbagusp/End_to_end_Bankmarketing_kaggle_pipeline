from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer as AvroSerializer
import os
import csv
from time import sleep
import gcsfs

credential_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path
bucket = 'de_bucket-1'
fs = gcsfs.GCSFileSystem(project= GCP_PROJECT_ID)

def load_avro_schema_from_file():
    value_schema = avro.load(
        "/End_to_end_Bankmarketing_kaggle_pipeline/source/Stream_processing/bank.avsc")

    return value_schema


def send_record():
    value_schema = load_avro_schema_from_file()

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1"
    }

    producer = AvroProducer(
        producer_config, default_value_schema=value_schema)

    file = fs.open(f'{bucket}/data_lake-us/bank-additional-full.csv', 'r')
    csvreader = csv.reader(file, delimiter=';')
    header = next(csvreader)
    for row in csvreader:
        value = {
            "age": int(row[0]),
            "job": str(row[1]),
            "marital": str(row[2]),
            "education": str(row[3]),
            "default": str(row[4]),
            "housing": str(row[5]),
            "loan": str(row[6]),
            "contact": str(row[7]),
            "month": str(row[8]),
            "day_of_week": str(row[9]),
            "duration": int(row[10]),
            "campaign": int(row[11]),
            "pdays": int(row[12]),
            "previous": int(row[13]),
            "poutcome": str(row[14]),
            "emp_var_rate": float(row[15]),
            "cons_price_idx": float(row[16]),
            "cons_conf_idx": float(row[17]),
            "euribor3m": float(row[18]),
            "nr_employed": float(row[19]),
            "y":str(row[20])
        }

        try:
            producer.produce(
                topic='raw.bank', key=None, value=value)
        except Exception as e:
            print(f"Exception while producing record value - {value}: {e}")
        else:
            print(f"Successfully producing record value - {value}")

        producer.flush()
        sleep(1)


if __name__ == "__main__":
    send_record()
