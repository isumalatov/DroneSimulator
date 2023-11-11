import sys
import socket
import threading
import sqlite3
from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
from time import sleep
from kafka.errors import KafkaError


class Engine:
    def __init__(self):
        self.figuras = []


engine = Engine()


PORT = int(sys.argv[1])
MAX_CONEXIONES = int(sys.argv[2])
IP_BROKER = sys.argv[3]
PORT_BROKER = int(sys.argv[4])
IP_ADWEATHER = sys.argv[5]
PORT_ADWEATHER = int(sys.argv[6])
ADDRW = (IP_ADWEATHER, PORT_ADWEATHER)
HEADER = 64
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = "utf-8"
FIN = "FIN"


producer = KafkaProducer(
    bootstrap_servers=[f"{IP_BROKER}:{PORT_BROKER}"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


def send(msg, client):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b" " * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)


def read_json():
    while True:
        try:
            with open("AwD_figuras_Correccion.json") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    print(
                        "Error: The file 'AwD_figuras_Correccion.json' is not a valid JSON file."
                    )
                    break
            new_figuras = data["figuras"]
            for figura in new_figuras:
                if figura not in engine.figuras:
                    engine.figuras.append(figura)
        except FileNotFoundError:
            print(
                "Error: The file 'AwD_figuras_Correccion.json' was not found. Retrying in 5 seconds..."
            )
        except PermissionError:
            print(
                "Error: No permission to read the file 'AwD_figuras_Correccion.json'."
            )
        except IsADirectoryError:
            print("Error: 'AwD_figuras_Correccion.json' is a directory, not a file.")
        sleep(5)  # wait for 5 seconds before checking the file again


def send_figura(figura):
    producer.send("destinos", value=figura)
    producer.flush()


def handle_error(e):
    print(f"Error: {e}")
    sleep(5)


def send_figuras():
    sent_figuras = []  # keep track of the figuras that have been sent
    try:
        while True:
            for figura in engine.figuras:
                if figura not in sent_figuras:
                    while True:
                        try:
                            send_figura(figura)
                            sent_figuras.add(figura)  # mark the figura as sent
                            break
                        except KafkaError as e:
                            handle_error(e)
                sleep(5)  # wait for 5 seconds before sending the next figura
            sleep(5)  # wait for 5 seconds before checking for new figuras
    finally:
        producer.close()


def start():
    print(f"[LISTENING] Engine a la escucha en {SERVER}:{PORT}")
    thread_read_json = threading.Thread(target=read_json)
    thread_read_json.start()
    thread_send_figuras = threading.Thread(target=send_figuras)
    thread_send_figuras.start()


print("[STARTING] Engine inicializándose...")
start()
