import sys
import socket
import uuid
from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
import threading
from time import sleep
import os
from kafka.errors import KafkaError


class Drone:
    def __init__(self):
        self.id = str(uuid.uuid4())  # Genera un UUID único
        self.dado_de_alta = False
        self.autentificado = False
        self.alias = None
        self.token = None
        self.estado = False
        self.position = [0, 0]
        self.positionfin = [0, 0]


dron = Drone()


IP_ENGINE = sys.argv[1]
PORT_ENGINE = int(sys.argv[2])
IP_BROKER = sys.argv[3]
PORT_BROKER = int(sys.argv[4])
IP_REGISTRY = sys.argv[5]
PORT_REGISTRY = int(sys.argv[6])
ADDRR = (IP_REGISTRY, PORT_REGISTRY)
ADDRE = (IP_ENGINE, PORT_ENGINE)
HEADER = 64
FORMAT = "utf-8"
FIN = "FIN"


consumer_destinos = KafkaConsumer(
    "destinos",
    bootstrap_servers=[IP_BROKER + ":" + str(PORT_BROKER)],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    group_id='drone-' + dron.id,
)


def send(msg, client):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b" " * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)


def process_message(message):
    # Procesar el mensaje
    print(message)


def handle_error(e):
    print(f"Error: {e}")
    sleep(5)


def read_figuras():
    try:
        while True:
            try:
                # Leer mensaje del topic
                for message in consumer_destinos:
                    # Procesar el mensaje
                    process_message(message.value)
                    # Confirmar el mensaje
                    consumer_destinos.commit()
            except KafkaError as e:
                handle_error(e)
    finally:
        consumer_destinos.close()


def darse_de_alta():
    while True:
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(ADDRR)
            print(f"Establecida conexión en [{ADDRR}]")

            if dron.token is not None:
                print("El dron ya está dado de alta.")
            else:
                alias = input("Alias del dron: ")
                dron.alias = alias
                print(f"ID único del dron: {dron.id}")
                print("Envio al Registry: alta", dron.id, dron.alias)
                send(f"alta {dron.id} {dron.alias}", client)
                response = client.recv(2048).decode(FORMAT)
                print("Recibo del Registry:", response)
                dron.token = response

            print("Envio al Registry: FIN")
            send(FIN, client)
            client.close()
            dron.dado_de_alta = True
            break
        except ConnectionRefusedError:
            print("Registry is not available. Please try again later.")
            sleep(5)
        except ConnectionResetError:
            print(
                "The connection was closed by the remote host. Please try again later."
            )
            sleep(5)


def darse_de_baja():
    while True:
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(ADDRR)
            print(f"Establecida conexión en [{ADDRR}]")

            if dron.token is None:
                print("El dron no está dado de alta.")
            else:
                dron_id = input("ID del dron a dar de baja: ")
                print("Envio al Registry: baja", dron_id)
                send(f"baja {dron_id}", client)
                response = client.recv(2048).decode(FORMAT)
                print("Recibo del Registry:", response)
                dron.token = None

            print("Envio al Registry: FIN")
            send(FIN, client)
            client.close()
            dron.dado_de_alta = False
            break
        except ConnectionRefusedError:
            print("Registry is not available. Please try again later.")
            sleep(5)
        except ConnectionResetError:
            print(
                "The connection was closed by the remote host. Please try again later."
            )
            sleep(5)


def editar_perfil():
    while True:
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(ADDRR)
            print(f"Establecida conexión en [{ADDRR}]")

            if dron.token is None:
                print("El dron no está dado de alta.")
            else:
                dron_id = input("ID del dron a editar: ")
                new_alias = input("Nuevo alias: ")
                print("Envio al Registry: editar", dron_id, new_alias)
                send(f"editar {dron_id} {new_alias}", client)
                response = client.recv(2048).decode(FORMAT)
                print("Recibo del Registry:", response)
                dron.alias = new_alias

            print("Envio al Registry: FIN")
            send(FIN, client)
            client.close()
            break
        except ConnectionRefusedError:
            print("Registry is not available. Please try again later.")
            sleep(5)
        except ConnectionResetError:
            print(
                "The connection was closed by the remote host. Please try again later."
            )
            sleep(5)


def start():
    theard_read_figuras = threading.Thread(target=read_figuras)
    theard_read_figuras.start()
    while True:
        print("¿Qué quieres hacer?")
        print("1. Darse de alta")
        print("2. Darse de baja")
        print("3. Editar perfil")
        print("4. Salir")
        opcion = input("Opción: ")

        if opcion == "1":
            darse_de_alta()

        elif opcion == "2":
            darse_de_baja()

        elif opcion == "3":
            editar_perfil()

        elif opcion == "4":
            print("Saliendo...")
            break

        else:
            print("Opción incorrecta.")


print("[STARTING] Dron inicializándose...")
start()
