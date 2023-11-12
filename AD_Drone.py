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
        self.id = None
        self.alias = None
        self.token = None
        self.dado_de_alta = False
        self.autentificado = False
        self.posicion = [0, 0]
        self.posicion_final = [0, 0]
        self.positioned = False


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


producer = KafkaProducer(
    bootstrap_servers=[IP_BROKER + ":" + str(PORT_BROKER)],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


def send(msg, client):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b" " * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)


def send_posicion(posicion):
    producer.send("posiciones", value=posicion)
    producer.flush()


def handle_error(e):
    print(f"Error: {e}")
    sleep(5)


def process_figura_message(figura):
    if figura:
        if figura["Nombre"] == "BASE":
            dron.posicion_final = [0, 0]
            move_to_position()
        else:
            for drone in figura["Drones"]:
                if str(drone["ID"]) == dron.id:
                    pos_x, pos_y = map(int, drone["POS"].split(","))
                    dron.posicion_final = [pos_x, pos_y]
                    move_to_position()


def move_to_position():
    while True:
        if dron.posicion_final != dron.posicion:
            dron.positioned = False
            if dron.posicion_final[0] > dron.posicion[0]:
                dron.posicion[0] += 1
            elif dron.posicion_final[0] < dron.posicion[0]:
                dron.posicion[0] -= 1
            if dron.posicion_final[1] > dron.posicion[1]:
                dron.posicion[1] += 1
            elif dron.posicion_final[1] < dron.posicion[1]:
                dron.posicion[1] -= 1
            posicion = {
                "ID": dron.id,
                "POS": f"{dron.posicion[0]},{dron.posicion[1]}",
                "STATE": "MOVING",
            }
            sleep(1)
            while True:
                try:
                    send_posicion(posicion)
                    break
                except KafkaError as e:
                    handle_error(e)
        else:
            dron.positioned = True
            posicion = {
                "ID": dron.id,
                "POS": f"{dron.posicion[0]},{dron.posicion[1]}",
                "STATE": "POSITIONED",
            }
            while True:
                try:
                    send_posicion(posicion)
                    break
                except KafkaError as e:
                    handle_error(e)
            break


def read_destinos():
    consumer_destinos = KafkaConsumer(
        "destinos",
        bootstrap_servers=[IP_BROKER + ":" + str(PORT_BROKER)],
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
    )
    try:
        while True:
            try:
                # Leer mensaje del topic
                for message in consumer_destinos:
                    # Procesar el mensaje
                    figura = message.value
                    process_figura_message(figura)
            except KafkaError as e:
                handle_error(e)
    finally:
        consumer_destinos.close()


def print_espacio_aereo(espacio):
    if os.name == "nt":
        os.system("cls")
        for row in espacio:
            print("[" + " ".join(row) + "]")
    print("\n")


def read_espacios():
    consumer_espacios = KafkaConsumer(
        "espacios",
        bootstrap_servers=[IP_BROKER + ":" + str(PORT_BROKER)],
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
    )
    try:
        while True:
            try:
                # Leer mensaje del topic
                for message in consumer_espacios:
                    # Procesar el mensaje
                    espacio = message.value
                    print_espacio_aereo(espacio)

            except KafkaError as e:
                handle_error(e)
    finally:
        consumer_espacios.close()


def darse_de_alta():
    if dron.token is not None:
        print("El dron ya está dado de alta.")
    else:
        while True:
            try:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(ADDRR)
                print(f"Establecida conexión en [{ADDRR}]")
                print("Envio al Registry: alta")
                send("alta", client)
                response = client.recv(2048).decode(FORMAT)
                print("Recibo del Registry:", response)
                input = response.split(" ")
                dron.id = input[0]
                dron.alias = input[1]
                dron.token = input[2]
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
    if dron.token is None:
        print("El dron no está dado de alta.")
    else:
        while True:
            try:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(ADDRR)
                print(f"Establecida conexión en [{ADDRR}]")
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
            except ConnectionRefusedError as e:
                handle_error(e)
            except ConnectionResetError as e:
                handle_error(e)


def editar_perfil():
    if dron.token is None:
        print("El dron no está dado de alta.")
    else:
        while True:
            try:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(ADDRR)
                print(f"Establecida conexión en [{ADDRR}]")
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


def recuperar_token():
    if dron.token is not None:
        print("El dron ya tiene un token.")
    else:
        while True:
            try:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(ADDRR)
                print(f"Establecida conexión en [{ADDRR}]")
                dron_id = input("ID del dron a recuperar: ")
                print("Envio al Registry: recuperar", dron_id)
                send(f"recuperar {dron_id}", client)
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


def autentificarse():
    if dron.token is None:
        print("El dron no tiene un token.")
    elif dron.autentificado:
        print("El dron ya está autentificado.")
    else:
        while True:
            try:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(ADDRE)
                print(f"Establecida conexión en [{ADDRE}]")
                print("Envio al Engine: autentificar", dron.id, dron.token)
                send(f"autentificar {dron.id} {dron.token}", client)
                response = client.recv(2048).decode(FORMAT)
                print("Recibo del Engine:", response)
                print("Envio al Engine: FIN")
                send(FIN, client)
                client.close()
                if response == "Autentificación correcta":
                    dron.autentificado = True
                break
            except ConnectionRefusedError:
                print("Engine is not available. Please try again later.")
                sleep(5)
            except ConnectionResetError:
                print(
                    "The connection was closed by the remote host. Please try again later."
                )
                sleep(5)


def start():
    mapa = input("¿Quieres mostrar el mapa en esta terminal? ('si', 'no'): ")
    if mapa == "si":
        thread_read_espacios = threading.Thread(target=read_espacios)
        thread_read_espacios.start()
    thread_read_figuras = threading.Thread(target=read_destinos)
    while True:
        print("¿Qué quieres hacer?")
        print("1. Darse de alta")
        print("2. Darse de baja")
        print("3. Editar perfil")
        print("4. Recuperar token")
        print("5. Autentificarse")
        print("6. Salir")
        opcion = input("Opción: ")

        if opcion == "1":
            darse_de_alta()

        elif opcion == "2":
            darse_de_baja()

        elif opcion == "3":
            editar_perfil()

        elif opcion == "4":
            recuperar_token()

        elif opcion == "5":
            autentificarse()
            if dron.autentificado:
                if not thread_read_figuras.is_alive():
                    thread_read_figuras.start()

        elif opcion == "6":
            print("Saliendo...")
            break

        else:
            print("Opción incorrecta.")


print("[STARTING] Dron inicializándose...")
start()
