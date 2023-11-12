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
        self.espacio_aereo = [
            [0] * 20 for _ in range(20)
        ]  # Matriz 20x20 para simular el espacio aéreo
        self.started = False
        self.drones = []


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


# Crear una conexión y un cursor específicos para cada hilo
local = threading.local()


def get_connection():
    if not hasattr(local, "conn"):
        local.conn = sqlite3.connect("database.db")
    return local.conn


def get_cursor():
    conn = get_connection()
    cursor = conn.cursor()
    return cursor


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


def handle_client(conn):
    while True:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == FIN:
                break
            else:
                input = msg.split(" ")
                action = input[0]
                dron_id = input[1]
                dron_token = input[2]
                cursor = get_cursor()

                if (
                    action == "autentificarse"
                    and engine.started == False
                    and len(engine.drones) <= MAX_CONEXIONES
                ):
                    if dron_id in engine.drones:
                        conn.send("Ya estás autentificado".encode(FORMAT))
                    else:
                        cursor.execute(
                            "SELECT token FROM drones WHERE id = ?", (dron_id,)
                        )
                        token = cursor.fetchone()
                        if token:
                            if token[0] == dron_token:
                                conn.send("Autentificación correcta".encode(FORMAT))
                                engine.drones.append(dron_id)

                            else:
                                conn.send("Autentificación erronea".encode(FORMAT))

                        else:
                            conn.send("Autentificacion erronea".encode(FORMAT))

                elif action == "autentificarse" and engine.started == True:
                    conn.send(
                        "OOppsss... COMENZÓ EL ESPECTÁCULO. Tendrás que esperar a que termine".encode(
                            FORMAT
                        )
                    )

                elif action == "autentificarse" and len(engine.drones) > MAX_CONEXIONES:
                    conn.send(
                        "OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(
                            FORMAT
                        )
                    )

    conn.close()


def handle_conexions():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn,))
        thread.start()


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
                            sent_figuras.append(figura)
                            break
                        except KafkaError as e:
                            handle_error(e)
            sleep(10)  # wait for 10 seconds before checking for new figuras
            if len(sent_figuras) == len(engine.figuras):
                print("Todas las figuras han sido enviadas")
                break
    finally:
        producer.close()


def start():
    print(f"[LISTENING] Engine a la escucha en {SERVER}:{PORT}")
    thread_handle_conexions = threading.Thread(target=handle_conexions)
    thread_handle_conexions.start()
    while True:
        start_input = input("Escriba 'start' para iniciar el espectaculo: ")
        if start_input == "start":
            engine.started = True
            break
    thread_read_json = threading.Thread(target=read_json)
    thread_read_json.start()
    thread_send_figuras = threading.Thread(target=send_figuras)
    thread_send_figuras.start()


print("[STARTING] Engine inicializándose...")
start()
