import sys
import socket
import threading
import sqlite3
from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
from time import sleep
from kafka.errors import KafkaError
import os


class Engine:
    def __init__(self):
        self.figuras = []
        self.espacio_aereo = [
            [" "] * 20 for _ in range(20)
        ]  # Matriz 20x20 para simular el espacio aéreo
        self.started = False
        self.drones = []
        self.drones_en_posicion = []
        self.moviendose = False
        self.temperatura = None


engine = Engine()


espacio_aereo_lock = threading.Lock()


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


def handle_client(conn, addr):
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
                    action == "autentificar"
                    and engine.started == False
                    and len(engine.drones) <= MAX_CONEXIONES
                ):
                    if dron_id in engine.drones:
                        conn.send("Ya estás autentificado".encode(FORMAT))
                    else:
                        cursor.execute(
                            "SELECT token FROM drones WHERE dron_id = ?", (dron_id,)
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
                elif (
                    action == "autentificar"
                    and engine.started == True
                    and len(engine.drones) <= MAX_CONEXIONES
                    and dron_id in engine.drones
                ):
                    cursor.execute(
                        "SELECT token FROM drones WHERE dron_id = ?", (dron_id,)
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

                elif action == "autentificar" and engine.started == True:
                    conn.send(
                        "OOppsss... COMENZÓ EL ESPECTÁCULO. Tendrás que esperar a que termine".encode(
                            FORMAT
                        )
                    )

                elif action == "autentificar" and len(engine.drones) > MAX_CONEXIONES:
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
        thread = threading.Thread(target=handle_client, args=(conn, addr))
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
                            if engine.moviendose == False:
                                send_figura(figura)
                                engine.moviendose = True
                                sent_figuras.append(figura)
                                break
                        except KafkaError as e:
                            handle_error(e)
            if engine.moviendose == False:
                sleep(10)
                if len(sent_figuras) == len(engine.figuras):
                    base_msj = {"Nombre": "BASE"}
                    send_figura(base_msj)
                    engine.moviendose = True
    finally:
        producer.close()


def process_position_message(posicion):
    if posicion:
        dron_id = posicion["ID"]
        dron_posicion_x, dron_posicion_y = map(int, posicion["POS"].split(","))
        dron_estado = posicion["STATE"]
        with (
            espacio_aereo_lock
        ):  # Adquirir el lock antes de modificar engine.espacio_aereo
            for i in range(20):
                for j in range(20):
                    if engine.espacio_aereo[i][j] == dron_id:
                        engine.espacio_aereo[i][j] = " "
            engine.espacio_aereo[dron_posicion_x][dron_posicion_y] = dron_id
        if dron_estado == "POSITIONED":
            engine.drones_en_posicion.append(dron_id)
        if len(engine.drones_en_posicion) == len(engine.drones):
            engine.drones_en_posicion = []
            sleep(5)
            engine.moviendose = False


def print_espacio_aereo():
    while True:
        if os.name == "nt":
            os.system("cls")
        with espacio_aereo_lock:  # Adquirir el lock antes de leer engine.espacio_aereo
            for row in engine.espacio_aereo:
                print("[" + " ".join(row) + "]")
        print("\n")
        sleep(0.2)


def read_positions():
    consumer_posiciones = KafkaConsumer(
        "posiciones",
        bootstrap_servers=[f"{IP_BROKER}:{PORT_BROKER}"],
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
    )
    try:
        while True:
            try:
                # Leer mensaje del topic
                for message in consumer_posiciones:
                    # Procesar el mensaje
                    posicion = message.value
                    process_position_message(posicion)
            except KafkaError as e:
                handle_error(e)
    finally:
        consumer_posiciones.close()


def handle_weather():
    while True:
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(ADDRW)
            print(f"Establecida conexión en [{ADDRW}]")
            while True:
                send("get", client)
                response = client.recv(2048).decode(FORMAT)
                engine.temperature = float(response)
                sleep(5)
        except ConnectionRefusedError as e:
            handle_error(e)
        except ConnectionResetError as e:
            handle_error(e)


def start():
    print(f"[LISTENING] Engine a la escucha en {SERVER}:{PORT}")
    thread_handle_conexions = threading.Thread(target=handle_conexions)
    thread_handle_conexions.start()
    thread_handle_weather = threading.Thread(target=handle_weather)
    thread_handle_weather.start()
    while True:
        start_input = input("Escriba 'start' para iniciar el espectaculo: ")
        if engine.temperatura<0:
            print("No se puede iniciar el espectaculo con temperaturas bajo cero,pruebe mas tarde")
        else:
            if start_input == "start":
                engine.started = True
                break
    thread_read_json = threading.Thread(target=read_json)
    thread_read_json.start()
    thread_read_positions = threading.Thread(target=read_positions)
    thread_read_positions.start()
    thread_send_figuras = threading.Thread(target=send_figuras)
    thread_send_figuras.start()
    thead_print_espacio_aereo = threading.Thread(target=print_espacio_aereo)
    thead_print_espacio_aereo.start()


print("[STARTING] Engine inicializándose...")
start()
