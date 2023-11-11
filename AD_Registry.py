import sys
import socket
import threading
import sqlite3
import uuid


PORT = int(sys.argv[1])
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
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS drones (
            dron_id TEXT PRIMARY KEY,
            alias TEXT,
            token TEXT
        )
    """
    )
    return cursor


def drop_table():
    cursor = get_cursor()
    cursor.execute("DROP TABLE IF EXISTS drones")
    get_connection().commit()


def get_numero_drones():
    cursor = get_cursor()
    cursor.execute("SELECT COUNT(*) FROM drones")
    return cursor.fetchone()[0]


def generate_token():
    return str(uuid.uuid4())


def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")

    while True:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)

            if msg == FIN:
                break

            input = msg.split(" ")
            action = input[0]

            cursor = get_cursor()

            if action == "alta":
                dron_id = get_numero_drones() + 1
                alias = "Dron" + str(dron_id)
                token = generate_token()
                cursor.execute(
                    "INSERT INTO drones (dron_id, alias, token) VALUES (?, ?, ?)",
                    (dron_id, alias, token),
                )
                conn.send(f"{dron_id} {alias} {token}".encode(FORMAT))

            elif action == "baja":
                dron_id = input[1]
                cursor.execute("DELETE FROM drones WHERE dron_id = ?", (dron_id,))
                conn.send(f"¡Dron con ID {dron_id} dado de baja!".encode(FORMAT))

            elif action == "editar":
                dron_id = input[1]
                new_alias = input[2]
                cursor.execute(
                    "UPDATE drones SET alias = ? WHERE dron_id = ?",
                    (new_alias, dron_id),
                )
                conn.send(
                    f"¡Alias del dron con ID {dron_id} actualizado a {new_alias}!".encode(
                        FORMAT
                    )
                )

            elif action == "recuperar":
                dron_id = input[1]
                cursor.execute("SELECT token FROM drones WHERE dron_id = ?", (dron_id,))
                token = cursor.fetchone()
                conn.send(token[0].encode(FORMAT))

            else:
                conn.send("Acción no válida".encode(FORMAT))

            get_connection().commit()  # Commit los cambios en la base de datos

    conn.close()


def start():
    print(f"[LISTENING] Registry a la escucha en {SERVER}:{PORT}")
    borrar_tabla_input = input("¿Borrar la tabla de drones? (s/n): ")
    if borrar_tabla_input == "s":
        drop_table()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    print("[WAITING] Esperando conexiones...")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()


print("[STARTING] Registry inicializándose...")
start()
