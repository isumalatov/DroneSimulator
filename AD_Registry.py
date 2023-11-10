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
            id TEXT PRIMARY KEY,
            alias TEXT,
            token TEXT
        )
    """
    )
    return cursor


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

            tokens = msg.split(" ")
            action = tokens[0]
            dron_id = tokens[1]

            cursor = get_cursor()

            if action == "alta":
                alias = tokens[2]
                token = generate_token()
                cursor.execute(
                    "INSERT INTO drones (id, alias, token) VALUES (?, ?, ?)",
                    (dron_id, alias, token),
                )
                conn.send(token.encode(FORMAT))

            elif action == "baja":
                cursor.execute("DELETE FROM drones WHERE id = ?", (dron_id,))
                conn.send(f"¡Dron con ID {dron_id} dado de baja!".encode(FORMAT))

            elif action == "editar":
                new_alias = tokens[2]
                cursor.execute(
                    "UPDATE drones SET alias = ? WHERE id = ?", (new_alias, dron_id)
                )
                conn.send(
                    f"¡Alias del dron con ID {dron_id} actualizado a {new_alias}!".encode(
                        FORMAT
                    )
                )

            else:
                conn.send("Acción no válida".encode(FORMAT))

            get_connection().commit()  # Commit los cambios en la base de datos

    conn.close()


def start():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    print(f"[LISTENING] Registry a la escucha en {SERVER}:{PORT}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()


print("[STARTING] Registry inicializándose...")
start()
