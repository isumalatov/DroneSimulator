import sys
import socket
import threading

PORT = int(sys.argv[1])
HEADER = 64
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = "utf-8"
FIN = "FIN"

# Crear una conexión y un cursor específicos para cada hilo
local = threading.local()


def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")

    while True:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)

            with open("clima.txt", "r") as f:
                temperature = float(f.readline().strip())

            print(temperature)
            conn.send(str(temperature).encode(FORMAT))


def start():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    print(f"[LISTENING] Weather a la escucha en {SERVER}:{PORT}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()


print("[STARTING] Weather inicializándose...")
start()
