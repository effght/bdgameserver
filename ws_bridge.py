import argparse
import base64
import hashlib
import json
import os
import socket
import struct
import threading
from typing import Optional


GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class WebSocketClient:
    def __init__(self, conn: socket.socket, addr):
        self.conn = conn
        self.addr = addr
        self.lock = threading.Lock()
        self.role = "unknown"
        self.room_id = "default"
        self.client_id = ""

    def send_text(self, text: str):
        payload = text.encode("utf-8")
        header = bytearray([0x81])
        length = len(payload)
        if length < 126:
          header.append(length)
        elif length < 65536:
          header.append(126)
          header.extend(struct.pack("!H", length))
        else:
          header.append(127)
          header.extend(struct.pack("!Q", length))
        with self.lock:
            self.conn.sendall(header + payload)

    def close(self):
        try:
            self.conn.close()
        except OSError:
            pass


class WebSocketBridge:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.clients = []
        self.clients_lock = threading.Lock()

    def recv_exact(self, conn: socket.socket, size: int) -> Optional[bytes]:
        data = bytearray()
        try:
            while len(data) < size:
                chunk = conn.recv(size - len(data))
                if not chunk:
                    return None
                data.extend(chunk)
        except (ConnectionResetError, ConnectionAbortedError, OSError):
            return None
        return bytes(data)

    def add_client(self, client: WebSocketClient):
        with self.clients_lock:
            self.clients.append(client)

    def remove_client(self, client: WebSocketClient):
        with self.clients_lock:
            self.clients = [c for c in self.clients if c is not client]
        client.close()

    def route_message(self, message: str, packet: dict, sender: Optional[WebSocketClient] = None):
        sender_room_id = sender.room_id if sender else "default"
        room_id = str(packet.get("roomId") or packet.get("channelId") or sender_room_id)
        target_client_id = str(packet.get("targetClientId") or "")
        target_role = str(packet.get("targetRole") or "")
        with self.clients_lock:
            clients = list(self.clients)
        dead = []
        for client in clients:
            if client is sender:
                continue
            if client.room_id != room_id:
                continue
            if target_client_id and client.client_id != target_client_id:
                continue
            if target_role and client.role != target_role:
                continue
            try:
                client.send_text(message)
            except OSError:
                dead.append(client)
        for client in dead:
            self.remove_client(client)

    def perform_handshake(self, conn: socket.socket) -> bool:
        request = b""
        try:
            while b"\r\n\r\n" not in request:
                chunk = conn.recv(4096)
                if not chunk:
                    return False
                request += chunk
        except (ConnectionResetError, ConnectionAbortedError, OSError):
            return False

        headers = request.decode("utf-8", errors="ignore").split("\r\n")
        key = None
        for line in headers:
            if line.lower().startswith("sec-websocket-key:"):
                key = line.split(":", 1)[1].strip()
                break
        if not key:
            return False

        accept = base64.b64encode(hashlib.sha1((key + GUID).encode("utf-8")).digest()).decode("ascii")
        response = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
        )
        conn.sendall(response.encode("utf-8"))
        return True

    def recv_frame(self, conn: socket.socket) -> Optional[str]:
        header = self.recv_exact(conn, 2)
        if not header or len(header) < 2:
            return None

        b1, b2 = header
        opcode = b1 & 0x0F
        masked = (b2 >> 7) & 1
        length = b2 & 0x7F

        if opcode == 0x8:
            return None
        if opcode == 0x9:
            return ""
        if opcode == 0xA:
            return ""

        if length == 126:
            ext = self.recv_exact(conn, 2)
            if not ext or len(ext) < 2:
                return None
            length = struct.unpack("!H", ext)[0]
        elif length == 127:
            ext = self.recv_exact(conn, 8)
            if not ext or len(ext) < 8:
                return None
            length = struct.unpack("!Q", ext)[0]

        mask = self.recv_exact(conn, 4) if masked else b""
        if masked and (not mask or len(mask) < 4):
            return None
        payload = self.recv_exact(conn, length)
        if payload is None:
            return None
        payload = bytearray(payload)

        if masked:
            for i in range(length):
                payload[i] ^= mask[i % 4]

        return payload.decode("utf-8", errors="ignore")

    def handle_client(self, conn: socket.socket, addr):
        if not self.perform_handshake(conn):
            conn.close()
            return

        client = WebSocketClient(conn, addr)
        self.add_client(client)
        print(f"[connect] {addr}")

        try:
            while True:
                message = self.recv_frame(conn)
                if message is None:
                    break
                if message == "":
                    continue

                try:
                    packet = json.loads(message)
                except json.JSONDecodeError:
                    continue

                if packet.get("type") == "hello":
                    client.role = str(packet.get("role") or "unknown")
                    client.room_id = str(packet.get("roomId") or packet.get("channelId") or "default")
                    client.client_id = str(packet.get("clientId") or f"{client.role}:{addr[0]}:{addr[1]}")
                    ack = json.dumps({
                        "type": "hello_ack",
                        "role": "bridge",
                        "roomId": client.room_id,
                        "clientId": client.client_id,
                        "connected_clients": len(self.clients)
                    })
                    client.send_text(ack)
                    continue

                packet.setdefault("roomId", client.room_id)
                packet.setdefault("sourceClientId", client.client_id)
                packet.setdefault("sourceRole", client.role)
                self.route_message(json.dumps(packet), packet, sender=client)
        finally:
            print(f"[disconnect] {addr} role={client.role} room={client.room_id} client={client.client_id}")
            self.remove_client(client)

    def serve_forever(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen()
        print(f"WebSocket bridge listening on ws://{self.host}:{self.port}")
        try:
            while True:
                conn, addr = server.accept()
                thread = threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True)
                thread.start()
        finally:
            server.close()


def main():
    parser = argparse.ArgumentParser(description="Simple WebSocket bridge for badminton simulator")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8765")))
    args = parser.parse_args()
    WebSocketBridge(args.host, args.port).serve_forever()


if __name__ == "__main__":
    main()
