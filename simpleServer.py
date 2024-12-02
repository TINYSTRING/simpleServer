import socket
import json
from typing import Dict, Any

from dbUtils import DBUtils

class SimpleServer:
    def __init__(self, host: str, port: int, db_name: str):
        """
        初始化服务器。
        :param host: 服务器地址
        :param port: 服务器端口
        :param db_name: 数据库文件名
        """
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.db_manager = DBUtils(db_name)

    def start(self):
        """启动服务器，监听客户端请求。"""
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"服务器已启动，监听 {self.host}:{self.port}...")
        self.db_manager.connect()  # 启动时连接数据库

        while True:
            client_socket, address = self.server_socket.accept()
            print(f"收到来自 {address} 的连接。")
            try:
                self.handle_client(client_socket)
            finally:
                client_socket.close()


    def handle_client(self, client_socket: socket.socket):
        """处理客户端请求。"""
        data = client_socket.recv(1024).decode('utf-8')
        print(f"收到数据: {data}")
        request = json.loads(data)
        response = self.process_request(request)
        client_socket.send(json.dumps(response).encode('utf-8'))

    def process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """处理客户端请求并返回响应。"""
        try:
            action = request.get("action")
            if action == "create_table":
                table_name = request["table_name"]
                schema = request["schema"]
                self.db_manager.create_table(table_name, schema)
                return {"status": "success", "message": f"Table {table_name} created."}

            elif action == "insert":
                table_name = request["table_name"]
                values = tuple(request["values"])
                self.db_manager.insert(table_name, values)
                return {"status": "success", "message": "Data inserted successfully."}

            elif action == "query":
                query = request["query"]
                results = self.db_manager.query(query)
                return {"status": "success", "data": results}

            else:
                return {"status": "error", "message": "Unsupported action."}

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def stop(self):
        """停止服务器并关闭数据库连接。"""
        self.db_manager.close()
        self.server_socket.close()
        print("服务器已关闭。")


if __name__ == "__main__":
    server = SimpleServer("127.0.0.1", 4120, 'data.db')
    server.start()
