import socket
import json

def send_request(host, port, action, data):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host, port))
    request = {"action": action, **data}
    client_socket.send(json.dumps(request).encode('utf-8'))
    response = client_socket.recv(1024).decode('utf-8')
    client_socket.close()
    return json.loads(response)

# 示例请求
if __name__ == "__main__":
    host = "127.0.0.1"
    port = 4120
    
    # 创建表
    print(send_request(host, port, "create_table", {"table_name": "users", "schema": "id INTEGER PRIMARY KEY, name TEXT"}))

    # 插入数据
    print(send_request(host, port, "insert", {"table_name": "users", "values": [1, "Alice"]}))
    
    # 查询数据
    print(send_request(host, port, "query", {"query": "SELECT * FROM users"}))
