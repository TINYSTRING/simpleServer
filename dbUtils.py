import sqlite3
from typing import List, Tuple, Dict, Any

class DBUtils:
    def __init__(self, db_name: str):
        """
        初始化数据库管理类，设置数据库文件名。
        :param db_name: 数据库文件名
        """
        self.db_name = db_name
        self.connection = None
        self.cursor = None


    def connect(self):
        """连接到 SQLite 数据库。"""
        try:
            self.connection = sqlite3.connect(self.db_name)
            self.cursor = self.connection.cursor()
            print(f"已连接到数据库: {self.db_name}")
        except sqlite3.Error as e:
            print(f"连接数据库失败: {e}")


    def create_table(self, table_name: str, schema: str):
        """
        创建表。
        :param table_name: 表名
        :param schema: 表结构，如 "id INTEGER PRIMARY KEY, name TEXT"
        """
        try:
            self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})")
            self.connection.commit()
            print(f"表 {table_name} 创建成功或已存在。")
        except sqlite3.Error as e:
            print(f"创建表失败: {e}")


    def insert(self, table_name: str, values: Tuple):
        """
        插入数据。
        :param table_name: 表名
        :param values: 插入的值，如 (1, "Alice")
        """
        try:
            placeholders = ", ".join(["?"] * len(values))
            self.cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", values)
            self.connection.commit()
            print("数据插入成功。")
        except sqlite3.Error as e:
            print(f"插入数据失败: {e}")


    def update(self, table_name: str, updates: Dict[str, Any], condition: str, condition_params: Tuple):
        """
        更新数据。
        :param table_name: 表名
        :param updates: 要更新的数据
        """
        try:
            set_clause = ", ".join([f"{col} = ?" for col in updates])
            query = f"UPDATE {table_name} SET {set_clause} WHERE {condition};"
            self.cursor.execute(query, tuple(updates.values()) + condition_params)
            self.connection.commit()
            print(f"更新数据成功。")
        except sqlite3.Error as e:
            print(f"更新失败: {e}")
            return []


    def query(self, query: str) -> List[Tuple]:
        """
        执行查询。
        :param query: 查询语句
        :return: 查询结果
        """
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            print("查询成功。")
            return results
        except sqlite3.Error as e:
            print(f"查询失败: {e}")
            return []


    def close(self):
        """关闭数据库连接。"""
        if self.connection:
            self.connection.close()
            print("数据库连接已关闭。")
