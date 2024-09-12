import sqlite3

class DatabaseManager:
    def __init__(self, connection):
        self.conn = connection

    def execute_query(self, query, params=None):
        try:
            cursor = self.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
        except sqlite3.Error as e:
            print(f"数据库查询错误: {e}")
            return []

    def bulk_insert(self, table_name, data):
        try:
            cursor = self.conn.cursor()
            placeholders = ', '.join(['?' for _ in data[0]])
            query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.executemany(query, data)
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"批量插入错误: {e}")
            self.conn.rollback()

    def close(self):
        if self.conn:
            self.conn.close()