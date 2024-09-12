import csv
from typing import List, Dict, Any
import time

class CSVReader:
    def __init__(self, chunk_size: int = 1000):
        self.chunk_size = chunk_size

    def read_in_chunks(self, file_path: str) -> List[Dict[str, Any]]:
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            chunk = []
            for row in reader:
                chunk.append(row)
                if len(chunk) >= self.chunk_size:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk

class PerformanceMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.file_count = 0
        self.row_count = 0

    def update(self, file_count: int, row_count: int):
        self.file_count += file_count
        self.row_count += row_count

    def get_stats(self) -> Dict[str, Any]:
        elapsed_time = time.time() - self.start_time
        return {
            "elapsed_time": elapsed_time,
            "files_processed": self.file_count,
            "rows_processed": self.row_count,
            "avg_speed": self.row_count / elapsed_time if elapsed_time > 0 else 0
        }

def validate_csv_row(row: Dict[str, str]) -> Dict[str, Any]:
    """验证并转换CSV行数据"""
    try:
        return {
            'Time': row.get('Time', ''),
            'BarCode': row.get('BarCode', ''),
            'ModelName': row.get('ModelName', ''),
            'Name_': row.get('Name_', ''),
            'Status_V': row.get('Status_V', ''),
            'V_Current': float(row.get('V_Current', '0')),
            'V_Min': float(row.get('V_Min', '0')),
            'V_Max': float(row.get('V_Max', '0')),
            'Status_A': row.get('Status_A', ''),
            'A_Current': float(row.get('A_Current', '0')),
            'A_Min': float(row.get('A_Min', '0')),
            'A_Max': float(row.get('A_Max', '0')),
            'Status_O': row.get('Status_O', ''),
            'Offset': float(row.get('Offset', '0')),
            'Offset_Min': float(row.get('Offset_Min', '0')),
            'Offset_Max': float(row.get('Offset_Max', '0')),
            'Status_VAO': row.get('Status_VAO', ''),
            'RResult': row.get('RResult', ''),
            'Result': row.get('Result', '')
        }
    except ValueError as e:
        raise ValueError(f"数据转换错误: {e}")

# class DatabaseManager:
#     def __init__(self, db_connection):
#         self.conn = db_connection

#     def bulk_insert(self, table_name: str, data: List[Dict[str, Any]]):
#         if not data:
#             return

#         columns = ', '.join(data[0].keys())
#         placeholders = ', '.join(['?' for _ in data[0]])
#         query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

#         cursor = self.conn.cursor()
#         cursor.executemany(query, [tuple(row.values()) for row in data])
#         self.conn.commit()

#     def execute_query(self, query: str, params: tuple = None):
#         cursor = self.conn.cursor()
#         if params:
#             cursor.execute(query, params)
#         else:
#             cursor.execute(query)
#         return cursor.fetchall()

class DataAnalyzer:
    @staticmethod
    def calculate_statistics(data: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not data:
            return {}

        v_current = [row['V_Current'] for row in data]
        a_current = [row['A_Current'] for row in data]
        offset = [row['Offset'] for row in data]

        return {
            'V_Current': {
                'min': min(v_current),
                'max': max(v_current),
                'avg': sum(v_current) / len(v_current)
            },
            'A_Current': {
                'min': min(a_current),
                'max': max(a_current),
                'avg': sum(a_current) / len(a_current)
            },
            'Offset': {
                'min': min(offset),
                'max': max(offset),
                'avg': sum(offset) / len(offset)
            }
        }