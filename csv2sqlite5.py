import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
import sqlite3
import csv
import logging
import queue
import threading
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import numpy as np


class CSVToSQLiteApp:
    def __init__(self, master):
        self.master = master
        master.title("CSV to SQLite 导入工具 (单表版 - 优化)")
        master.geometry("600x600")

        # 设置默认文件夹和数据库文件
        self.default_directory = os.path.expanduser("~\CSVToSQLite")
        
        print(self.default_directory)
        self.default_db_file = os.path.join(self.default_directory, "avisql_single.db")
        
        self.directory = self.default_directory
        self.db_file = self.default_db_file

        logging.basicConfig(filename='csv_to_sqlite.log', level=logging.INFO,
                            format='%(asctime)s:%(levelname)s:%(message)s')

        self.select_dir_button = ttk.Button(master, text="选择CSV文件目录", command=self.select_directory)
        self.select_dir_button.pack(pady=20)
        
        self.current_dir_label = ttk.Label(master, text=f"当前目录: {self.directory}")
        self.current_dir_label.pack(pady=5)


        self.execute_button = ttk.Button(master, text="执行导入", command=self.execute_import)
        self.execute_button.pack(pady=20)

        self.plot_button = ttk.Button(master, text="生成直方图", command=self.plot_histograms)
        self.plot_button.pack(pady=20)

        self.distribution_button = ttk.Button(master, text="生成分布图", command=self.plot_distribution)
        self.distribution_button.pack(pady=20)

        self.model_name_label = ttk.Label(master, text="选择ModelName:")
        self.model_name_label.pack(pady=10)

        self.model_name_var = tk.StringVar()
        self.model_name_combobox = ttk.Combobox(master, textvariable=self.model_name_var)
        self.model_name_combobox.pack(pady=10)

        self.status_label = ttk.Label(master, text="")
        self.status_label.pack(pady=10)

        self.progress_bar = ttk.Progressbar(master, length=400, mode='determinate')
        self.progress_bar.pack(pady=10)

        self.log_text = tk.Text(master, height=10, width=70)
        self.log_text.pack(pady=10)

        self.stop_event = threading.Event()
        self.message_queue = queue.Queue()

        # 初始化时更新ModelName下拉菜单
        self.update_model_name_combobox()

    def select_directory(self):
        new_directory = filedialog.askdirectory(title="选择包含CSV文件的目录", initialdir=self.directory)
        if new_directory:
            self.directory = new_directory
            self.db_file = os.path.join(self.directory, "avisql_single.db")
            self.log_message(f"已选择目录: {self.directory}")
            self.log_message(f"数据库文件将保存为: {self.db_file}")
            print(self.directory)
            self.update_model_name_combobox()
            self.current_dir_label.config(text=f"当前目录: {self.directory}")

            

    def update_model_name_combobox(self):
        conn = self.create_connection(self.db_file)
        if conn is not None:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT ModelName FROM all_data")
                model_names = [row[0] for row in cursor.fetchall()]
                self.model_name_combobox['values'] = model_names
                if model_names:
                    self.model_name_combobox.set(model_names[0])
            except sqlite3.Error as e:
                self.log_message(f"获取ModelName时出错: {e}")
            finally:
                conn.close()

    def execute_import(self):
        self.execute_button.config(state=tk.DISABLED)
        self.stop_event.clear()
        threading.Thread(target=self.process_files_thread, daemon=True).start()
        self.master.after(100, self.check_message_queue)

    def process_files_thread(self):
        conn = self.create_connection(self.db_file)
        if conn is None:
            self.message_queue.put(("error", "无法建立数据库连接。"))
            return

        try:
            self.create_table(conn)
            csv_files = [f for f in self.get_all_csv_files(self.directory) if f.endswith('.csv')]
            total_files = len(csv_files)
            self.message_queue.put(("progress_max", total_files))

            for i, file_path in enumerate(csv_files):
                if self.stop_event.is_set():
                    break
                self.process_csv(file_path, conn)
                self.message_queue.put(("progress", i + 1))

            conn.close()
            self.message_queue.put(("info", "所有CSV文件已处理完毕，数据已存储到SQLite数据库中。"))
        except Exception as e:
            self.message_queue.put(("error", f"处理过程中出错: {str(e)}"))
        finally:
            self.message_queue.put(("finished", None))

    def check_message_queue(self):
        try:
            while True:
                message = self.message_queue.get_nowait()
                if message[0] == "log":
                    self.log_message(message[1])
                elif message[0] == "progress":
                    self.progress_bar['value'] = message[1]
                elif message[0] == "progress_max":
                    self.progress_bar['maximum'] = message[1]
                elif message[0] == "info":
                    messagebox.showinfo("信息", message[1])
                elif message[0] == "error":
                    messagebox.showerror("错误", message[1])
                elif message[0] == "finished":
                    self.execute_button.config(state=tk.NORMAL)
                    return
                self.master.update_idletasks()
        except queue.Empty:
            self.master.after(100, self.check_message_queue)

    def create_connection(self, db_file):
        conn = None
        try:
            conn = sqlite3.connect(db_file, check_same_thread=False)
            self.message_queue.put(("log", f"成功连接到数据库: {db_file}"))
        except sqlite3.Error as e:
            self.message_queue.put(("log", f"连接数据库时出错: {e}"))
        return conn

    def create_table(self, conn):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS all_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Time TEXT,
            BarCode TEXT,
            ModelName TEXT,
            Name_ TEXT,
            Status_V TEXT,
            V_Current REAL,
            V_Min REAL,
            V_Max REAL,
            Status_A TEXT,
            A_Current REAL,
            A_Min REAL,
            A_Max REAL,
            Status_O TEXT,
            Offset REAL,
            Offset_Min REAL,
            Offset_Max REAL,
            Status_VAO TEXT,
            RResult TEXT,
            Result TEXT,
            UNIQUE(ModelName, BarCode, V_Current, A_Current, Offset)
        )
        """
        try:
            conn.execute(create_table_sql)
            conn.commit()
            self.message_queue.put(("log", "成功创建数据表"))
        except sqlite3.Error as e:
            self.message_queue.put(("log", f"创建数据表时出错: {e}"))

    def get_all_csv_files(self, directory):
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith('.csv'):
                    yield os.path.join(root, file)

    def process_csv(self, file_path, conn):
        try:
            with open(file_path, 'r', newline='', encoding='utf-8-sig') as csvfile:
                csv_reader = csv.DictReader(csvfile)
                if not csv_reader.fieldnames:
                    self.message_queue.put(("log", f"警告: 跳过空文件 {file_path}"))
                    return

                cursor = conn.cursor()
                cursor.execute("BEGIN TRANSACTION")
                for row in csv_reader:
                    cursor.execute("""
                    INSERT OR IGNORE INTO all_data 
                    (Time, BarCode, ModelName, Name_, Status_V, V_Current, V_Min, V_Max, 
                    Status_A, A_Current, A_Min, A_Max, Status_O, Offset, Offset_Min, Offset_Max,
                    Status_VAO, RResult, Result)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row.get('Time', ''),
                        row.get('BarCode', ''),
                        row.get('ModelName', ''),
                        row.get('Name_', ''),
                        row.get('Status_V', ''),
                        float(row.get('V_Current', '0')),
                        float(row.get('V_Min', '0')),
                        float(row.get('V_Max', '0')),
                        row.get('Status_A', ''),
                        float(row.get('A_Current', '0')),
                        float(row.get('A_Min', '0')),
                        float(row.get('A_Max', '0')),
                        row.get('Status_O', ''),
                        float(row.get('Offset', '0')),
                        float(row.get('Offset_Min', '0')),
                        float(row.get('Offset_Max', '0')),
                        row.get('Status_VAO', ''),
                        row.get('RResult', ''),
                        row.get('Result', '')
                    ))
                conn.commit()
            self.message_queue.put(("log", f"成功处理文件: {file_path}"))
        except Exception as e:
            self.message_queue.put(("log", f"处理文件时出错 {file_path}: {e}"))

    def log_message(self, message):
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        logging.info(message)

    def plot_histograms(self):
        model_name = self.model_name_var.get()
        if not model_name:
            messagebox.showerror("错误", "请选择ModelName")
            returnn

        conn = self.create_connection(self.db_file)
        if conn is None:
            messagebox.showerror("错误", "无法连接到数据库")
            return

        try:
            query = """
            SELECT V_Current, A_Current, Offset FROM all_data WHERE ModelName = ?
            """
            cursor = conn.cursor()
            cursor.execute(query, (model_name,))
            rows = cursor.fetchall()
            if not rows:
                messagebox.showinfo("信息", f"没有找到ModelName为 {model_name} 的数据")
                return

            v_current_data = [row[0] for row in rows]
            a_current_data = [row[1] for row in rows]
            offset_data = [row[2] for row in rows]

            fig, axs = plt.subplots(3, 1, figsize=(8, 12))
            axs[0].hist(v_current_data, bins=50, color='b', alpha=0.7)
            axs[0].set_title('V_Current Histogram')
            axs[0].set_xlabel('V_Current')
            axs[0].set_ylabel('Frequency')

            axs[1].hist(a_current_data, bins=50, color='r', alpha=0.7)
            axs[1].set_title('A_Current Histogram')
            axs[1].set_xlabel('A_Current')
            axs[1].set_ylabel('Frequency')

            axs[2].hist(offset_data, bins=50, color='g', alpha=0.7)
            axs[2].set_title('Offset Histogram')
            axs[2].set_xlabel('Offset')
            axs[2].set_ylabel('Frequency')

            plt.tight_layout()
            plt.show()
        except sqlite3.Error as e:
            messagebox.showerror("错误", f"查询数据库时出错: {e}")
        finally:
            conn.close()

    def plot_distribution(self):
        model_name = self.model_name_var.get()
        if not model_name:
            messagebox.showerror("错误", "请选择ModelName")
            return

        conn = self.create_connection(self.db_file)
        if conn is None:
            messagebox.showerror("错误", "无法连接到数据库")
            return

        try:
            query = """
            SELECT V_Current, V_Min, V_Max, A_Current, Offset FROM all_data WHERE ModelName = ?
            """
            cursor = conn.cursor()
            cursor.execute(query, (model_name,))
            rows = cursor.fetchall()
            if not rows:
                messagebox.showinfo("信息", f"没有找到ModelName为 {model_name} 的数据")
                return

            v_current_data = [row[0] for row in rows]
            v_min_data = [row[1] for row in rows]
            v_max_data = [row[2] for row in rows]
            a_current_data = [row[3] for row in rows]
            offset_data = [row[4] for row in rows]

            # 计算V_current的Sigma值
            v_nominal = [(v_min + v_max) / 2 for v_min, v_max in zip(v_min_data, v_max_data)]
            v_sigma = [abs(v_current - v_nom) / ((v_max - v_min) / 6) for v_current, v_nom, v_min, v_max in zip(v_current_data, v_nominal, v_min_data, v_max_data)]

            fig, axs = plt.subplots(4, 1, figsize=(10, 20))
            fig.suptitle(f'数据分布图 - {model_name}', fontsize=16)

            def plot_distribution_histogram(ax, data, title, xlabel):
                min_val, max_val = min(data), max(data)
                range_val = max_val - min_val
                extended_min = min_val - 0.3 * range_val
                extended_max = max_val + 0.3 * range_val
                
                bins = np.linspace(extended_min, extended_max, 101)
                ax.hist(data, bins=bins, edgecolor='black')
                ax.set_title(title)
                ax.set_xlabel(xlabel)
                ax.set_ylabel('频率')

            plot_distribution_histogram(axs[0], v_current_data, 'V_Current 分布', 'V_Current')
            plot_distribution_histogram(axs[1], a_current_data, 'A_Current 分布', 'A_Current')
            plot_distribution_histogram(axs[2], offset_data, 'Offset 分布', 'Offset')
            plot_distribution_histogram(axs[3], v_sigma, 'V_Current Sigma 分布', 'Sigma')

            plt.tight_layout()
            plt.show()

            # 显示V_current的平均Sigma值
            avg_sigma = sum(v_sigma) / len(v_sigma)
            messagebox.showinfo("V_Current Sigma", f"V_Current的平均Sigma值: {avg_sigma:.4f}")

        except sqlite3.Error as e:
            messagebox.showerror("错误", f"查询数据库时出错: {e}")
        finally:
            conn.close()


if __name__ == "__main__":
    root = tk.Tk()
    app = CSVToSQLiteApp(root)
    root.mainloop()