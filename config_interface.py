import tkinter as tk
from tkinter import ttk

class ConfigInterface(tk.Toplevel):
    def __init__(self, master, current_config):
        super().__init__(master)
        self.title("配置设置")
        self.geometry("400x300")
        self.current_config = current_config
        self.result = None

        self.create_widgets()

    def create_widgets(self):
        ttk.Label(self, text="CSV 读取块大小:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.chunk_size = ttk.Entry(self)
        self.chunk_size.insert(0, str(self.current_config.get('chunk_size', 1000)))
        self.chunk_size.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(self, text="数据库文件路径:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        self.db_path = ttk.Entry(self)
        self.db_path.insert(0, self.current_config.get('db_path', 'optimized_data.db'))
        self.db_path.grid(row=1, column=1, padx=5, pady=5)

        ttk.Label(self, text="最大线程数:").grid(row=2, column=0, padx=5, pady=5, sticky="w")
        self.max_threads = ttk.Entry(self)
        self.max_threads.insert(0, str(self.current_config.get('max_threads', 4)))
        self.max_threads.grid(row=2, column=1, padx=5, pady=5)

        ttk.Label(self, text="日志级别:").grid(row=3, column=0, padx=5, pady=5, sticky="w")
        self.log_level = ttk.Combobox(self, values=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
        self.log_level.set(self.current_config.get('log_level', 'INFO'))
        self.log_level.grid(row=3, column=1, padx=5, pady=5)

        save_button = ttk.Button(self, text="保存", command=self.save_config)
        save_button.grid(row=4, column=0, columnspan=2, pady=20)

    def save_config(self):
        self.result = {
            'chunk_size': int(self.chunk_size.get()),
            'db_path': self.db_path.get(),
            'max_threads': int(self.max_threads.get()),
            'log_level': self.log_level.get()
        }
        self.destroy()

def show_config_dialog(master, current_config):
    dialog = ConfigInterface(master, current_config)
    dialog.wait_window()
    return dialog.result