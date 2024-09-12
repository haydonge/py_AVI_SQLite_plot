import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
import math
import sqlite3
import logging
import queue
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 导入新的辅助类和函数
from csv_processor_helpers import CSVReader, PerformanceMonitor, validate_csv_row, DataAnalyzer
from config_interface import show_config_dialog
from database_manager import DatabaseManager  # 新增这行


class OptimizedCSVToSQLiteApp:
    def __init__(self, master):
        self.master = master
        master.title("优化版 CSV to SQLite 导入工具")
        master.geometry("800x600")
        
        self.config = {
            'chunk_size': 1000,
            'db_path': 'avisql_single.db',
            'max_threads': 4,
            'log_level': 'INFO'
        }

        self.setup_async_processor()
        self.setup_logging()
        self.setup_database()  # 移到这里
        self.setup_ui()  # 移到数据库设置之后
        
        self.csv_reader = CSVReader(chunk_size=self.config['chunk_size'])
        self.performance_monitor = PerformanceMonitor()

    def setup_ui(self):
        self.create_menu()
        self.create_file_list()
        self.create_progress_bars()
        self.create_log_area()
        self.create_model_selector()
        self.update_model_list()  # 确保在这里调用

    def setup_database(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(current_dir, "avisql_single.db")
        
        if not os.path.exists(db_path):
            messagebox.showerror("错误", f"数据库文件 {db_path} 不存在")
            return

        try:
            connection = sqlite3.connect(db_path, check_same_thread=False)
            self.db_manager = DatabaseManager(connection)
            self.db_manager.conn.execute("PRAGMA journal_mode=WAL")
            self.log_message(f"成功连接到数据库: {db_path}")
        except sqlite3.Error as e:
            self.log_message(f"连接数据库时出错: {e}")
            messagebox.showerror("数据库错误", f"无法连接到数据库: {e}")
            return

        self.create_table()

    def create_model_selector(self):
        self.model_var = tk.StringVar()
        self.model_selector = ttk.Combobox(self.master, textvariable=self.model_var)
        self.model_selector.pack(pady=10)
        self.model_selector.bind("<<ComboboxSelected>>", self.on_model_selected)
        
        if hasattr(self, 'db_manager'):
            self.update_model_list()
        else:
            self.log_message("警告：数据库管理器尚未初始化，无法更新模型列表。")

    def update_model_list(self):
        if hasattr(self, 'db_manager'):
            try:
                query = "SELECT DISTINCT ModelName FROM optimized_data"
                models = self.db_manager.execute_query(query)
                if models:
                    self.model_selector['values'] = [model[0] for model in models]
                    self.log_message(f"成功更新模型列表，找到 {len(models)} 个模型")
                else:
                    self.log_message("警告：没有找到任何模型")
                    self.model_selector['values'] = ["没有可用模型"]
            except Exception as e:
                self.log_message(f"更新模型列表时出错: {str(e)}")
                self.model_selector['values'] = ["更新失败"]
        else:
            self.log_message("警告：数据库管理器尚未初始化，无法更新模型列表")
            self.model_selector['values'] = ["数据库未连接"]

    def on_model_selected(self, event):
        selected_model = self.model_var.get()
        if selected_model:
            self.plot_distribution(selected_model)
        else:
            messagebox.showinfo("提示", "请选择一个模型")



    def create_menu(self):
        menubar = tk.Menu(self.master)
        self.master.config(menu=menubar)

        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="文件", menu=file_menu)
        file_menu.add_command(label="选择CSV文件夹", command=self.select_directory)
        file_menu.add_command(label="执行导入", command=self.execute_import)
        file_menu.add_separator()
        file_menu.add_command(label="退出", command=self.master.quit)

        view_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="查看", menu=view_menu)
        view_menu.add_command(label="生成数据分布图", command=self.plot_distribution)

        config_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="配置", menu=config_menu)
        config_menu.add_command(label="设置", command=self.show_config)

    def create_file_list(self):
        self.file_list = ttk.Treeview(self.master, columns=("Status",), show="headings")
        self.file_list.heading("Status", text="状态")
        self.file_list.column("Status", width=100)
        self.file_list.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)

    def create_progress_bars(self):
        self.overall_progress = ttk.Progressbar(self.master, length=400, mode='determinate')
        self.overall_progress.pack(pady=5)
        self.file_progress = ttk.Progressbar(self.master, length=400, mode='determinate')
        self.file_progress.pack(pady=5)

    def create_log_area(self):
        self.log_text = tk.Text(self.master, height=10, width=70)
        self.log_text.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)

    def setup_async_processor(self):
        self.executor = ThreadPoolExecutor(max_workers=self.config['max_threads'])
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        threading.Thread(target=self.run_async_loop, daemon=True).start()

    def run_async_loop(self):
       self.loop.run_forever()



    def setup_logging(self):
            logging.basicConfig(filename='output/csv_to_sqlite_optimized.log', level=getattr(logging, self.config['log_level']),
                                format='%(asctime)s:%(levelname)s:%(message)s')

    def create_table(self):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS optimized_data (
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
            Result TEXT
        )
        """
        try:
            self.db_manager.conn.execute(create_table_sql)
            self.db_manager.conn.commit()
            self.log_message("成功创建或验证数据表存在")
            self.create_indexes()
        except sqlite3.Error as e:
            self.log_message(f"创建或验证数据表时出错: {e}")
            messagebox.showerror("数据库错误", f"创建或验证数据表时出错: {e}")
       # 创建 all_data 视图
        create_view_sql = """
        CREATE VIEW IF NOT EXISTS optimized_data AS
        SELECT * FROM optimized_data
        """
        try:
            self.db_manager.conn.execute(create_view_sql)
            self.db_manager.conn.commit()
            self.log_message("成功创建或验证 optimized_data 视图存在")
        except sqlite3.Error as e:
            self.log_message(f"创建 optimized_data 视图时出错: {e}")
            messagebox.showerror("数据库错误", f"创建 optimized_data 视图时出错: {e}")

    def create_indexes(self):
        try:
            self.db_manager.conn.execute("CREATE INDEX IF NOT EXISTS idx_model_name ON optimized_data (ModelName)")
            self.db_manager.conn.execute("CREATE INDEX IF NOT EXISTS idx_bar_code ON optimized_data (BarCode)")
            self.db_manager.conn.commit()
            self.log_message("成功创建索引")
        except sqlite3.Error as e:
            self.log_message(f"创建索引时出错: {e}")

    def select_directory(self):
        self.directory = filedialog.askdirectory(title="选择包含CSV文件的目录")
        if self.directory:
            self.log_message(f"已选择目录: {self.directory}")
            self.update_file_list()

    def update_file_list(self):
        self.file_list.delete(*self.file_list.get_children())
        for file in os.listdir(self.directory):
            if file.endswith('.csv'):
                self.file_list.insert('', 'end', text=file, values=("待处理",))

    async def process_csv_files(self):
        csv_files = [os.path.join(self.directory, f) for f in os.listdir(self.directory) if f.endswith('.csv')]
        total_files = len(csv_files)
        self.overall_progress['maximum'] = total_files

        for i, file_path in enumerate(csv_files):
            try:
                await self.loop.run_in_executor(self.executor, self.process_csv, file_path)
                self.overall_progress['value'] = i + 1
                self.master.update_idletasks()
            except Exception as e:
                self.log_message(f"处理文件 {file_path} 时出错: {e}")

        self.log_message(f"处理完成。性能统计：{self.performance_monitor.get_stats()}")

    def process_csv(self, file_path):
        try:
            file_name = os.path.basename(file_path)
            self.update_file_status(file_name, "处理中")
            
            rows_processed = 0
            for chunk in self.csv_reader.read_in_chunks(file_path):
                validated_chunk = [validate_csv_row(row) for row in chunk]
                self.db_manager.bulk_insert('optimized_data', validated_chunk)
                rows_processed += len(chunk)
                self.performance_monitor.update(0, len(chunk))

            self.performance_monitor.update(1, 0)  # 更新处理的文件数
            self.update_file_status(file_name, "已完成")
            self.log_message(f"成功处理文件: {file_path}，共处理 {rows_processed} 行")
        except Exception as e:
            self.log_message(f"处理文件时出错 {file_path}: {e}")
            self.update_file_status(file_name, "处理失败")

    def update_file_status(self, file_name, status):
        for item in self.file_list.get_children():
            if self.file_list.item(item)["text"] == file_name:
                self.file_list.set(item, "Status", status)
                break

    def execute_import(self):
        if not hasattr(self, 'directory'):
            messagebox.showerror("错误", "请先选择CSV文件目录")
            return
        
        self.performance_monitor = PerformanceMonitor()  # 重置性能监控器
        self.log_message("开始导入过程...")
        self.master.after(0, self.start_import_process)
        self.master.after(1000, self.update_model_list)  # 添加这行，1秒后更新列表

    def start_import_process(self):
        try:
            asyncio.run_coroutine_threadsafe(self.process_csv_files(), self.loop)
            self.log_message("导入进程已启动")
        except Exception as e:
            self.log_message(f"启动导入进程时出错: {e}")
            messagebox.showerror("错误", f"启动导入进程时出错: {e}")

    def log_message(self, message):
        print(message)  # 总是打印到控制台
        if hasattr(self, 'log_text'):
            self.log_text.insert(tk.END, message + "\n")
            self.log_text.see(tk.END)
        logging.info(message)


    # def plot_distribution(self, model_name, page=1, locations_per_page=5):
    #  try:
    #     query = """
    #     SELECT Name_, V_Current, V_Min, V_Max, A_Current, A_Min, A_Max, Offset, Offset_Min, Offset_Max
    #     FROM all_data
    #     WHERE ModelName = ?
    #     """
    #     rows = self.db_manager.execute_query(query, (model_name,))

    #     if not rows:
    #         messagebox.showinfo("信息", f"没有找到{model_name}的数据")
    #         return

    #     measurement_locations = sorted(set(row[0] for row in rows))
    #     total_locations = len(measurement_locations)
    #     total_pages = math.ceil(total_locations / locations_per_page)

    #     start_index = (page - 1) * locations_per_page
    #     end_index = min(start_index + locations_per_page, total_locations)
    #     current_locations = measurement_locations[start_index:end_index]

    #     fig = make_subplots(rows=len(current_locations), cols=3,
    #                         subplot_titles=[f"{loc} - V_Current | A_Current | Offset" for loc in current_locations],
    #                         vertical_spacing=0.1,
    #                         horizontal_spacing=0.05)

    #     colors = {'V': 'blue', 'A': 'red', 'O': 'green'}

    #     for i, location in enumerate(current_locations):
    #         location_data = [row for row in rows if row[0] == location]
            
    #         v_data = [row[1] for row in location_data]
    #         a_data = [row[4] for row in location_data]
    #         o_data = [row[7] for row in location_data]

    #         row = i + 1

    #         # V_Current
    #         fig.add_trace(go.Histogram(x=v_data, name="V_Current", marker_color=colors['V'], opacity=0.7), row=row, col=1)
    #         fig.add_vline(x=location_data[0][2], line_dash="dash", line_color=colors['V'], row=row, col=1)  # V_Min
    #         fig.add_vline(x=location_data[0][3], line_dash="dash", line_color=colors['V'], row=row, col=1)  # V_Max

    #         # A_Current
    #         fig.add_trace(go.Histogram(x=a_data, name="A_Current", marker_color=colors['A'], opacity=0.7), row=row, col=2)
    #         fig.add_vline(x=location_data[0][5], line_dash="dash", line_color=colors['A'], row=row, col=2)  # A_Min
    #         fig.add_vline(x=location_data[0][6], line_dash="dash", line_color=colors['A'], row=row, col=2)  # A_Max

    #         # Offset
    #         fig.add_trace(go.Histogram(x=o_data, name="Offset", marker_color=colors['O'], opacity=0.7), row=row, col=3)
    #         fig.add_vline(x=location_data[0][8], line_dash="dash", line_color=colors['O'], row=row, col=3)  # Offset_Min
    #         fig.add_vline(x=location_data[0][9], line_dash="dash", line_color=colors['O'], row=row, col=3)  # Offset_Max

    #     fig.update_layout(
    #         height=300 * len(current_locations),
    #         width=1200,
    #         title_text=f"Distribution for {model_name} (Page {page}/{total_pages})",
    #         showlegend=False,
    #         font=dict(size=10)
    #     )

    #     for i in range(1, len(current_locations) + 1):
    #         for j in range(1, 4):
    #             fig.update_xaxes(title_text="Value", title_font=dict(size=8), tickfont=dict(size=8), row=i, col=j)
    #             fig.update_yaxes(title_text="Frequency", title_font=dict(size=8), tickfont=dict(size=8), row=i, col=j)

    #     fig.show()

    #     # 添加分页控制
    #     if total_pages > 1:
    #         if page < total_pages:
    #             messagebox.showinfo("分页", f"当前页面 {page}/{total_pages}。点击确定查看下一页。")
    #             self.plot_distribution(model_name, page + 1, locations_per_page)
    #         else:
    #             messagebox.showinfo("分页", "已经是最后一页。")

    #  except Exception as e:
    #     messagebox.showerror("错误", f"生成图表时出错: {str(e)}")


    def plot_distribution(self, model_name, page=1, rows_per_page=10):
     try:
        # 获取所有唯一的测量位置
        location_query = """
        SELECT DISTINCT Name_
        FROM optimized_data
        WHERE ModelName = ? AND Result = 'OK'
        ORDER BY Name_
        """
        locations = self.db_manager.execute_query(location_query, (model_name,))

        if not locations:
            messagebox.showinfo("信息", f"没有找到{model_name}的数据")
            return

        total_locations = len(locations)
        total_pages = math.ceil(total_locations / rows_per_page)

        start_index = (page - 1) * rows_per_page
        end_index = min(start_index + rows_per_page, total_locations)
        current_locations = locations[start_index:end_index]

        fig = make_subplots(rows=len(current_locations), cols=3,
                            subplot_titles=[f"{loc[0]} - V_Current | A_Current | Offset" for loc in current_locations for _ in range(3)],
                            vertical_spacing=0.05,
                            horizontal_spacing=0.02)

        for i, (location,) in enumerate(current_locations):
            row = i + 1

            # 获取该位置的所有数据
            data_query = """
            SELECT V_Current, A_Current, Offset, V_Min, V_Max, A_Min, A_Max, Offset_Min, Offset_Max
            FROM optimized_data
            WHERE ModelName = ? AND Name_ = ?
            """
            data = self.db_manager.execute_query(data_query, (model_name, location))

            if not data:
                continue

            v_data = [row[0] for row in data]
            a_data = [row[1] for row in data]
            o_data = [row[2] for row in data]
            v_min, v_max, a_min, a_max, o_min, o_max = data[0][3:]

            # V_Current
            fig.add_trace(go.Histogram(x=v_data, name="V_Current", marker_color='blue', opacity=0.7), row=row, col=1)
            fig.add_vline(x=v_min, line_dash="dash", line_color="red", row=row, col=1)
            fig.add_vline(x=v_max, line_dash="dash", line_color="red", row=row, col=1)

            # A_Current
            fig.add_trace(go.Histogram(x=a_data, name="A_Current", marker_color='green', opacity=0.7), row=row, col=2)
            fig.add_vline(x=a_min, line_dash="dash", line_color="red", row=row, col=2)
            fig.add_vline(x=a_max, line_dash="dash", line_color="red", row=row, col=2)

            # Offset
            fig.add_trace(go.Histogram(x=o_data, name="Offset", marker_color='orange', opacity=0.7), row=row, col=3)
            fig.add_vline(x=o_min, line_dash="dash", line_color="red", row=row, col=3)
            fig.add_vline(x=o_max, line_dash="dash", line_color="red", row=row, col=3)

        fig.update_layout(
            height=300 * len(current_locations),
            width=1500,
            title_text=f"Distribution for {model_name} (Page {page}/{total_pages})",
            showlegend=False,
        )

        for i in range(1, len(current_locations) + 1):
            for j in range(1, 4):
                fig.update_xaxes(title_text="Value", row=i, col=j)
                fig.update_yaxes(title_text="Frequency", row=i, col=j)

        fig.show()

        # 添加分页控制
        if total_pages > 1:
            if page < total_pages:
                next_page = messagebox.askyesno("分页", f"当前页面 {page}/{total_pages}。是否查看下一页？")
                if next_page:
                    self.plot_distribution(model_name, page + 1, rows_per_page)
            else:
                messagebox.showinfo("分页", "已经是最后一页。")

     except Exception as e:
        messagebox.showerror("错误", f"生成图表时出错: {str(e)}")


    def show_config(self):
        new_config = show_config_dialog(self.master, self.config)
        if new_config:
            self.config.update(new_config)
            self.csv_reader.chunk_size = self.config['chunk_size']
            self.executor = ThreadPoolExecutor(max_workers=self.config['max_threads'])
            self.setup_logging()
            messagebox.showinfo("配置", "配置已更新")

    def calculate_statistics(self):
        try:
            query = """
            SELECT ModelName, V_Current, A_Current, Offset FROM optimized_data
            """
            rows = self.db_manager.execute_query(query)

            if not rows:
                messagebox.showinfo("信息", "没有找到数据")
                return

            model_names = list(set(row[0] for row in rows))
            
            stats = {}
            for model_name in model_names:
                model_data = [{'V_Current': row[1], 'A_Current': row[2], 'Offset': row[3]} 
                              for row in rows if row[0] == model_name]
                stats[model_name] = DataAnalyzer.calculate_statistics(model_data)

            self.display_statistics(stats)

        except sqlite3.Error as e:
            messagebox.showerror("错误", f"查询数据库时出错: {e}")

    def display_statistics(self, stats):
        stats_window = tk.Toplevel(self.master)
        stats_window.title("数据统计")
        stats_window.geometry("600x400")

        text_widget = tk.Text(stats_window, wrap=tk.WORD)
        text_widget.pack(expand=True, fill=tk.BOTH)

        for model, model_stats in stats.items():
            text_widget.insert(tk.END, f"Model: {model}\n")
            for measure, measure_stats in model_stats.items():
                text_widget.insert(tk.END, f"  {measure}:\n")
                for stat, value in measure_stats.items():
                    text_widget.insert(tk.END, f"    {stat}: {value:.4f}\n")
            text_widget.insert(tk.END, "\n")

        text_widget.config(state=tk.DISABLED)