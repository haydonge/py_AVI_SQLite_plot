import tkinter as tk
from optimized_csv_to_sqlite_app import OptimizedCSVToSQLiteApp

if __name__ == "__main__":
    root = tk.Tk()
    app = OptimizedCSVToSQLiteApp(root)
    root.mainloop()