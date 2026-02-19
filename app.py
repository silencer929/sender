
import customtkinter
import tkinter
from tkinter import filedialog, messagebox
from pathlib import Path
import threading
import psycopg2
from core_sms_sender import SMSSender, get_file_headers, get_row_count
from gateway_dialog import GatewayDialog

# --- Database setup ---
def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname="sms_app",
            user="postgres",
            password="root",  # Replace with your DB password
            host="localhost",
            port="5432"
        )
        return conn
    except psycopg2.OperationalError as e:
        messagebox.showerror("Database Error", f"Could not connect to PostgreSQL: {e}")
        return None

def initialize_db():
    conn = get_db_connection()
    if conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS gateways (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    url TEXT NOT NULL,
                    token TEXT NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    gateway_id INTEGER NOT NULL,
                    phone_number TEXT NOT NULL,
                    message TEXT NOT NULL,
                    status TEXT NOT NULL,
                    sent_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (gateway_id) REFERENCES gateways (id)
                );
            """)
            conn.commit()
        conn.close()

# --- Main Application ---
class App(customtkinter.CTk):
    def __init__(self):
        super().__init__()

        self.title("Bulk SMS Sender")
        self.geometry("1200x750")

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        # --- Sidebar ---
        self.sidebar_frame = customtkinter.CTkFrame(self, width=200, corner_radius=0)
        self.sidebar_frame.grid(row=0, column=0, rowspan=4, sticky="nsew")

        self.logo_label = customtkinter.CTkLabel(self.sidebar_frame, text="Settings", font=customtkinter.CTkFont(size=20, weight="bold"))
        self.logo_label.grid(row=0, column=0, padx=20, pady=(20, 10))

        self.gateway_label = customtkinter.CTkLabel(self.sidebar_frame, text="Gateway:")
        self.gateway_label.grid(row=1, column=0, padx=20, pady=5)
        self.gateway_menu = customtkinter.CTkOptionMenu(self.sidebar_frame, values=["Add New..."], command=self.on_gateway_select)
        self.gateway_menu.grid(row=2, column=0, padx=20, pady=(0, 10))

        self.file_button = customtkinter.CTkButton(self.sidebar_frame, text="Select File", command=self.select_file)
        self.file_button.grid(row=3, column=0, padx=20, pady=10)
        self.file_label = customtkinter.CTkLabel(self.sidebar_frame, text="No file selected", wraplength=180)
        self.file_label.grid(row=4, column=0, padx=20, pady=(0, 10))
        
        self.variables_label = customtkinter.CTkLabel(self.sidebar_frame, text="Template Variables:")
        self.variables_label.grid(row=5, column=0, padx=20, pady=10)
        self.variables_textbox = customtkinter.CTkTextbox(self.sidebar_frame, height=100, state="disabled")
        self.variables_textbox.grid(row=6, column=0, padx=20, pady=(0, 10))

        # --- Main Content ---
        self.main_frame = customtkinter.CTkFrame(self, corner_radius=0)
        self.main_frame.grid(row=0, column=1, rowspan=4, sticky="nsew", padx=20, pady=20)
        self.main_frame.grid_columnconfigure(0, weight=1)

        self.template_label = customtkinter.CTkLabel(self.main_frame, text="Message Template:")
        self.template_label.grid(row=0, column=0, columnspan=4, padx=10, pady=5, sticky="w")
        self.template_textbox = customtkinter.CTkTextbox(self.main_frame, height=150)
        self.template_textbox.grid(row=1, column=0, columnspan=4, padx=10, pady=10, sticky="nsew")

        # Additional settings
        self.limit_label = customtkinter.CTkLabel(self.main_frame, text="Limit:")
        self.limit_label.grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.limit_entry = customtkinter.CTkEntry(self.main_frame, placeholder_text="0 for no limit")
        self.limit_entry.grid(row=2, column=1, padx=10, pady=5)
        
        self.delay_label = customtkinter.CTkLabel(self.main_frame, text="Delay (s):")
        self.delay_label.grid(row=2, column=2, padx=10, pady=5, sticky="w")
        self.delay_entry = customtkinter.CTkEntry(self.main_frame)
        self.delay_entry.insert(0, "0.2")
        self.delay_entry.grid(row=2, column=3, padx=10, pady=5)

        self.prefix_label = customtkinter.CTkLabel(self.main_frame, text="Country Prefix:")
        self.prefix_label.grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.prefix_entry = customtkinter.CTkEntry(self.main_frame, placeholder_text="+254")
        self.prefix_entry.grid(row=3, column=1, padx=10, pady=5)

        self.start_row_label = customtkinter.CTkLabel(self.main_frame, text="Start from Row:")
        self.start_row_label.grid(row=3, column=2, padx=10, pady=5, sticky="w")
        self.start_row_entry = customtkinter.CTkEntry(self.main_frame)
        self.start_row_entry.insert(0, "1")
        self.start_row_entry.grid(row=3, column=3, padx=10, pady=5)

        self.skip_duplicates_checkbox = customtkinter.CTkCheckBox(self.main_frame, text="Skip numbers that have already received a message today")
        self.skip_duplicates_checkbox.grid(row=4, column=0, columnspan=4, padx=10, pady=10, sticky="w")
        
        self.send_button = customtkinter.CTkButton(self.main_frame, text="Send Messages", command=self.send_messages)
        self.send_button.grid(row=5, column=0, columnspan=4, padx=10, pady=20)
        
        self.progress_bar = customtkinter.CTkProgressBar(self.main_frame)
        self.progress_bar.set(0)
        self.progress_bar.grid(row=6, column=0, columnspan=4, padx=10, pady=10, sticky="ew")

        self.log_textbox = customtkinter.CTkTextbox(self.main_frame, height=200, state="disabled")
        self.log_textbox.grid(row=7, column=0, columnspan=4, padx=10, pady=10, sticky="nsew")

        # --- Initial state ---
        self.contact_file = None
        self.gateways = {}
        self.total_rows = 0
        self.load_gateways()

    def select_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx *.xls"), ("CSV files", "*.csv")])
        if file_path:
            self.contact_file = Path(file_path)
            self.file_label.configure(text=self.contact_file.name)
            self.display_template_variables()
            
            # Start row counting in a separate thread to not freeze the GUI
            threading.Thread(target=self.count_rows).start()


    def count_rows(self):
        self.progress_bar.configure(mode="indeterminate")
        self.progress_bar.start()
        self.total_rows = get_row_count(self.contact_file)
        self.progress_bar.stop()
        if self.total_rows is not None:
            self.progress_bar.configure(mode="determinate")
            self.progress_bar.set(0)
        else:
            # Keep it indeterminate if count fails
            self.progress_bar.configure(mode="indeterminate")
            self.progress_bar.start()


    def display_template_variables(self):
        headers = get_file_headers(self.contact_file)
        self.variables_textbox.configure(state="normal")
        self.variables_textbox.delete("1.0", "end")
        self.variables_textbox.insert("1.0", "\n".join([f"{{{{{h}}}}}" for h in headers]))
        self.variables_textbox.configure(state="disabled")

    def load_gateways(self):
        conn = get_db_connection()
        if conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, name, url, token FROM gateways")
                gateway_list = ["Add New..."]
                self.gateways = {}
                for row in cur.fetchall():
                    self.gateways[row[1]] = {"id": row[0], "url": row[2], "token": row[3]}
                    gateway_list.append(row[1])
                self.gateway_menu.configure(values=gateway_list)
            conn.close()

    def on_gateway_select(self, selection):
        if selection == "Add New...":
            self.add_new_gateway()

    def add_new_gateway(self):
        dialog = GatewayDialog(self)
        self.wait_window(dialog)
        if dialog.gateway_data:
            conn = get_db_connection()
            if conn:
                with conn.cursor() as cur:
                    try:
                        cur.execute("INSERT INTO gateways (name, url, token) VALUES (%s, %s, %s)",
                                    (dialog.gateway_data["name"], dialog.gateway_data["url"], dialog.gateway_data["token"]))
                        conn.commit()
                        self.load_gateways()
                        self.gateway_menu.set(dialog.gateway_data["name"])
                    except psycopg2.Error as e:
                        messagebox.showerror("Database Error", f"Could not save gateway: {e}")
                conn.close()

    def send_messages(self):
        if not self.contact_file:
            messagebox.showerror("Error", "Please select a contact file.")
            return
        
        template = self.template_textbox.get("1.0", "end-1c")
        if not template:
            messagebox.showerror("Error", "Please enter a message template.")
            return

        selected_gateway = self.gateway_menu.get()
        if selected_gateway == "Add New..." or not selected_gateway:
            messagebox.showerror("Error", "Please select a gateway.")
            return

        config = {
            "gateway_id": self.gateways[selected_gateway]["id"],
            "gateway_url": self.gateways[selected_gateway]["url"],
            "auth": self.gateways[selected_gateway]["token"],
            "retries": 2,
            "timeout": 10.0,
            "delay": float(self.delay_entry.get() or 0.2),
            "limit": int(self.limit_entry.get() or 0),
            "start_row": int(self.start_row_entry.get() or 1),
            "country_prefix": self.prefix_entry.get() or None,
            "skip_duplicates": self.skip_duplicates_checkbox.get(),
            "output_file": "gui_sms_log.csv"
        }
        
        self.log_textbox.configure(state="normal")
        self.log_textbox.delete("1.0", "end")
        self.log_textbox.insert("end", "Starting to send messages...\n")
        self.log_textbox.configure(state="disabled")
        self.progress_bar.set(0)

        db_conn = get_db_connection()
        sender = SMSSender(config, db_conn)
        thread = threading.Thread(target=sender.send_messages, args=(self.contact_file, template, self.total_rows, False, self.update_progress, self.on_sending_complete))
        thread.start()

    def update_progress(self, message, progress):
        """Callback to update progress bar and log"""
        self.log_textbox.configure(state="normal")
        self.log_textbox.insert("end", message + "\n")
        self.log_textbox.see("end")
        self.log_textbox.configure(state="disabled")
        
        if self.total_rows:
            self.progress_bar.set(progress / self.total_rows)

    def on_sending_complete(self, message):
        self.log_textbox.configure(state="normal")
        self.log_textbox.insert("end", f"\n---\n{message}\n---\n")
        self.log_textbox.see("end")
        self.log_textbox.configure(state="disabled")
        if self.total_rows:
            self.progress_bar.set(1)
        else:
            self.progress_bar.stop()
            self.progress_bar.configure(mode="determinate")


if __name__ == "__main__":
    initialize_db()
    app = App()
    app.mainloop()
