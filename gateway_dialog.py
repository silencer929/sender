import customtkinter
from tkinter import messagebox

class GatewayDialog(customtkinter.CTkToplevel):
    def __init__(self, parent):
        super().__init__(parent)

        self.title("Gateway Configuration")
        self.geometry("400x300")

        self.grid_columnconfigure(1, weight=1)

        self.name_label = customtkinter.CTkLabel(self, text="Name:")
        self.name_label.grid(row=0, column=0, padx=10, pady=10, sticky="w")
        self.name_entry = customtkinter.CTkEntry(self)
        self.name_entry.grid(row=0, column=1, padx=10, pady=10, sticky="ew")

        self.url_label = customtkinter.CTkLabel(self, text="URL:")
        self.url_label.grid(row=1, column=0, padx=10, pady=10, sticky="w")
        self.url_entry = customtkinter.CTkEntry(self)
        self.url_entry.grid(row=1, column=1, padx=10, pady=10, sticky="ew")

        self.token_label = customtkinter.CTkLabel(self, text="Token:")
        self.token_label.grid(row=2, column=0, padx=10, pady=10, sticky="w")
        self.token_entry = customtkinter.CTkEntry(self, show="*")
        self.token_entry.grid(row=2, column=1, padx=10, pady=10, sticky="ew")

        self.save_button = customtkinter.CTkButton(self, text="Save", command=self.save)
        self.save_button.grid(row=3, column=1, padx=10, pady=20)

        self.gateway_data = None

    def save(self):
        name = self.name_entry.get()
        url = self.url_entry.get()
        token = self.token_entry.get()

        if not all([name, url, token]):
            messagebox.showerror("Error", "Please fill all fields.")
            return

        self.gateway_data = {"name": name, "url": url, "token": token}
        self.destroy()
