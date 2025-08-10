'''
Created on Aug 9, 2025

@author: jarpy
'''

import os 
import tkinter as tk 
from tkinter import messagebox

def create_client(client_name="missing_client_name"):
    """
    Creates a new client directory structure under ../../data, if it doesn't already exist.
    If the client already exists, a warning pop-up will appear.

    Args:
        client_name (str): Name of the client to create folders for
    """
    base_path = os.path.join("../../data",client_name)
    
    if os.path.exists(base_path):
        root = tk.Tk()
        root.withdraw()
        messagebox.showwarning("Client Already Exists\nPlease enter another client name.")
        root.destroy()
        return
    
    print(f"\nSafe to proceed: {client_name} available.")
    
    # Build the full directory structure
    subfolders = [ 
        os.path.join(base_path,"data","raw"),
        os.path.join(base_path,"data","processed"),
        os.path.join(base_path,"data","corrected"),
        os.path.join(base_path,"data","mapping"),
        os.path.join(base_path,"report","company_level_report"),
        os.path.join(base_path,"report","incomplete_rows")        
    ]
    
    for folder in subfolders:
        os.makedirs(folder,exist_ok=True)
        print(f"Created: {folder}")
        
    print(f"Client folder structure created for '{client_name}'.")

if __name__ == '__main__':
    create_client(client_name = "test_client")




if __name__ == '__main__':
    pass