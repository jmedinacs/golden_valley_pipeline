"""
update_client_structure.py

Scans all client folders under /data and ensures each has the full required folder structure.
Adds missing subfolders silently and logs results.

Author: John Medina
Date: August 9, 2025
"""

import os

# Define relative subfolder paths required inside each client folder
CLIENT_STRUCTURE = [
    "data/raw",
    "data/processed",
    "data/corrected",
    "data/mapping",
    "report/company_level_report",
    "report/incomplete_rows",
    "report/employee_level_report",
    "report/duplication_report",
    "data/mapping",
    "documentation"
    # Add new folders here with proper structure (relative to client folder)
]

def update_client_structure(base_data_dir="../../data"):
    """
    Updates the folder structure for all clients under the given base directory.

    Args:
        base_data_dir (str): Path to the main /data directory
    """
    if not os.path.exists(base_data_dir):
        print(f" Base data directory not found: {base_data_dir}")
        return

    client_names = [
        name for name in os.listdir(base_data_dir)
        if os.path.isdir(os.path.join(base_data_dir, name))
    ]

    for client in client_names:
        print(f"\nChecking structure for client: {client}")
        client_base_path = os.path.join(base_data_dir, client)

        for rel_path in CLIENT_STRUCTURE:
            full_path = os.path.join(client_base_path, rel_path)
            if not os.path.exists(full_path):
                os.makedirs(full_path)
                print(f"Created missing folder: {rel_path}")
            else:
                print(f"Already exists: {rel_path}")

if __name__ == "__main__":
    update_client_structure()
