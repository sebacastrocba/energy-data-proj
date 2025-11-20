#!/usr/bin/env python3
"""Genera la Fernet key necesaria para Airflow"""

from cryptography.fernet import Fernet

fernet_key = Fernet.generate_key().decode()

print("Copia esta linea a tu archivo .env:")
print(f"AIRFLOW_FERNET_KEY={fernet_key}")
