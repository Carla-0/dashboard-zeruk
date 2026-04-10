#!/usr/bin/env python3
"""
Script de extracción de datos para Dashboard de Producción - Zeruk Brokers
===========================================================================
Este script se conecta a la base de datos MySQL, extrae los datos de la tabla
DashboardLk y los exporta como archivo JSON para el dashboard HTML.

REQUISITOS:
    pip install pymysql

USO:
    python extract_data.py

El script generará el archivo 'dashboard_data.json' en la misma carpeta.
Luego abre 'dashboard.html' en tu navegador para ver el dashboard.
"""

import json
import sys
from datetime import datetime, date
from decimal import Decimal

# ─── Configuración de conexión ───────────────────────────────────────────────
DB_CONFIG = {
    "host": "34.125.156.253",
    "database": "zeruk",
    "user": "mantenedor",
    "password": "DDI!dev%2024",
    "port": 3306,
    "charset": "utf8mb4",
}

TABLE_NAME = "DashbordLk"
OUTPUT_FILE = "dashboard_data.json"


def json_serializer(obj):
    """Serializa tipos especiales a JSON."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    raise TypeError(f"Type {type(obj)} not serializable")


def main():
    try:
        import pymysql
    except ImportError:
        print("=" * 60)
        print("ERROR: PyMySQL no está instalado.")
        print("Ejecuta: pip install pymysql")
        print("=" * 60)
        sys.exit(1)

    print("Conectando a la base de datos...")
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✓ Conexión exitosa")
    except Exception as e:
        print(f"✗ Error de conexión: {e}")
        sys.exit(1)

    # 1. Obtener estructura de la tabla
    print(f"\nEstructura de la tabla '{TABLE_NAME}':")
    print("-" * 50)
    cursor.execute(f"DESCRIBE `{TABLE_NAME}`")
    columns_info = cursor.fetchall()
    column_names = []
    for col in columns_info:
        col_name, col_type = col[0], col[1]
        column_names.append(col_name)
        print(f"  {col_name:<30} {col_type}")

    # 2. Contar registros
    cursor.execute(f"SELECT COUNT(*) FROM `{TABLE_NAME}`")
    total_rows = cursor.fetchone()[0]
    print(f"\nTotal de registros: {total_rows:,}")

    # 3. Extraer todos los datos
    print(f"\nExtrayendo datos...")
    cursor.execute(f"SELECT * FROM `{TABLE_NAME}`")
    rows = cursor.fetchall()

    # 4. Convertir a lista de diccionarios
    data = []
    for row in rows:
        record = {}
        for i, col_name in enumerate(column_names):
            record[col_name] = row[i]
        data.append(record)

    # 5. Guardar JSON
    output = {
        "metadata": {
            "table": TABLE_NAME,
            "total_records": total_rows,
            "columns": column_names,
            "extracted_at": datetime.now().isoformat(),
        },
        "data": data,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, default=json_serializer, ensure_ascii=False, indent=2)

    print(f"✓ Datos exportados a '{OUTPUT_FILE}'")
    print(f"  ({total_rows:,} registros, {len(column_names)} columnas)")
    print(f"\nAhora abre 'dashboard.html' en tu navegador.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
