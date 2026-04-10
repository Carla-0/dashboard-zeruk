"""
API para Dashboard Zyra - Render Web Service
=============================================
Consulta la base de datos MySQL y devuelve los datos como JSON.
"""

import json
import os
from datetime import datetime, date
from decimal import Decimal

import pymysql
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app, origins=[
    "https://carla-0.github.io",
    "http://localhost:8080",
    "http://127.0.0.1:8080",
])

# Configuración de conexión (usar variables de entorno en producción)
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "34.125.156.253"),
    "database": os.environ.get("DB_NAME", "zeruk"),
    "user": os.environ.get("DB_USER", "mantenedor"),
    "password": os.environ.get("DB_PASSWORD", "DDI!dev%2024"),
    "port": int(os.environ.get("DB_PORT", "3306")),
    "charset": "utf8mb4",
}

TABLE_NAME = "DashbordLk"


def json_serializer(obj):
    """Serializa tipos especiales a JSON."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    raise TypeError(f"Type {type(obj)} not serializable")


@app.route("/", methods=["GET"])
def dashboard_data():
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Obtener nombres de columnas
        cursor.execute(f"DESCRIBE `{TABLE_NAME}`")
        columns_info = cursor.fetchall()
        column_names = [col[0] for col in columns_info]

        # Contar registros
        cursor.execute(f"SELECT COUNT(*) FROM `{TABLE_NAME}`")
        total_rows = cursor.fetchone()[0]

        # Extraer todos los datos
        cursor.execute(f"SELECT * FROM `{TABLE_NAME}`")
        rows = cursor.fetchall()

        # Convertir a lista de diccionarios
        data = []
        for row in rows:
            record = {}
            for i, col_name in enumerate(column_names):
                record[col_name] = row[i]
            data.append(record)

        cursor.close()
        conn.close()

        output = {
            "metadata": {
                "table": TABLE_NAME,
                "total_records": total_rows,
                "columns": column_names,
                "extracted_at": datetime.now().isoformat(),
            },
            "data": data,
        }

        return app.response_class(
            response=json.dumps(output, default=json_serializer, ensure_ascii=False),
            status=200,
            mimetype="application/json",
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
