# -*- coding: utf-8 -*-
"""
Dashboard de Producción — Zeruk Brokers
Tabla: zeruk.DashbordLk
Métricas: Fee Neto USD, Prima Neta USD, Comisión Zyra USD, Comisión Producer USD, Cantidad
Dimensiones: Aseguradora, Producer, Ramo, Razón Social, Tipo de Venta
Filtro temporal: Inicio de Vigencia, con comparativo vs período anterior.
"""
import os
import calendar
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pymysql
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

# ------------------------------------------------------------------ #
# Configuración de base de datos (en Render definir como env vars)
# ------------------------------------------------------------------ #
DB_CONFIG = dict(
    host=os.environ.get("DB_HOST", "34.125.156.253"),
    user=os.environ.get("DB_USER", "mantenedor"),
    password=os.environ.get("DB_PASSWORD", "DDI!dev%2024"),
    database=os.environ.get("DB_NAME", "zeruk"),
    charset="utf8mb4",
    connect_timeout=10,
    read_timeout=30,
    write_timeout=30,
    cursorclass=pymysql.cursors.DictCursor,
)

TABLA = "DashbordLk"
TZ_LIMA = ZoneInfo("America/Lima")

# Whitelist de dimensiones -> columna real (evita inyección SQL)
# Normalización de Tipo de Venta: unifica "Cross up", "Up/Cross-selling",
# "UpcrossSelling" y variantes similares bajo un solo valor.
TIPO_VENTA_SQL = (
    "CASE WHEN UPPER(REPLACE(REPLACE(REPLACE("
    "DashbordLkTipoVenta, '/', ''), '-', ''), ' ', '')) "
    "IN ('CROSSUP', 'UPCROSS', 'UPCROSSSELLING', 'CROSSUPSELLING') "
    "THEN 'Up/Cross-selling' ELSE DashbordLkTipoVenta END"
)

# ------------------------------------------------------------------ #
# Normalización de valores
# Para unificar variantes, agregar entradas a los diccionarios ALIAS:
#   "Nombre canónico": ("variante 1", "variante 2", ...)
# La comparación ignora mayúsculas/minúsculas y espacios al borde.
# ------------------------------------------------------------------ #
PRODUCER_ALIAS = {
    "RIERA RIOS MIGUEL WALTER": (
        "WALTER MIGUEL RIERA RIOS",
        "RIERA RIOS WALTER",
    ),
}

ASEGURADORA_ALIAS = {
    "Pacifico": (
        "pacifico",
        "Pacifico Seguros",
        "Pacífico",
        "Pacífico Seguros",
    ),
}


def sql_normalizacion(columna, alias_dict, valor_vacio=None):
    """Genera un CASE SQL que unifica variantes y opcionalmente
    reemplaza valores vacíos por un texto dado."""
    casos = []
    if valor_vacio:
        vv = valor_vacio.replace("'", "''")
        casos.append(f"WHEN TRIM({columna}) = '' THEN '{vv}'")
    for canonico, variantes in alias_dict.items():
        todas = ", ".join(
            "'" + v.upper().replace("'", "''") + "'"
            for v in (canonico,) + tuple(variantes)
        )
        canon = canonico.replace("'", "''")
        casos.append(f"WHEN UPPER(TRIM({columna})) IN ({todas}) THEN '{canon}'")
    return "CASE " + " ".join(casos) + f" ELSE {columna} END"


PRODUCER_SQL = sql_normalizacion(
    "DashbordLkProducer", PRODUCER_ALIAS, valor_vacio="Cartera Zeruk"
)
ASEGURADORA_SQL = sql_normalizacion(
    "DashbordLkAseguradora", ASEGURADORA_ALIAS
)

DIMENSIONES = {
    "aseguradora": ASEGURADORA_SQL,
    "producer": PRODUCER_SQL,
    "ramo": "DashbordLkRamo",
    "razon_social": "DashbordLkRazonSocial",
    "tipo_venta": TIPO_VENTA_SQL,
}

METRICAS_SQL = """
    COUNT(*)                                  AS cantidad,
    COALESCE(SUM(DashbordLkFeeNetoUSD), 0)    AS fee_neto_usd,
    COALESCE(SUM(DashbordLkPrimaNetaUSD), 0)  AS prima_neta_usd,
    COALESCE(SUM(DashbordLkMCZyraUSD), 0)     AS comision_zyra_usd,
    COALESCE(SUM(DashbordLkMCProducerUSD), 0) AS comision_producer_usd
"""

# Ramos clasificados como "Riesgos Humanos" (por palabra clave, en mayúsculas).
# Todo ramo que no coincida se considera "Riesgos Generales".
# Ajustar esta lista según el catálogo real de ramos de Zeruk.
RAMOS_HUMANOS_KEYWORDS = (
    "VIDA", "SALUD", "SCTR", "EPS", "ACCIDENTES",
    "ASISTENCIA", "ONCOL", "DESGRAVAMEN", "SEPELIO",
)


def es_ramo_humano(ramo) -> bool:
    r = (ramo or "").upper()
    return any(k in r for k in RAMOS_HUMANOS_KEYWORDS)


def get_conn():
    return pymysql.connect(**DB_CONFIG)


# ------------------------------------------------------------------ #
# Helpers de fechas
# ------------------------------------------------------------------ #
def parse_fecha(s, default):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return default


def es_inicio_de_mes(d: date) -> bool:
    return d.day == 1


def es_fin_de_mes(d: date) -> bool:
    return d.day == calendar.monthrange(d.year, d.month)[1]


def restar_meses(d: date, meses: int) -> date:
    """Resta meses conservando día 1 (se usa solo con inicios de mes)."""
    total = d.year * 12 + (d.month - 1) - meses
    return date(total // 12, total % 12 + 1, 1)


def periodo_anterior(desde: date, hasta: date):
    """
    Calcula el período inmediatamente anterior de la misma duración.
    Si el rango está alineado a meses calendario completos, retrocede
    en meses (jun 2026 -> may 2026; Q2 -> Q1), si no, retrocede en días.
    """
    if es_inicio_de_mes(desde) and es_fin_de_mes(hasta):
        n_meses = (hasta.year * 12 + hasta.month) - (desde.year * 12 + desde.month) + 1
        prev_desde = restar_meses(desde, n_meses)
        prev_hasta = desde - timedelta(days=1)
        return prev_desde, prev_hasta
    duracion = (hasta - desde).days
    prev_hasta = desde - timedelta(days=1)
    prev_desde = prev_hasta - timedelta(days=duracion)
    return prev_desde, prev_hasta


# ------------------------------------------------------------------ #
# Construcción de filtros
# ------------------------------------------------------------------ #
def construir_filtros(args, desde, hasta):
    """Retorna (clausula_where, parametros) parametrizados."""
    where = ["DashbordLkInicioVigencia BETWEEN %s AND %s"]
    params = [desde, hasta]
    mapeo = {
        "aseguradora": ASEGURADORA_SQL,
        "producer": PRODUCER_SQL,
        "ramo": "DashbordLkRamo",
        "tipo_venta": TIPO_VENTA_SQL,
        "razon_social": "DashbordLkRazonSocial",
    }
    for clave, columna in mapeo.items():
        valor = args.get(clave, "").strip()
        if valor and valor.lower() != "todos":
            where.append(f"{columna} = %s")
            params.append(valor)
    return " AND ".join(where), params


def fila_a_float(fila):
    return {
        "cantidad": int(fila["cantidad"]),
        "fee_neto_usd": float(fila["fee_neto_usd"]),
        "prima_neta_usd": float(fila["prima_neta_usd"]),
        "comision_zyra_usd": float(fila["comision_zyra_usd"]),
        "comision_producer_usd": float(fila["comision_producer_usd"]),
    }


# ------------------------------------------------------------------ #
# Rutas
# ------------------------------------------------------------------ #
@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/api/filtros")
def api_filtros():
    """Valores distintos para poblar los selectores."""
    consultas = {
        "aseguradoras": ASEGURADORA_SQL,
        "producers": PRODUCER_SQL,
        "ramos": "DashbordLkRamo",
        "tipos_venta": TIPO_VENTA_SQL,
    }
    resultado = {}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for clave, col in consultas.items():
                cur.execute(
                    f"SELECT DISTINCT {col} AS v FROM {TABLA} "
                    f"WHERE TRIM({col}) <> '' ORDER BY v"
                )
                resultado[clave] = [r["v"] for r in cur.fetchall()]
            # Rango de fechas disponible en la data
            cur.execute(
                f"SELECT MIN(DashbordLkInicioVigencia) AS minf, "
                f"MAX(DashbordLkInicioVigencia) AS maxf FROM {TABLA}"
            )
            r = cur.fetchone()
            resultado["fecha_min"] = r["minf"].isoformat() if r["minf"] else None
            resultado["fecha_max"] = r["maxf"].isoformat() if r["maxf"] else None
    finally:
        conn.close()
    return jsonify(resultado)


@app.route("/api/data")
def api_data():
    hoy = datetime.now(TZ_LIMA).date()
    # Por defecto: mes calendario actual
    default_desde = hoy.replace(day=1)
    default_hasta = hoy.replace(day=calendar.monthrange(hoy.year, hoy.month)[1])

    desde = parse_fecha(request.args.get("desde"), default_desde)
    hasta = parse_fecha(request.args.get("hasta"), default_hasta)
    if desde > hasta:
        desde, hasta = hasta, desde

    prev_desde, prev_hasta = periodo_anterior(desde, hasta)

    dimension = request.args.get("dimension", "aseguradora")
    col_dim = DIMENSIONES.get(dimension, DIMENSIONES["aseguradora"])

    where_actual, params_actual = construir_filtros(request.args, desde, hasta)
    where_prev, params_prev = construir_filtros(request.args, prev_desde, prev_hasta)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # --- KPIs período actual y anterior ---
            cur.execute(f"SELECT {METRICAS_SQL} FROM {TABLA} WHERE {where_actual}", params_actual)
            kpis_actual = fila_a_float(cur.fetchone())

            cur.execute(f"SELECT {METRICAS_SQL} FROM {TABLA} WHERE {where_prev}", params_prev)
            kpis_anterior = fila_a_float(cur.fetchone())

            # --- Desglose por dimensión (actual) ---
            cur.execute(
                f"SELECT {col_dim} AS nombre, {METRICAS_SQL} "
                f"FROM {TABLA} WHERE {where_actual} "
                f"GROUP BY {col_dim} ORDER BY prima_neta_usd DESC",
                params_actual,
            )
            desglose_actual = cur.fetchall()

            # --- Desglose por dimensión (anterior, para variación) ---
            cur.execute(
                f"SELECT {col_dim} AS nombre, {METRICAS_SQL} "
                f"FROM {TABLA} WHERE {where_prev} GROUP BY {col_dim}",
                params_prev,
            )
            prev_map = {r["nombre"]: fila_a_float(r) for r in cur.fetchall()}

            # --- Evolución mensual (rango anterior + actual, para el gráfico) ---
            where_tend, params_tend = construir_filtros(request.args, prev_desde, hasta)
            cur.execute(
                f"SELECT DATE_FORMAT(DashbordLkInicioVigencia, '%%Y-%%m') AS mes, {METRICAS_SQL} "
                f"FROM {TABLA} WHERE {where_tend} GROUP BY mes ORDER BY mes",
                params_tend,
            )
            tendencia = [
                {"mes": r["mes"], **fila_a_float(r)} for r in cur.fetchall()
            ]

            # --- Resumen por tipo de venta y categoría de riesgo ---
            cur.execute(
                f"SELECT {TIPO_VENTA_SQL} AS tipo, DashbordLkRamo AS ramo, "
                f"COALESCE(SUM(DashbordLkPrimaNetaUSD), 0) AS prima, "
                f"COALESCE(SUM(DashbordLkFeeNetoUSD), 0) AS fee "
                f"FROM {TABLA} WHERE {where_actual} GROUP BY tipo, ramo",
                params_actual,
            )
            filas_rv = cur.fetchall()
    finally:
        conn.close()

    # Armar matriz Primas / Fees / Riesgos Generales / Riesgos Humanos
    def celda():
        return {"total": 0.0, "por_tipo": {}}

    resumen = {
        "primas": celda(),
        "fees": celda(),
        "riesgos_generales": celda(),
        "riesgos_humanos": celda(),
    }
    for f in filas_rv:
        tipo = (f["tipo"] or "").strip() or "(Sin tipo)"
        prima, fee = float(f["prima"]), float(f["fee"])
        for clave, valor in (("primas", prima), ("fees", fee)):
            resumen[clave]["total"] += valor
            resumen[clave]["por_tipo"][tipo] = resumen[clave]["por_tipo"].get(tipo, 0.0) + valor
        cat = "riesgos_humanos" if es_ramo_humano(f["ramo"]) else "riesgos_generales"
        resumen[cat]["total"] += prima
        resumen[cat]["por_tipo"][tipo] = resumen[cat]["por_tipo"].get(tipo, 0.0) + prima

    tipos_orden = sorted(
        resumen["primas"]["por_tipo"],
        key=lambda t: -resumen["primas"]["por_tipo"][t],
    )

    desglose = []
    for r in desglose_actual:
        nombre = r["nombre"] if r["nombre"] and str(r["nombre"]).strip() else "(Sin asignar)"
        act = fila_a_float(r)
        prev = prev_map.get(r["nombre"], None)
        desglose.append({
            "nombre": nombre,
            **act,
            "prev_fee_neto_usd": prev["fee_neto_usd"] if prev else 0.0,
            "prev_prima_neta_usd": prev["prima_neta_usd"] if prev else 0.0,
            "prev_cantidad": prev["cantidad"] if prev else 0,
        })

    return jsonify({
        "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()},
        "periodo_anterior": {"desde": prev_desde.isoformat(), "hasta": prev_hasta.isoformat()},
        "kpis": {"actual": kpis_actual, "anterior": kpis_anterior},
        "dimension": dimension,
        "desglose": desglose,
        "tendencia": tendencia,
        "resumen_venta": {"tipos": tipos_orden, "columnas": resumen},
        "actualizado": datetime.now(TZ_LIMA).strftime("%d/%m/%Y %H:%M"),
    })


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
