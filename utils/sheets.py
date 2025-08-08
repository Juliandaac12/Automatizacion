import os
import json
import gspread
import pandas as pd
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from gspread.utils import rowcol_to_a1

SPREADSHEET_ID = "1TqiNXXAgfKlSu2b_Yr9r6AdQU_WacdROsuhcHL0i6Mk"

def conectar_google_sheets():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    if "GCP_SERVICE_ACCOUNT_KEY" in os.environ:
        creds_dict = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    else:
        creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
    client = gspread.authorize(creds)
    print("✅ Conexión con Google Sheets exitosa")
    return client.open_by_key(SPREADSHEET_ID)

def cargar_palabras_clave(sheet):
    try:
        hoja = sheet.worksheet("Palabras Clave")
        palabras_raw = hoja.col_values(6)[7:19]
        palabras_clave = [p.strip() for p in palabras_raw if p.strip()]
        print(f"🔑 {len(palabras_clave)} palabras clave cargadas.")
        return palabras_clave
    except Exception as e:
        print(f"❌ Error al cargar palabras clave: {e}")
        return []

def guardar_en_hoja(resultados, fecha_objetivo):
    if not resultados:
        print("⚠️ No hay resultados para guardar.")
        return

    mes = datetime.strptime(fecha_objetivo, "%Y-%m-%d").strftime("%B").capitalize()
    sheet = conectar_google_sheets()

    columnas = [
        "Número", "FyH Extracción", "FyH Publicación", "ID", "Título",
        "Descripción", "Tipo", "Monto", "Tipo Monto",
        "LINK FICHA", "FyH TERRENO", "OBLIG?", "FyH CIERRE"
    ]
    df = pd.DataFrame(resultados)

    try:
        hoja = sheet.worksheet(mes)
        datos_existentes = hoja.get_all_records()
    except gspread.exceptions.WorksheetNotFound:
        hoja = sheet.add_worksheet(title=mes, rows="1000", cols="20")
        hoja.append_row(columnas)
        # Pintar encabezado de amarillo
        hoja.format("A1:M1", {"backgroundColor": {"red": 1, "green": 1, "blue": 0}})
        datos_existentes = []

    último = int(datos_existentes[-1]["Número"]) if datos_existentes else 0
    ids_exist = {r["ID"] for r in datos_existentes}

    df = df[~df["id"].isin(ids_exist)]
    if df.empty:
        print("📄 No hay nuevas licitaciones.")
        return

    # Renombrar columnas y ordenar
    df["Número"]           = range(último+1, último+1+len(df))
    df["FyH Extracción"]   = df["fecha_extraccion"]
    df["FyH Publicación"]  = df["fecha_publicacion"]
    df["ID"]               = df["id"]
    df["Título"]           = df["titulo"]
    df["Descripción"]      = df["descripcion"]
    df["Tipo"]             = df["tipo"]
    df["Monto"]            = df["monto"]
    df["Tipo Monto"]       = df["tipo_monto"]
    df["LINK FICHA"]       = df["link_ficha"]
    df["FyH TERRENO"]      = df["fecha_visita"]
    df["OBLIG?"]           = df["visita_obligatoria"]
    df["FyH CIERRE"]       = df["fecha_cierre"]
    df = df[columnas]

    # Insertar datos
    start_row = len(datos_existentes) + 2
    hoja.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")

    # Formato condicional manual (rojo/verde) en columnas clave
    verdes = {"backgroundColor": {"red": 0.8, "green": 0.94, "blue": 0.75}}
    rojos  = {"backgroundColor": {"red": 0.98, "green": 0.7,  "blue": 0.7}}
    cols_idx = {"Monto":8, "Tipo Monto":9, "FyH TERRENO":11, "OBLIG?":12}
    for i, row in enumerate(df.itertuples(index=False), start=start_row):
        for name, col in cols_idx.items():
            val = getattr(row, name.replace(" ", "_").lower())
            cell = rowcol_to_a1(i, col)
            hoja.format(cell, verdes if val != "NF" else rojos)

    print(f"✅ {len(df)} nuevas licitaciones guardadas en '{mes}'.")
