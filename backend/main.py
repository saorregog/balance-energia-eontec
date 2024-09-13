# Run server with terminal command: uvicorn main:app --reload

# Server-related libraries
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import socketio

# ETL-relatedlibraries
import requests
import polars as pl

# Other libraries
import os
from dotenv import load_dotenv
from datetime import date

# Modules
from google_auth import gauth


# Load environment variables from .env file
load_dotenv()


# FastAPI class
fastapi = FastAPI()
fastapi.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173"])

# SocketIO class
sio = socketio.AsyncServer(
    async_mode="asgi", cors_allowed_origins=["http://localhost:5173"]
)

# ASGI app
app = socketio.ASGIApp(socketio_server=sio, other_asgi_app=fastapi)


@fastapi.get("/")
def checkServer():
    return "Servidor activo"


@sio.event
def connect(sid, environ):
    print(f"Connected: SID {sid}")


@sio.event
def disconnect(sid):
    print(f"Disconnected: SID {sid}")


@sio.event
async def start_etl(sid):
    print(f"Starting ETL: SID {sid}")

    # Class with methods to interact with the required drive
    try:
        gdrive = gauth()
    except Exception("Authentication failed"):
        # Emit a message to the client indicating that the authentication to Google Drive failed
        await sio.emit(
            "events_messages",
            {"status": "error", "content": "Error de autenticación en Google Drive"},
        )
        await sio.emit("stop_processing")
        return
    else:
        # Emit a message to the client indicating that the authentication to Google Drive was successful
        await sio.emit(
            "events_messages",
            {"status": "success", "content": "Autenticación exitosa en Google Drive"},
        )

    # EXTRACT 1
    # Check if at least one file exists in the extraction folder
    file_list = gdrive.ListFile(
        {"q": f"'{os.getenv("EXTRACTION_FOLDER_ID")}' in parents and trashed=false"}
    ).GetList()

    if len(file_list) == 0:
        # Emit a message to the client indicating that no file exists in the extraction folder
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": "La carpeta de extracción no contiene algún archivo",
            },
        )
        await sio.emit("stop_processing")
        return

    # Emit a message to the client indicating that at least one file exists in the extraction folder
    await sio.emit(
        "events_messages",
        {
            "status": "success",
            "content": "La carpeta de extracción contiene al menos un archivo",
        },
    )

    # Get the first xlsx file in the folder or emit a message to the client indicating that no xlsx file exists in the extraction folder
    xlsx_file = {}

    for index in range(0, len(file_list)):
        if file_list[index]["title"].endswith(".xlsx"):
            xlsx_file = file_list[index]
            await sio.emit(
                "events_messages",
                {
                    "status": "success",
                    "content": "La carpeta de extracción contiene un archivo XLSX",
                },
            )
            break

        if index == len(file_list) - 1:
            await sio.emit(
                "events_messages",
                {
                    "status": "error",
                    "content": "La carpeta de extracción no contiene un archivo XLSX",
                },
            )
            await sio.emit("stop_processing")
            return

    # Save the content of the XLSX file in a local file
    capacidad_generacion = gdrive.CreateFile({"id": xlsx_file["id"]})
    capacidad_generacion.GetContentFile("capacidad_generacion.xlsx")

    # TRANSFORM 1
    # Read the content of the file using polars, excluding columns "PLANTA" and "GENERADOR"
    df_capacidad_generacion = pl.read_excel(
        source="capacidad_generacion.xlsx", has_header=False, columns=[0, 3, 4]
    )
    print(df_capacidad_generacion)

    # Filter useless rows
    df_capacidad_generacion = df_capacidad_generacion.tail(-4)
    print(df_capacidad_generacion)

    # VALIDATIONS
    if (
        df_capacidad_generacion.select(
            pl.when(pl.col("column_1").is_not_null()).then(1).otherwise(None)
        )
        .count()
        .item()
        < 144
    ):
        # Emit a message to the client indicating that "capacidad de generación" file has null values in the "FECHA" column
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": 'El archivo de capacidad de generación contiene valores nulos en la columna "FECHA"',
            },
        )
        await sio.emit("stop_processing")
        return

    if (
        df_capacidad_generacion.select(
            pl.when(pl.col("column_2").is_not_null()).then(1).otherwise(None)
        )
        .count()
        .item()
        < 144
    ):
        # Emit a message to the client indicating that "capacidad de generación" file has null values in the "CAPACIDAD (Kwh)" column
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": 'El archivo de capacidad de generación contiene valores nulos en la columna "CAPACIDAD (Kwh)"',
            },
        )
        await sio.emit("stop_processing")
        return

    if (
        df_capacidad_generacion.select(
            pl.when(pl.col("column_3").is_not_null()).then(1).otherwise(None)
        )
        .count()
        .item()
        < 144
    ):
        # Emit a message to the client indicating that "capacidad de generación" file has null values in the "CODIGO" column
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": 'El archivo de capacidad de generación contiene valores nulos en la columna "CODIGO"',
            },
        )
        await sio.emit("stop_processing")
        return

    try:
        # Compute "Fecha" and "Hora" columns
        df_capacidad_generacion = df_capacidad_generacion.with_columns(
            pl.col("column_1")
            .str.slice(offset=0, length=10)
            .str.to_date("%Y-%m-%d")
            .alias("Fecha"),
            pl.col("column_1").str.slice(offset=11, length=2).alias("Hora"),
        )
    except pl.exceptions.InvalidOperationError:
        # Emit a message to the client indicating that "capacidad de generación" file has at least one row with a non-valid date
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": "El archivo de capacidad de generación contiene al menos un registro con una fecha no válida",
            },
        )
        await sio.emit("stop_processing")
        return

    try:
        # Rename the columns 2 and 3 to "Capacidad" and "Codigo"; casting "Capacidad" column to Int64 data type
        df_capacidad_generacion = df_capacidad_generacion.select(
            pl.col("Fecha"),
            pl.col("Hora"),
            pl.col("column_2").cast(pl.Int64).alias("Capacidad (kWh)"),
            pl.col("column_3").alias("Codigo"),
        )
    except pl.exceptions.InvalidOperationError:
        # Emit a message to the client indicating that "capacidad de generación" file has at least one row with a non-numeric value in the "CAPACIDAD (Kwh)" column
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": 'El archivo de capacidad de generación contiene al menos un registro con un valor no numérico en la columna "CAPACIDAD (Kwh)"',
            },
        )
        await sio.emit("stop_processing")
        return

    if (
        df_capacidad_generacion.select(pl.col("Fecha")).min().item()
        != df_capacidad_generacion.select(pl.col("Fecha")).max().item()
    ):
        # Emit a message to the client indicating that "capacidad de generación" file has at least one row with a date different from the others
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": "El archivo de capacidad de generación contiene al menos un registro con una fecha distinta a las demás",
            },
        )
        await sio.emit("stop_processing")
        return

    if (
        df_capacidad_generacion.select(
            pl.when(
                pl.col("Codigo").is_in(["ZPA2", "ZPA3", "ZPA4", "ZPA5", "GVIO", "QUI1"])
            )
            .then(1)
            .otherwise(None)
        )
        .count()
        .item()
        != df_capacidad_generacion.select(pl.col("Codigo")).count().item()
    ):
        # Emit a message to the client indicating that "capacidad de generación" file has at least one row with a "CODIGO" value different from the power generators of interest
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": 'El archivo de capacidad de generación contiene al menos un registro con un valor diferente a los generadores de interés en la columna "CODIGO"',
            },
        )
        await sio.emit("stop_processing")
        return

    print(df_capacidad_generacion)

    # Save the query date to be used in requests to SIMEM API
    query_date = df_capacidad_generacion.select(pl.col("Fecha")).min().item()

    # Emit a message to the client indicating that "capacidad de generación" file was loaded and transformed successfully
    await sio.emit(
        "events_messages",
        {
            "status": "success",
            "content": "El archivo de capacidad de generación fue cargado y transformado exitosamente",
        },
    )

    # EXTRACT 2
    # Get "Despacho programado recursos de generación" from SIMEM API, parse the data to JSON and extract the "records" key
    url_despacho_programado_SIMEM = f"https://www.simem.co/backend-files/api/PublicData?startdate={query_date}&enddate={query_date}&datasetId=ff027b"

    try:
        despacho_programado = requests.get(
            url=url_despacho_programado_SIMEM, timeout=(7, 4)
        )
    except requests.exceptions.Timeout:
        # Emit a message to the client indicating that the request to SIMEM API timed out
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": 'La solicitud del "Despacho programado recursos de generación" a la API del SIMEM excedió el tiempo de espera',
            },
        )
        await sio.emit("stop_processing")
        return
    except requests.exceptions.ConnectionError:
        # Emit a message to the client indicating that a connection error ocurred with SIMEM API
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": 'Error de conexión a la API del SIMEM al solicitar el "Despacho programado recursos de generación"',
            },
        )
        await sio.emit("stop_processing")
        return

    despacho_programado_json = despacho_programado.json()
    despacho_programado_json_records = despacho_programado_json["result"]["records"]

    # TRANSFORM 2
    # Define a polars' DataFrame with the records in JSON
    df_despacho_programado = pl.DataFrame(data=despacho_programado_json_records)
    print(df_despacho_programado)

    # Compute "Fecha" and "Hora" columns, and select "Valor" (renamed as "Capacidad") and "CodigoPlanta" (renamed as "Codigo") columns; casting "Valor" column to Int64 data type. Then, filter rows with "Codigo" values associated with the power generators of interest
    df_despacho_programado = df_despacho_programado.select(
        pl.col("FechaHora")
        .str.slice(offset=0, length=10)
        .str.to_date("%Y-%m-%d")
        .alias("Fecha"),
        pl.col("FechaHora").str.slice(offset=11, length=2).alias("Hora"),
        pl.col("Valor").cast(pl.Int64).alias("Compromiso (kWh)"),
        pl.col("CodigoPlanta").alias("Codigo"),
    ).filter(pl.col("Codigo").is_in(["ZPA2", "ZPA3", "ZPA4", "ZPA5", "GVIO", "QUI1"]))
    print(df_despacho_programado)

    # Emit a message to the client indicating that "Despacho programado recursos de generación" file was loaded and transformed successfully
    await sio.emit(
        "events_messages",
        {
            "status": "success",
            "content": 'El archivo "Despacho programado recursos de generación" fue cargado y transformado exitosamente',
        },
    )

    # EXTRACT 3
    # Get "Precio de bolsa ponderado" from SIMEM API, parse the data to JSON and extract the "records" key
    url_precio_bolsa_SIMEM = f"https://www.simem.co/backend-files/api/PublicData?startdate={query_date}&enddate={query_date}&datasetId=96D56E"

    try:
        precio_bolsa = requests.get(url=url_precio_bolsa_SIMEM, timeout=(7, 4))
    except requests.exceptions.Timeout:
        # Emit a message to the client indicating that the request to SIMEM API timed out
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": 'La solicitud del "Precio de bolsa ponderado" a la API del SIMEM excedió el tiempo de espera',
            },
        )
        await sio.emit("stop_processing")
        return
    except requests.exceptions.ConnectionError:
        # Emit a message to the client indicating that a connection error occurred with SIMEM API
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": 'Error de conexión a la API del SIMEM al solicitar el "Precio de bolsa ponderado"',
            },
        )
        await sio.emit("stop_processing")
        return

    precio_bolsa_json = precio_bolsa.json()
    precio_bolsa_json_records = precio_bolsa_json["result"]["records"]

    # TRANSFORM 3
    # Define a polars' DataFrame with the records in JSON
    df_precio_bolsa = pl.DataFrame(data=precio_bolsa_json_records)
    print(df_precio_bolsa)

    # Filter value with "CodigoVariable" equal to "PPBOGReal" and "Version" equal to "TXR"
    df_precio_bolsa = df_precio_bolsa.filter(
        (pl.col("CodigoVariable") == "PPBOGReal") & (pl.col("Version") == "TXR")
    ).select(pl.col("Valor").cast(pl.Float64).alias("Precio Bolsa (COP/kWh)"))
    print(df_precio_bolsa)

    # Emit a message to the client indicating that "Precio de bolsa ponderado" file was loaded and transformed successfully
    await sio.emit(
        "events_messages",
        {
            "status": "success",
            "content": 'El archivo "Precio de bolsa ponderado" fue cargado y transformado exitosamente',
        },
    )

    # TRANSFORM 4
    df_balance_energia = df_despacho_programado.join(
        other=df_capacidad_generacion, on=["Fecha", "Hora", "Codigo"], how="inner"
    )
    print(df_balance_energia)

    df_balance_energia_agrupado_fecha_codigo = (
        df_balance_energia.group_by(["Fecha", "Codigo"])
        .agg(
            pl.sum("Capacidad (kWh)").alias("Capacidad Total (kWh)"),
            pl.sum("Compromiso (kWh)").alias("Compromiso Total (kWh)"),
        )
        .sort("Codigo")
    )
    print(df_balance_energia_agrupado_fecha_codigo)

    df_balance_energia_consolidado = df_balance_energia_agrupado_fecha_codigo.select(
        pl.col("Fecha"),
        pl.col("Codigo"),
        (pl.col("Capacidad Total (kWh)") - pl.col("Compromiso Total (kWh)")).alias(
            "Balance (kWh)"
        ),
    )
    print(df_balance_energia_consolidado)

    df_balance_energia_consolidado_final = df_balance_energia_consolidado.with_columns(
        (pl.col("Balance (kWh)") * df_precio_bolsa)
        .cast(pl.Int64)
        .alias("Compromisos (COP)"),
        pl.when(pl.col("Balance (kWh)") > 0)
        .then(pl.lit("Vender"))
        .otherwise(pl.lit("Comprar"))
        .alias("Operacion"),
    )
    print(df_balance_energia_consolidado_final)

    # Emit a message to the client indicating that "balance de energía" report was calculated successfully
    await sio.emit(
        "events_messages",
        {
            "status": "success",
            "content": "El balance de energía fue calculado exitosamente",
        },
    )
    await sio.emit("stop_processing")

    balance_energia_json = df_balance_energia_consolidado_final.write_json()

    # LOAD
    # Emit "balance de energía" report to the client
    await sio.emit("energy_balance", balance_energia_json)

    # Save "balance de energía" report in a CSV file
    df_balance_energia_consolidado_final.write_csv("balance_energia.csv")

    # Save "balance de energía" report in a XLSX file
    df_balance_energia_consolidado_final.write_excel(
        f"balance_energia_{date.today()}.xlsx"
    )

    print(f"Finished ETL: SID {sid}")


@sio.event
async def send_report(sid):
    print(f"Sending report: SID {sid}")

    # UPLOAD CSV FILE TO FILEUPLOAD
    # FastUpload base URL
    url_fastupload = "https://api.fastupload.io/api/v2/"

    # FastUpload account API keys
    key_1 = os.getenv("KEY_1")
    key_2 = os.getenv("KEY_2")

    # FastUpload authentication
    url_fastupload_auth = f"{url_fastupload}authorize"

    try:
        credentials = requests.post(
            url=url_fastupload_auth, data={"key1": key_1, "key2": key_2}, timeout=(7, 4)
        )
    except requests.exceptions.Timeout:
        # Emit a message to the client indicating that the authentication to FastUpload API timed out
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": "La autenticación en FastUpload excedió el tiempo de espera",
            },
        )
        await sio.emit("stop_processing")
        return
    except requests.exceptions.ConnectionError:
        # Emit a message to the client indicating that a connection error occurred with FastUpload API
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": "Error de conexión a FastUpload durante la autenticación",
            },
        )
        await sio.emit("stop_processing")
        return

    credentials_json = credentials.json()

    # Emit a message to the client indicating that authentication failed
    if credentials_json["_status"] != "success":
        await sio.emit(
            "events_messages",
            {"status": "error", "content": "Error de autenticación en FastUpload"},
        )
        await sio.emit("stop_processing")
        return

    # FastUpload credentials
    access_token = credentials_json["data"]["access_token"]
    account_id = credentials_json["data"]["account_id"]

    # Open CSV file and return a file object
    csv_file = open("balance_energia.csv", "rb")

    # Upload CSV file to FastUpload
    url_fastupload_upload = f"{url_fastupload}file/upload"
    report_file = {"upload_file": (csv_file.name, csv_file)}
    data = {"access_token": access_token, "account_id": account_id}

    try:
        confirmation = requests.post(
            url=url_fastupload_upload, files=report_file, data=data
        )
    except requests.exceptions.Timeout:
        # Emit a message to the client indicating that the report upload to FastUpload API timed out
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": "La subida del informe a FastUpload excedió el tiempo de espera",
            },
        )
        await sio.emit("stop_processing")
        return
    except requests.exceptions.ConnectionError:
        # Emit a message to the client indicating that a connection error occurred with FastUpload API
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": "Error de conexión a FastUpload durante la subida del informe",
            },
        )
        await sio.emit("stop_processing")
        return

    confirmation_json = confirmation.json()

    # Close CSV file
    csv_file.close()

    # Emit a message to the client indicating that the report upload failed
    if confirmation_json["_status"] != "success":
        await sio.emit(
            "events_messages",
            {"status": "error", "content": "Error al subir el informe a FastUpload"},
        )
        await sio.emit("stop_processing")
        return

    # UPLOAD XLSX FILE TO GOOGLE DRIVE
    # Class with methods to interact with the required drive
    gdrive = gauth()

    # Save the content of the XLSX local file in a file to be uploaded
    xlsx_file = gdrive.CreateFile(
        {
            "parents": [
                {"kind": "drive#fileLink", "id": f"{os.getenv('UPLOAD_FOLDER_ID')}"}
            ]
        }
    )
    xlsx_file.SetContentFile(f"balance_energia_{date.today()}.xlsx")

    try:
        xlsx_file.Upload()
    except gdrive.ApiRequestError:
        # Emit a message to the client indicating that the report upload failed
        await sio.emit(
            "events_messages",
            {
                "status": "error",
                "content": "Error al guardar el informe en Google Drive",
            },
        )
        await sio.emit("stop_processing")
        return

    await sio.emit(
        "events_messages",
        {"status": "success", "content": "Informe enviado y guardado existosamente"},
    )
    await sio.emit("stop_processing")

    print(f"Report sent: SID {sid}")
