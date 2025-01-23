"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import glob
import pandas as pd
import zipfile
import os


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de df_clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las cols indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    def charge_data(input_folder):
        def unzip(input_folder):
            archivos = glob.glob(os.path.join(input_folder, "*.zip"))
            for azip in archivos:
                with zipfile.ZipFile(azip, "r") as zip_ref:
                    for archivo_info in zip_ref.infolist():
                        with zip_ref.open(archivo_info) as archivo:
                            yield pd.read_csv(archivo)

        dataframes = [df for df in unzip(input_folder)]
        return pd.concat(dataframes, ignore_index=True)

    def clients(df):
        cols = [
            "client_id",
            "age",
            "job",
            "marital",
            "education",
            "credit_default",
            "mortgage",
        ]
        df_clientes = df[cols].copy()
        df_clientes["job"] = (
            df_clientes["job"]
            .str.replace(".", "", regex=False)
            .str.replace("-", "_", regex=False)
        )
        df_clientes["education"] = (
            df_clientes["education"]
            .str.replace(".", "_", regex=False)
            .replace("unknown", pd.NA)
        )
        df_clientes["credit_default"] = df_clientes["credit_default"].apply(
            lambda x: 1 if x == "yes" else 0
        )
        df_clientes["mortgage"] = df_clientes["mortgage"].apply(
            lambda x: 1 if x == "yes" else 0
        )
        return df_clientes

    def campaigns(df):
        cols = [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "last_contact_date",
        ]
        df_campañas = df.copy()
        meses = {
            "jan": 1,
            "feb": 2,
            "mar": 3,
            "apr": 4,
            "may": 5,
            "jun": 6,
            "jul": 7,
            "aug": 8,
            "sep": 9,
            "oct": 10,
            "nov": 11,
            "dec": 12,
        }
        df_campañas["month"] = df_campañas["month"].str.lower().map(meses)
        df_campañas["last_contact_date"] = pd.to_datetime(
            "2022-"
            + df_campañas["month"].astype(str).str.zfill(2)
            + "-"
            + df_campañas["day"].astype(str).str.zfill(2),
            format="%Y-%m-%d",
        )
        df_campañas["previous_outcome"] = df_campañas["previous_outcome"].apply(
            lambda x: 1 if x == "success" else 0
        )
        df_campañas["campaign_outcome"] = df_campañas["campaign_outcome"].apply(
            lambda x: 1 if x == "yes" else 0
        )
        return df_campañas[cols]

    def economy(df):
        cols = ["client_id", "cons_price_idx", "euribor_three_months"]
        df_economia = df[cols].copy()
        return df_economia

    def save_data(df_clientes, df_campañas, economia, carpeta_salida):
        if not os.path.exists(carpeta_salida):
            os.makedirs(carpeta_salida)
        df_clientes.to_csv(os.path.join(carpeta_salida, "client.csv"), index=False)
        df_campañas.to_csv(os.path.join(carpeta_salida, "campaign.csv"), index=False)
        economia.to_csv(os.path.join(carpeta_salida, "economics.csv"), index=False)

    # Cargar datos
    df = charge_data("files/input")

    # Procesar datos
    df_clientes = clients(df)
    df_campañas = campaigns(df)
    economia = economy(df)

    # Guardar datos
    print("Guardando los archivos procesados en la carpeta de salida...")
    save_data(df_clientes, df_campañas, economia, "files/output")


clean_campaign_data()
