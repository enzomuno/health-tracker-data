import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# Define o escopo e autentica
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(r"C:\muno_environment\projects\health-tracker-data-pipeline\gscredentials.json", scope)
client = gspread.authorize(creds)

# Abre a planilha pelo pelo ID
spreadsheet = client.open_by_key("1-tPlTCF4xztJ0F33RdVVNuFZcaL_tDLYNYnN-AZyDM4")


# ==== Extrai todas as informações baseado no nome da aba.
def get_df_from_gsheet(sheet_name):
    worksheet = spreadsheet.worksheet(sheet_name)
    data = worksheet.get_all_records()
    return pd.DataFrame(data)
