import os
import psycopg2
from dotenv import load_dotenv

# Carrega as variáveis do .env
load_dotenv()

def get_db_connection():

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("Variável de ambiente DATABASE_URL não encontrada.")
    
    conn = psycopg2.connect(db_url)
    return conn
