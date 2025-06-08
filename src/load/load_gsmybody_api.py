from datetime import datetime, timezone
from src.extract.extract_gsmybody_api import get_df_from_gsheet
from src.db_config import get_db_connection
from psycopg2.extras import execute_values
import pandas as pd

# Função para extrair os dados vindo da função 'get_df_from_gsheet'
def extract_tb_users():
    df = get_df_from_gsheet("tb_users")
    
    # Adiciona timestamp de extração
    df["extracted_at"] = datetime.now(timezone.utc)

    
    # Converte para dicionário (para usar no upsert)
    return df.to_dict(orient="records")

# Função para ingestão dos dados vindo da função 'extract_tb_users'
def upsert_users_entries(conn, users):
    with conn.cursor() as cur:
        sql = """
        INSERT INTO raw.tb_users (
            id_user, name, birth_date,
            height_cm, sex, extracted_at
        ) VALUES %s
        ON CONFLICT (id_user) DO UPDATE SET
            name = EXCLUDED.name,
            birth_date = EXCLUDED.birth_date,
            height_cm = EXCLUDED.height_cm,
            sex = EXCLUDED.sex,
            extracted_at = EXCLUDED.extracted_at;
        """
        values = [
            (
                u["id_user"], u["name"], u["birth_date"],
                u["height_cm"], u["sex"], u["extracted_at"]
            )
            for u in users
        ]
        execute_values(cur, sql, values)
    conn.commit()

# 1. Extração do Google Sheets
def extract_tb_body_stats():
    df = get_df_from_gsheet("tb_body_stats")
    
    # Adiciona timestamp de extração
    df["extracted_at"] = datetime.now(timezone.utc)
    
    return df.to_dict(orient="records")

# 2. Upsert no banco de dados
def upsert_body_stats_entries(conn, stats):
    with conn.cursor() as cur:
        sql = """
        INSERT INTO raw.tb_body_stats (
            id_stats, id_user, date,
            chest_cm, waist_cm, hips_cm,
            arm_right_cm, arm_left_cm, thigh_cm,
            extracted_at
        ) VALUES %s
        ON CONFLICT (id_stats) DO UPDATE SET
            id_user = EXCLUDED.id_user,
            date = EXCLUDED.date,
            chest_cm = EXCLUDED.chest_cm,
            waist_cm = EXCLUDED.waist_cm,
            hips_cm = EXCLUDED.hips_cm,
            arm_right_cm = EXCLUDED.arm_right_cm,
            arm_left_cm = EXCLUDED.arm_left_cm,
            thigh_cm = EXCLUDED.thigh_cm,
            extracted_at = EXCLUDED.extracted_at;
        """
        values = [
            (
                s["id_stats"], s["id_user"], s["date"],
                s["chest_cm"], s["waist_cm"], s["hips_cm"],
                s["arm_right_cm"], s["arm_left_cm"], s["thigh_cm"],
                s["extracted_at"]
            )
            for s in stats
        ]
        execute_values(cur, sql, values)
    conn.commit()

# 1. Função de extração da aba tb_exercises
def extract_tb_exercises():
    df = get_df_from_gsheet("tb_exercises")

    # Converte para datetime (se necessário)
    df["created_at"] = pd.to_datetime(df["created_at"], dayfirst=True, errors="coerce")

    # Timestamp da extração
    df["extracted_at"] = datetime.now(timezone.utc)

    return df.to_dict(orient="records")

# 2. Função de upsert no banco
def upsert_exercises_entries(conn, exercises):
    with conn.cursor() as cur:
        sql = """
        INSERT INTO raw.tb_exercises (
            id_exercises, name, created_at, extracted_at
        ) VALUES %s
        ON CONFLICT (id_exercises) DO UPDATE SET
            name = EXCLUDED.name,
            created_at = EXCLUDED.created_at,
            extracted_at = EXCLUDED.extracted_at;
        """
        values = [
            (
                e["id_exercises"], e["name"],
                e["created_at"], e["extracted_at"]
            )
            for e in exercises
        ]
        execute_values(cur, sql, values)
    conn.commit()

# 1. Extração do Google Sheets
def extract_tb_workout():
    df = get_df_from_gsheet("tb_workout")

    # Converte a coluna de data (se necessário)
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    
    # Adiciona timestamp da extração
    df["extracted_at"] = datetime.now(timezone.utc)

    return df.to_dict(orient="records")

# 2. Upsert no banco Neon
def upsert_workout_entries(conn, workouts):
    with conn.cursor() as cur:
        sql = """
        INSERT INTO raw.tb_workout (
            id_workout, id_dateworkout, date,
            id_user, id_exercises, kg, extracted_at
        ) VALUES %s
        ON CONFLICT (id_workout) DO UPDATE SET
            id_dateworkout = EXCLUDED.id_dateworkout,
            date = EXCLUDED.date,
            id_user = EXCLUDED.id_user,
            id_exercises = EXCLUDED.id_exercises,
            kg = EXCLUDED.kg,
            extracted_at = EXCLUDED.extracted_at;
        """
        values = [
            (
                w["id_workout"], w["id_dateworkout"], w["date"],
                w["id_user"], w["id_exercises"], w["kg"], w["extracted_at"]
            )
            for w in workouts
        ]
        execute_values(cur, sql, values)
    conn.commit()


def load_all_raw_tables():
    conn = get_db_connection()

    try:
        print("Iniciando ingestão de tb_users...")
        users = extract_tb_users()
        upsert_users_entries(conn, users)
        print("tb_users OK.")

        print("Iniciando ingestão de tb_body_stats...")
        body_stats = extract_tb_body_stats()
        upsert_body_stats_entries(conn, body_stats)
        print("tb_body_stats OK.")

        print("Iniciando ingestão de tb_exercises...")
        exercises = extract_tb_exercises()
        upsert_exercises_entries(conn, exercises)
        print("tb_exercises OK.")

        print("Iniciando ingestão de tb_workout...")
        workouts = extract_tb_workout()
        upsert_workout_entries(conn, workouts)
        print("tb_workout OK.")

        print("✅ Ingestão de todas as tabelas RAW concluída com sucesso.")

    except Exception as e:
        print("❌ Erro durante a execução do pipeline:", e)

    finally:
        conn.close()