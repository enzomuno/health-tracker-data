from src.extract.extract_fatsecret_api import get_weight_entries, get_food_entries
from src.db_config import get_db_connection
from psycopg2.extras import execute_values
from datetime import datetime, timezone


# Transforma JSON da função 'get_weight_entries' em registros
def extract_weight_entries(json_data):
    month = json_data.get("month", {})
    day = month.get("day", [])
    from_date_int = int(month.get("from_date_int", 0))
    to_date_int = int(month.get("to_date_int", 0))
    extracted_at = datetime.now(timezone.utc)

    return [
        {
            "date_int": int(i["date_int"]),
            "weight_kg": float(i["weight_kg"]),
            "weight_comment": i.get("weight_comment"),
            "from_date_int": from_date_int,
            "to_date_int": to_date_int,
            "extracted_at": extracted_at
        }
        for i in day
    ]


# Transforma JSON da função 'get_food_entries' em registros
def extract_food_entries(json_data):
    month = json_data.get("month", {})
    day = month.get("day", [])
    from_date_int = int(month.get("from_date_int", 0))
    to_date_int = int(month.get("to_date_int", 0))
    extracted_at = datetime.now(timezone.utc)

    return [
        {
            "date_int": int(i["date_int"]),
            "calories": int(i["calories"]),
            "carbohydrate": float(i["carbohydrate"]),
            "fat": float(i["fat"]),
            "protein": float(i["protein"]),
            "from_date_int": from_date_int,
            "to_date_int": to_date_int,
            "extracted_at": extracted_at
        }
        for i in day
    ]


# UPSERT no banco - Weight
def upsert_weight_entries(conn, entries):
    with conn.cursor() as cur:
        sql = """
        INSERT INTO raw.weight_entries (
            date_int, weight_kg, weight_comment,
            from_date_int, to_date_int, extracted_at
        ) VALUES %s
        ON CONFLICT (date_int) DO UPDATE SET
            weight_kg = EXCLUDED.weight_kg,
            weight_comment = EXCLUDED.weight_comment,
            from_date_int = EXCLUDED.from_date_int,
            to_date_int = EXCLUDED.to_date_int,
            extracted_at = EXCLUDED.extracted_at;
        """
        values = [(e["date_int"], e["weight_kg"], e["weight_comment"],
                   e["from_date_int"], e["to_date_int"], e["extracted_at"]) for e in entries]
        execute_values(cur, sql, values)
    conn.commit()


# UPSERT no banco - Foods
def upsert_food_entries(conn, entries):
    with conn.cursor() as cur:
        sql = """
        INSERT INTO raw.food_entries (
            date_int, calories, carbohydrate,
            fat, protein, from_date_int, to_date_int, extracted_at
        ) VALUES %s
        ON CONFLICT (date_int) DO UPDATE SET
            calories = EXCLUDED.calories,
            carbohydrate = EXCLUDED.carbohydrate,
            fat = EXCLUDED.fat,
            protein = EXCLUDED.protein,
            from_date_int = EXCLUDED.from_date_int,
            to_date_int = EXCLUDED.to_date_int,
            extracted_at = EXCLUDED.extracted_at;
        """
        values = [(f["date_int"], f["calories"], f["carbohydrate"], f["fat"],
                   f["protein"], f["from_date_int"], f["to_date_int"], f["extracted_at"]) for f in entries]
        execute_values(cur, sql, values)
    conn.commit()


# Realiza a ingestão completa dos dados
def run_ingest_fatsecret():
    conn = get_db_connection()

    # Peso
    print("🔁 Extraindo dados de PESO...")
    weight_json = get_weight_entries()
    weight_entries = extract_weight_entries(weight_json)
    print(f"💾 Peso - {len(weight_entries)} registros encontrados.")
    upsert_weight_entries(conn, weight_entries)

    # Alimentação
    print("\n🔁 Extraindo dados de ALIMENTAÇÃO...")
    food_json = get_food_entries()
    food_entries = extract_food_entries(food_json)
    print(f"💾 Alimentação - {len(food_entries)} registros encontrados.")
    upsert_food_entries(conn, food_entries)

    conn.close()
    print("\n✅ Pipeline finalizado com sucesso!")
