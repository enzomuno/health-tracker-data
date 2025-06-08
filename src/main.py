from src.load.load_gsmybody_api import load_all_raw_tables
from src.load.load_fatsecret_api import run_ingest_fatsecret

if __name__ == "__main__":
    #load_all_raw_tables()
    run_ingest_fatsecret()
