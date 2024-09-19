from Utils.logger import get_logger

logger = get_logger(__name__)

# TODO : Put this in a seperate utils file (def load_ingest_config(table))
def load_table_config(table_name):
    config_path = f"/Config/ingest_config/{table_name}.yml"
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def ingest_to_bronze(table_config):
    logger.info(f"Starting ingestion for {table_config['table_name']}")
    # Ingestion logic here
    logger.info(f"Completed ingestion for {table_config['table_name']}")

table_config = load_table_config("table_1")