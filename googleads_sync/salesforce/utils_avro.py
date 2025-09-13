# googleads_sync/salesforce/utils_avro.py
from fastavro import schemaless_reader
from io import BytesIO

# TODO: Провідник схем: отримаєш schema_id з події → завантажиш writer schema з SF (або кеш/локально)
# Тимчасово читаємо як RAW або з відомою схемою.
WRITER_SCHEMA = {"type": "record", "name": "Dummy", "fields": []}

def avro_decode(payload: bytes, schema: dict | None = None) -> dict:
    if not payload:
        return {}
    bio = BytesIO(payload)
    return schemaless_reader(bio, (schema or WRITER_SCHEMA))
