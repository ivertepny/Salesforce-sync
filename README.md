Salesforce-sync/
├─ manage.py
├─ Dockerfile
├─ docker-compose.yml
├─ requirements.txt
├─ .env.example
│
├─ salesforce_sync/                 # Django project package (налаштування)
│  ├─ __init__.py
│  ├─ settings.py                   # + INSTALLED_APPS, CELERY_BEAT, SF_PUBSUB_TOPICS
│  ├─ urls.py
│  ├─ asgi.py
│  ├─ wsgi.py
│  └─ celery.py                     # конфіг Celery (broker, beat, autodiscover_tasks)
│
├─ googleads_sync/                  # вже існуючий додаток (з твоїх логів)
│  ├─ __init__.py
│  ├─ apps.py
│  ├─ tasks.py                      # pipeline/chain для Google Ads (можеш залишити як є)
│  ├─ models.py                     # якщо є свої моделі
│  ├─ admin.py
│  └─ services/                     # клієнти/адаптери Google Ads
│     └─ __init__.py
│
├─ salesforce_pubsub/               # НОВИЙ додаток (CDC/Platform Events через Pub/Sub)
│  ├─ __init__.py
│  ├─ apps.py
│  ├─ admin.py
│  ├─ models.py                     # ReplayState, SyncCursor, *Snapshot моделі
│  ├─ tasks.py                      # Celery chain для snapshotів + обробка CDC→GA
│  ├─ migrations/
│  │  └─ 0001_initial.py
│  ├─ services/
│  │  ├─ __init__.py
│  │  ├─ pubsub_client.py           # gRPC клієнт з метаданими accesstoken/instanceurl/tenantid
│  │  ├─ avro_utils.py              # fastavro encode/decode
│  │  └─ pubsub_pb2_stubs.py        # тимчасовий shim; замінити на згенеровані gRPC stubs
│  ├─ management/
│  │  └─ commands/
│  │     └─ run_pubsub.py           # менеджмент-команда для підписки на топіки
│  └─ README_INTEGRATION.md
│
├─ scripts/
│  └─ generate_sf_stubs.sh          # (опц.) команда генерації gRPC-stubs із proto
│
└─ tests/
   ├─ __init__.py
   └─ test_pubsub_resume.py         # (опц.) тести на відновлення по replay_id
