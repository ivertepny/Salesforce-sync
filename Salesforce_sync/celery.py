import os
from celery import Celery
from django.conf import settings

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Salesforce_sync.settings")

celery_app = Celery("Salesforce_sync")
celery_app.config_from_object("django.conf:settings", namespace="CELERY")
celery_app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
