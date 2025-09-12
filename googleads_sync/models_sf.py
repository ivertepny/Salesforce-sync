from django.db import models
from django.utils import timezone

class SalesforceEvent(models.Model):
    object_name = models.CharField(max_length=128)  # topic or object name
    sf_id = models.CharField(max_length=32)
    payload = models.JSONField(default=dict)
    received_at = models.DateTimeField(default=timezone.now)

    class Meta:
        indexes = [
            models.Index(fields=["object_name", "received_at"]),
        ]
