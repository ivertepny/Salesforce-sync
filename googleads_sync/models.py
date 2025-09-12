from django.db import models
from django.utils import timezone


class Timestamped(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Campaign(Timestamped):
    resource_name = models.CharField(max_length=255, unique=True)
    campaign_id = models.BigIntegerField(unique=True)
    name = models.CharField(max_length=255)
    status = models.CharField(max_length=64)
    advertising_channel_type = models.CharField(max_length=64, blank=True, null=True)
    budget_micros = models.BigIntegerField(blank=True, null=True)
    start_date = models.DateField(blank=True, null=True)
    end_date = models.DateField(blank=True, null=True)

    external_updated_at = models.DateTimeField(blank=True, null=True)
    last_synced_at = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return f"[{self.campaign_id}] {self.name}"


class SyncCursor(models.Model):
    resource = models.CharField(max_length=64, unique=True)
    cursor = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return f"{self.resource} @ {self.cursor.isoformat()}"


class PendingChange(Timestamped):
    ACTIONS = (
        ("create", "Create"),
        ("update", "Update"),
        ("remove", "Remove"),
        ("pause", "Pause"),
        ("enable", "Enable"),
    )
    RESOURCES = (
        ("campaign", "Campaign"),
    )

    resource = models.CharField(max_length=32, choices=RESOURCES)
    action = models.CharField(max_length=16, choices=ACTIONS)
    payload = models.JSONField(default=dict)
    status = models.CharField(max_length=16, default="pending")
    error = models.TextField(blank=True, null=True)

    class Meta:
        indexes = [
            models.Index(fields=["resource", "status", "created_at"]),
        ]


class SalesforceEvent(models.Model):
    object_name = models.CharField(max_length=128)  # topic or object name
    sf_id = models.CharField(max_length=32)
    payload = models.JSONField(default=dict)
    received_at = models.DateTimeField(default=timezone.now)

    class Meta:
        indexes = [
            models.Index(fields=["object_name", "received_at"]),
        ]
