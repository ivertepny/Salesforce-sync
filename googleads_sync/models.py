# googleads_sync/models.py

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
        ("lead", "Lead"),
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


# ── NEW: персистенція replay_id для надійного resume ─────────────────────────

class ReplayState(models.Model):
    """Зберігає останній успішний replay_id на кожний Pub/Sub topic."""
    topic_name = models.CharField(max_length=255, unique=True)
    replay_id = models.BinaryField(null=True, blank=True)
    replay_id_hex = models.CharField(max_length=1024, blank=True, default="")
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "sf_replay_state"
        indexes = [
            models.Index(fields=["topic_name"]),
        ]

    def __str__(self):
        return f"{self.topic_name} @ {self.replay_id_hex[:16]}..."

    def set_replay(self, rid: bytes | None):
        self.replay_id = rid
        self.replay_id_hex = rid.hex() if rid else ""
        self.save(update_fields=["replay_id", "replay_id_hex", "updated_at"])


# ── NEW: snapshot-моделі для GA entity (наповнюй із Celery chain) ────────────

class BaseSnapshot(models.Model):
    external_id = models.CharField(max_length=128, db_index=True)  # resource name/ID
    source = models.CharField(
        max_length=32,
        choices=(("google_ads", "Google Ads"), ("salesforce", "Salesforce")),
        default="google_ads",
    )
    name = models.CharField(max_length=255, blank=True, default="")
    status = models.CharField(max_length=64, blank=True, default="")
    raw_payload = models.JSONField(default=dict, blank=True)
    snapshot_at = models.DateTimeField(default=timezone.now)

    class Meta:
        abstract = True


class GoogleAdsCampaignSnapshot(BaseSnapshot):
    campaign_budget_micros = models.BigIntegerField(null=True, blank=True)
    channel_type = models.CharField(max_length=64, blank=True, default="")

    class Meta:
        db_table = "ga_campaign_snapshot"
        indexes = [models.Index(fields=["external_id", "snapshot_at"])]


class GoogleAdsAdGroupSnapshot(BaseSnapshot):
    campaign_external_id = models.CharField(max_length=128, db_index=True)
    type = models.CharField(max_length=64, blank=True, default="")

    class Meta:
        db_table = "ga_adgroup_snapshot"
        indexes = [models.Index(fields=["external_id", "snapshot_at"])]


class GoogleAdsAdSnapshot(BaseSnapshot):
    ad_group_external_id = models.CharField(max_length=128, db_index=True)
    ad_type = models.CharField(max_length=64, blank=True, default="")

    class Meta:
        db_table = "ga_ad_snapshot"
        indexes = [models.Index(fields=["external_id", "snapshot_at"])]


# --- ADD: відповідності між SF і GA ---
class ExternalIdMap(Timestamped):
    KIND_CHOICES = (
        ("campaign", "Campaign"),
        ("lead", "Lead"),
    )
    kind = models.CharField(max_length=32, choices=KIND_CHOICES)
    sf_id = models.CharField(max_length=64, blank=True, null=True, db_index=True)
    ga_resource = models.CharField(max_length=255, blank=True, null=True, db_index=True)

    class Meta:
        unique_together = (("kind", "sf_id"), ("kind", "ga_resource"))
        indexes = [
            models.Index(fields=["kind", "sf_id"]),
            models.Index(fields=["kind", "ga_resource"]),
        ]

    def __str__(self):
        return f"{self.kind}: SF={self.sf_id} <-> GA={self.ga_resource}"


# --- ADD: модель Lead для локальних snapshot’ів/стану ---
class Lead(Timestamped):
    # мінімальний кістяк; додати поля під кейс замовника !!!!!!!!TODO
    sf_id = models.CharField(max_length=64, unique=True, null=True, blank=True)
    ga_click_id = models.CharField(max_length=255, null=True, blank=True)  # gclid/gbraid/wbraid тощо
    ga_lead_resource = models.CharField(max_length=255, null=True, blank=True)

    status = models.CharField(max_length=64, blank=True, default="")
    email_sha256 = models.CharField(max_length=128, blank=True, default="")  # якщо будеш вантажити оффлайн-конверсії
    phone_sha256 = models.CharField(max_length=128, blank=True, default="")

    external_updated_at = models.DateTimeField(blank=True, null=True)
    last_synced_at = models.DateTimeField(default=timezone.now)

    class Meta:
        indexes = [
            models.Index(fields=["sf_id"]),
            models.Index(fields=["ga_click_id"]),
            models.Index(fields=["ga_lead_resource"]),
        ]

    def __str__(self):
        return self.sf_id or self.ga_lead_resource or "lead"