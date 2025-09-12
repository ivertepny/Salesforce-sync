from datetime import timedelta, timezone
from django.db import transaction
from django.utils import timezone as djtz

from ..models import Campaign, SyncCursor, PendingChange
from .google_ads_client import GoogleAds
from .mappers import campaign_row_to_dict

RESOURCE = "campaign"

def _get_cursor(resource: str, default_minutes: int = 1440):
    sc, _ = SyncCursor.objects.get_or_create(resource=resource)
    if not sc.cursor:
        sc.cursor = djtz.now() - timedelta(minutes=default_minutes)
        sc.save(update_fields=["cursor"])
    return sc.cursor

def _set_cursor(resource: str, new_cursor):
    SyncCursor.objects.update_or_create(
        resource=resource, defaults={"cursor": new_cursor}
    )

def pull_campaign_deltas() -> int:
    client = GoogleAds()
    since = _get_cursor(RESOURCE)

    gaql = f"""
        SELECT
          campaign.resource_name,
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.advertising_channel_type,
          campaign.start_date,
          campaign.end_date,
          campaign.last_modified_time
        FROM campaign
        WHERE campaign.last_modified_time >= '{since.astimezone(timezone.utc).isoformat()}'
        ORDER BY campaign.last_modified_time DESC
    """

    processed = 0
    latest_ts = since

    for row in client.search_stream(gaql):
        data = campaign_row_to_dict(row)
        with transaction.atomic():
            obj, created = Campaign.objects.update_or_create(
                resource_name=data["resource_name"],
                defaults={
                    **data,
                    "last_synced_at": djtz.now(),
                },
            )
        processed += 1
        if data.get("external_updated_at") and data["external_updated_at"] > latest_ts:
            latest_ts = data["external_updated_at"]

    if latest_ts > since:
        _set_cursor(RESOURCE, latest_ts)

    return processed

def push_campaign_changes() -> int:
    client = GoogleAds()
    changes = list(
        PendingChange.objects.select_for_update(skip_locked=True)
        .filter(resource=RESOURCE, status="pending")
        .order_by("created_at")[:200]
    )

    if not changes:
        return 0

    campaign_operation = client.client.get_type("CampaignOperation")
    ops = []
    for ch in changes:
        payload = ch.payload or {}
        if ch.action == "create":
            op = campaign_operation()
            c = op.create
            c.name = payload.get("name", "New Campaign")
            AdvertisingChannelTypeEnum = client.client.get_type("AdvertisingChannelTypeEnum")
            c.advertising_channel_type = payload.get("advertising_channel_type", AdvertisingChannelTypeEnum.SEARCH)
            ops.append(op)
        elif ch.action in ("update", "pause", "enable"):
            op = campaign_operation()
            c = op.update
            c.resource_name = payload["resource_name"]
            field_mask = client.client.get_type("FieldMask")
            CampaignStatusEnum = client.client.get_type("CampaignStatusEnum")
            if ch.action == "pause":
                c.status = CampaignStatusEnum.PAUSED
                field_mask.paths.append("status")
            elif ch.action == "enable":
                c.status = CampaignStatusEnum.ENABLED
                field_mask.paths.append("status")
            else:
                for key, value in payload.get("fields", {}).items():
                    setattr(c, key, value)
                    field_mask.paths.append(key)
            op.update_mask.CopyFrom(field_mask)
            ops.append(op)
        elif ch.action == "remove":
            op = campaign_operation()
            op.remove = payload["resource_name"]
            ops.append(op)

    if not ops:
        return 0

    client.mutate_campaigns(ops)
    ids = [c.id for c in changes]
    PendingChange.objects.filter(id__in=ids).update(status="done")
    return len(ops)
