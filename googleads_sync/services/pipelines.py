# googleads_sync/services/pipelines.py
from datetime import timedelta, timezone
from django.db import transaction
from django.utils import timezone as djtz

from .sf_bridge import publish_sf_platform_event
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
          campaign.end_date
        FROM campaign
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


def push_campaign_changes(batch_size: int = 200) -> int:
    """
    Обробляє PendingChange(resource='campaign', status='pending') у батчах.
    - Вибірка із select_for_update(skip_locked=True) лише в межах transaction.atomic()
    - Спочатку позначаємо записи як 'processing' і відпускаємо транзакцію,
      щоб зовнішній виклик до Google Ads не тримав блокування.
    - Після виклику позначаємо 'done' або 'error'.
    """
    client = GoogleAds()
    processed = 0

    while True:
        # 1) Вибрати батч і помітити 'processing' ПІД замком
        with transaction.atomic():
            to_process = list(
                PendingChange.objects.filter(resource=RESOURCE, status="pending")
                .order_by("created_at")
                .select_for_update(skip_locked=True)[:batch_size]
            )
            if not to_process:
                break
            ids = [c.id for c in to_process]
            PendingChange.objects.filter(id__in=ids).update(status="processing")

        # 2) Зібрати операції ПОЗА транзакцією
        campaign_operation = client.client.get_type("CampaignOperation")
        ops = []
        id_map = []  # збережемо відповідність для логування/помилок
        for ch in to_process:
            payload = ch.payload or {}
            try:
                if ch.action == "create":
                    op = campaign_operation()
                    c = op.create
                    c.name = payload.get("name", "New Campaign")
                    AdvertisingChannelTypeEnum = client.client.get_type("AdvertisingChannelTypeEnum")
                    c.advertising_channel_type = payload.get(
                        "advertising_channel_type",
                        AdvertisingChannelTypeEnum.SEARCH,
                    )
                    ops.append(op)
                    id_map.append(ch.id)

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
                        for key, value in (payload.get("fields") or {}).items():
                            setattr(c, key, value)
                            field_mask.paths.append(key)
                    op.update_mask.CopyFrom(field_mask)
                    ops.append(op)
                    id_map.append(ch.id)

                elif ch.action == "remove":
                    op = campaign_operation()
                    op.remove = payload["resource_name"]
                    ops.append(op)
                    id_map.append(ch.id)

                else:
                    # Невідома дія — позначимо error
                    with transaction.atomic():
                        PendingChange.objects.filter(id=ch.id).update(
                            status="error",
                            error=f"Unsupported action: {ch.action}",
                        )

            except Exception as e:
                # Проблема підготовки операції — позначаємо error для конкретного запису
                with transaction.atomic():
                    PendingChange.objects.filter(id=ch.id).update(
                        status="error",
                        error=str(e)[:1000],
                    )

        if not ops:
            # Усі пішли в error або нічого робити
            continue

        # 3) Виконати мутацію ПОЗА транзакцією
        try:
            client.mutate_campaigns(ops)
            # 4a) Успіх — помітити done
            with transaction.atomic():
                PendingChange.objects.filter(id__in=id_map).update(status="done", error="")
            processed += len(ops)
        except Exception as e:
            # 4b) Фейл — помітити error з повідомленням
            err = str(e)[:1000]
            with transaction.atomic():
                PendingChange.objects.filter(id__in=id_map).update(status="error", error=err)

    return processed


# --- ADD: SF -> GA (Lead) ---
def push_lead_changes(batch_size: int = 200) -> int:
    """
    Обробляє PendingChange(resource='lead') і виконує дію на стороні Google Ads.
    Тут два типові варіанти:
      1) Upload offline conversions (за gclid, email/phone sha256) → ConversionUploadService
      2) Customer Match (user lists)
    Нижче — каркас з транзакціями, TODO там де треба підключити конкретний сервіс.
    """
    from django.db import transaction
    processed = 0
    client = GoogleAds()

    while True:
        with transaction.atomic():
            to_process = list(
                PendingChange.objects.filter(resource="lead", status="pending")
                .order_by("created_at")
                .select_for_update(skip_locked=True)[:batch_size]
            )
            if not to_process:
                break
            ids = [c.id for c in to_process]
            PendingChange.objects.filter(id__in=ids).update(status="processing")

        # TODO: зібрати операції для ConversionUploadService або іншого сервісу
        ok_ids, err = [], None
        try:
            # Приклад заглушки: успішно обробили
            ok_ids = ids
        except Exception as e:
            err = str(e)[:1000]

        with transaction.atomic():
            if ok_ids:
                PendingChange.objects.filter(id__in=ok_ids).update(status="done", error="")
                processed += len(ok_ids)
            if err:
                PendingChange.objects.filter(id__in=ids).exclude(id__in=ok_ids).update(status="error", error=err)

    return processed


# --- ADD: GA -> SF (через Platform Event) ---
def pull_lead_deltas(topic: str = "/event/GA_Lead_Upsert__e") -> int:
    """
    Google Ads -> Salesforce без REST: публікуємо Platform Event, який у SF (Flow/Apex)
    створює/оновлює Lead. Залежить від наявності відповідної PE-схеми у SF.
    Використання: викликати з pipeline (напр., коли з GA отримані нові ліди/сигнали).
    """


    # TODO: дістань нові або змінені leads з GA (Lead Form або свій механізм).
    # Нижче — демо-запис, щоб показати публікацію події.
    demo_payload = {
        # ПРИКЛАД ПОЛІВ — повинен відповідати Avro-схемі події GA_Lead_Upsert__e
        "ExternalId__c": "ext-123",
        "Email__c": "user@example.com",
        "Phone__c": "+10000000000",
        "Status__c": "New",
    }
    try:
        publish_sf_platform_event(topic, demo_payload)
        return 1
    except Exception:
        return 0
