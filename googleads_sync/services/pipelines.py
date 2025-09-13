from datetime import timedelta
import os
import re
import hashlib
from typing import List, Tuple, Dict, Any, Optional

from django.conf import settings
from django.db import transaction
from django.utils import timezone as djtz

from .sf_bridge import publish_sf_platform_event
from ..models import Campaign, SyncCursor, PendingChange
from .google_ads_client import GoogleAds
from .mappers import campaign_row_to_dict

RESOURCE = "campaign"

# ---- Config helpers ---------------------------------------------------------

def _getenv(name: str, default: Optional[str] = None) -> Optional[str]:
    if hasattr(settings, name):
        return getattr(settings, name)
    return os.getenv(name, default)

GA_CUSTOMER_ID = _getenv("GA_CUSTOMER_ID")  # e.g. "1234567890"
GA_CONVERSION_ACTION = _getenv("GA_CONVERSION_ACTION")  # e.g. "customers/1234567890/conversionActions/111"
GA_DEFAULT_CURRENCY = _getenv("GA_DEFAULT_CURRENCY", "USD")
GA_CM_USER_LIST = _getenv("GA_CM_USER_LIST")  # e.g. "customers/1234567890/userLists/222"

# ---- Cursor utils -----------------------------------------------------------

def _get_cursor(resource: str, default_minutes: int = 1440):
    sc, _ = SyncCursor.objects.get_or_create(resource=resource)
    if not sc.cursor:
        sc.cursor = djtz.now() - timedelta(minutes=default_minutes)
        sc.save(update_fields=["cursor"])
    return sc.cursor

def _set_cursor(resource: str, new_cursor):
    SyncCursor.objects.update_or_create(resource=resource, defaults={"cursor": new_cursor})

# ---- Campaign snapshots (GA -> local) --------------------------------------

def pull_campaign_deltas() -> int:
    client = GoogleAds()
    since = _get_cursor(RESOURCE)

    gaql = """
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
            Campaign.objects.update_or_create(
                resource_name=data["resource_name"],
                defaults={**data, "last_synced_at": djtz.now()},
            )
        processed += 1
        if data.get("external_updated_at") and data["external_updated_at"] > latest_ts:
            latest_ts = data["external_updated_at"]

    if latest_ts > since:
        _set_cursor(RESOURCE, latest_ts)

    return processed

# ---- Campaign mutations (SF -> GA) -----------------------------------------

def push_campaign_changes(batch_size: int = 200) -> int:
    """
    Processes PendingChange(resource='campaign', status='pending') in batches.
    Uses select_for_update(skip_locked=True) under transaction.atomic().
    """
    client = GoogleAds()
    processed = 0

    while True:
        # 1) Pull batch and mark 'processing'
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

        # 2) Build operations OUTSIDE transaction
        campaign_operation = client.client.get_type("CampaignOperation")
        ops = []
        id_map = []
        for ch in to_process:
            payload = ch.payload or {}
            try:
                if ch.action == "create":
                    op = campaign_operation()
                    c = op.create
                    c.name = payload.get("name", "New Campaign")
                    AdvertisingChannelTypeEnum = client.client.get_type("AdvertisingChannelTypeEnum")
                    c.advertising_channel_type = payload.get(
                        "advertising_channel_type", AdvertisingChannelTypeEnum.SEARCH
                    )
                    ops.append(op); id_map.append(ch.id)

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
                    ops.append(op); id_map.append(ch.id)

                elif ch.action == "remove":
                    op = campaign_operation()
                    op.remove = payload["resource_name"]
                    ops.append(op); id_map.append(ch.id)

                else:
                    with transaction.atomic():
                        PendingChange.objects.filter(id=ch.id).update(
                            status="error", error=f"Unsupported action: {ch.action}"
                        )

            except Exception as e:
                with transaction.atomic():
                    PendingChange.objects.filter(id=ch.id).update(
                        status="error", error=str(e)[:1000]
                    )

        if not ops:
            continue

        # 3) Execute mutation OUTSIDE transaction
        try:
            client.mutate_campaigns(ops)
            with transaction.atomic():
                PendingChange.objects.filter(id__in=id_map).update(status="done", error="")
            processed += len(ops)
        except Exception as e:
            err = str(e)[:1000]
            with transaction.atomic():
                PendingChange.objects.filter(id__in=id_map).update(status="error", error=err)

    return processed

# =============================================================================
#                              L  E  A  D  S
# =============================================================================
# SF -> GA : upload offline conversions OR customer match
# GA -> SF : publish Platform Events built from GA lead forms
# =============================================================================

# ---- Helpers for hashing & normalization -----------------------------------

_email_re = re.compile(r"\s+")
_phone_re = re.compile(r"[^\d]")

def _sha256_lower(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _norm_email(email: str) -> str:
    return _email_re.sub("", (email or "").strip().lower())

def _norm_phone(phone: str) -> str:
    return _phone_re.sub("", (phone or ""))

# ---- SF -> GA (Lead): Upload Click Conversions / Customer Match -------------

def _build_click_conversions(client: GoogleAds, items: List[PendingChange]) -> Tuple[list, list]:
    """Return (click_conversions, ids) from PendingChange payloads that have gclid/gbraid/wbraid."""
    ClickConversion = client.client.get_type("ClickConversion")
    click_conversions = []
    ids = []
    for ch in items:
        p = ch.payload or {}
        gclid = p.get("gclid")
        gbraid = p.get("gbraid")
        wbraid = p.get("wbraid")
        conv_time = p.get("conversion_time")  # RFC3339, e.g. "2025-09-13T10:00:00Z"
        conv_value = float(p.get("conversion_value", 0.0))
        currency = p.get("currency_code", GA_DEFAULT_CURRENCY)

        if (gclid or gbraid or wbraid) and conv_time and GA_CONVERSION_ACTION:
            cc = ClickConversion()
            if gclid: cc.gclid = gclid
            if gbraid: cc.gbraid = gbraid
            if wbraid: cc.wbraid = wbraid
            cc.conversion_action = GA_CONVERSION_ACTION
            cc.conversion_date_time = conv_time
            cc.conversion_value = conv_value
            cc.currency_code = currency
            order_id = p.get("order_id") or p.get("external_id")
            if order_id:
                cc.order_id = str(order_id)
            click_conversions.append(cc)
            ids.append(ch.id)
    return click_conversions, ids

def _build_user_data_ops(client: GoogleAds, items: List[PendingChange]) -> Tuple[list, list]:
    """
    Build UserDataOperations for Customer Match if we have hashed identifiers.
    Requires GA_CM_USER_LIST (resource name).
    """
    if not GA_CM_USER_LIST:
        return [], []
    UserIdentifier = client.client.get_type("UserIdentifier")
    UserData = client.client.get_type("UserData")
    UserDataOperation = client.client.get_type("UserDataOperation")

    ops = []
    ids = []
    for ch in items:
        p = ch.payload or {}
        email = p.get("email") or p.get("Email") or p.get("Email__c")
        phone = p.get("phone") or p.get("Phone") or p.get("Phone__c")
        email_h = p.get("email_sha256") or ( _sha256_lower(_norm_email(email)) if email else None )
        phone_h = p.get("phone_sha256") or ( _sha256_lower(_norm_phone(phone)) if phone else None )

        identifiers = []
        if email_h:
            ui = UserIdentifier()
            ui.hashed_email = email_h
            identifiers.append(ui)
        if phone_h:
            ui = UserIdentifier()
            ui.hashed_phone_number = phone_h
            identifiers.append(ui)

        if not identifiers:
            continue

        ud = UserData()
        ud.user_identifiers.extend(identifiers)

        op = UserDataOperation()
        op.create.CopyFrom(ud)
        ops.append(op)
        ids.append(ch.id)

    return ops, ids

def push_lead_changes(batch_size: int = 200) -> int:
    """
    Processes PendingChange(resource='lead'):
      1) If gclid/gbraid/wbraid present => Upload Click Conversions
      2) Else if (email/phone present) => Customer Match to GA_CM_USER_LIST
    """
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

        # ---- 1) Upload Click Conversions
        click_convs, click_ids = _build_click_conversions(client, to_process)
        if click_convs and GA_CUSTOMER_ID:
            try:
                service = client.client.get_service("ConversionUploadService")
                req = client.client.get_type("UploadClickConversionsRequest")
                req.customer_id = GA_CUSTOMER_ID
                req.conversions.extend(click_convs)
                # partial_failure=True allows per-row errors without failing the whole request
                req.partial_failure = True
                resp = service.UploadClickConversions(request=req)
                # Mark successes; on partial failures, mark specific items as error
                ok = []
                err_map = {}
                for i, res in enumerate(resp.results):
                    # results aligns with input order
                    if getattr(res, "gclid", "") or getattr(res, "gbraid", "") or getattr(res, "wbraid", ""):
                        ok.append(click_ids[i])
                if resp.partial_failure_error and resp.partial_failure_error.message:
                    err_msg = resp.partial_failure_error.message
                    # Conservatively mark all non-ok as error
                    bad = set(click_ids) - set(ok)
                    with transaction.atomic():
                        if ok:
                            PendingChange.objects.filter(id__in=ok).update(status="done", error="")
                        if bad:
                            PendingChange.objects.filter(id__in=list(bad)).update(status="error", error=err_msg[:1000])
                    processed += len(ok)
                else:
                    with transaction.atomic():
                        PendingChange.objects.filter(id__in=click_ids).update(status="done", error="")
                    processed += len(click_ids)
            except Exception as e:
                with transaction.atomic():
                    PendingChange.objects.filter(id__in=click_ids).update(status="error", error=str(e)[:1000])

        # ---- 2) Customer Match (only those not already done/error)
        remaining = list(
            PendingChange.objects.filter(resource="lead", status="processing", id__in=ids)
            .order_by("created_at")
        )
        cm_ops, cm_ids = _build_user_data_ops(client, remaining)
        if cm_ops and GA_CUSTOMER_ID and GA_CM_USER_LIST:
            try:
                svc = client.client.get_service("UserDataService")
                req = client.client.get_type("UploadUserDataRequest")
                req.customer_id = GA_CUSTOMER_ID
                req.operations.extend(cm_ops)
                req.customer_match_user_list_metadata.user_list = GA_CM_USER_LIST
                req.partial_failure = True
                resp = svc.UploadUserData(request=req)

                ok = cm_ids[:]  # if no partial failures, all good
                if resp.partial_failure_error and resp.partial_failure_error.message:
                    err_msg = resp.partial_failure_error.message
                    # We don't have per-row mapping easily; conservative handling:
                    with transaction.atomic():
                        PendingChange.objects.filter(id__in=cm_ids).update(status="error", error=err_msg[:1000])
                else:
                    with transaction.atomic():
                        PendingChange.objects.filter(id__in=cm_ids).update(status="done", error="")
                    processed += len(ok)
            except Exception as e:
                with transaction.atomic():
                    PendingChange.objects.filter(id__in=cm_ids).update(status="error", error=str(e)[:1000])

        # Any items left in 'processing' at this point didn't match either path — mark error
        with transaction.atomic():
            PendingChange.objects.filter(id__in=ids, status="processing").update(
                status="error", error="No applicable lead operation (need gclid/gbraid/wbraid or email/phone)."
            )

    return processed

# ---- GA -> SF (Lead): publish PE from Lead Forms ---------------------------

def pull_lead_deltas(topic: str = "/event/GA_Lead_Upsert__e") -> int:
    """
    Pull lead form submissions from Google Ads and publish a Platform Event to SF.
    Requires a Platform Event in SF whose schema matches the payload you send.
    """
    client = GoogleAds()
    # GAQL fields below may require adjustment based on your API version & assets used
    gaql = """
        SELECT
          lead_form_submission_data.resource_name,
          lead_form_submission_data.gclid,
          lead_form_submission_data.submission_date_time,
          lead_form_submission_data.ad_group_ad,
          lead_form_submission_data.ad_group,
          lead_form_submission_data.campaign
        FROM lead_form_submission_data
        ORDER BY lead_form_submission_data.submission_date_time DESC
        LIMIT 50
    """

    published = 0
    try:
        for row in client.search_stream(gaql):
            # Depending on your mapper needs, enrich with user-provided data if you store it.
            payload = {
                # Adjust field names to your SF Platform Event schema:
                "Gclid__c": getattr(row.lead_form_submission_data, "gclid", "") or "",
                "SubmissionTime__c": getattr(row.lead_form_submission_data, "submission_date_time", "") or "",
                "CampaignResource__c": getattr(row.lead_form_submission_data, "campaign", "") or "",
                "AdGroupResource__c": getattr(row.lead_form_submission_data, "ad_group", "") or "",
                "AdGroupAdResource__c": getattr(row.lead_form_submission_data, "ad_group_ad", "") or "",
            }
            publish_sf_platform_event(topic, payload)
            published += 1
    except Exception:
        # Swallow errors here to avoid killing the chain; real impl: log/metrics
        return published

    return published
