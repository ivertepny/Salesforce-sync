from datetime import datetime

def to_dt(ts: str | None):
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def campaign_row_to_dict(row):
    c = row.campaign
    return {
        "resource_name": c.resource_name,
        "campaign_id": int(c.id),
        "name": c.name,
        "status": c.status.name if hasattr(c.status, "name") else str(c.status),
        "advertising_channel_type": c.advertising_channel_type.name if hasattr(c.advertising_channel_type, "name") else str(c.advertising_channel_type),
        "budget_micros": None,
        "start_date": str(c.start_date) or None,
        "end_date": str(c.end_date) or None,
        "external_updated_at": to_dt(getattr(c, "last_modified_time", None)),
    }
