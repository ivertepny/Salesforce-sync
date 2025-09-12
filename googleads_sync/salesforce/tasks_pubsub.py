from django.db import transaction
from django.utils import timezone as djtz
from celery import shared_task
from .pubsub_client import PubSubClient
from ..models_sf import SalesforceEvent

@shared_task(bind=True, name="ads_sync.sf_pubsub_subscribe")
def sf_pubsub_subscribe(self, topic_name: str = "/data/LeadChangeEvent", replay_preset: str = "LATEST"):
    """Run a streaming subscriber and persist incoming events to SalesforceEvent.
    Tip: run this on a dedicated Celery worker queue so it can stay long-lived.
    """
    client = PubSubClient()
    received = 0
    for message in client.subscribe(topic_name=topic_name, replay_preset=replay_preset, batch=1):
        payload = message.get("payload", {})
        sf_id = payload.get("Id") or payload.get("ChangeEventHeader", {}).get("recordIds", [""])[0]
        with transaction.atomic():
            SalesforceEvent.objects.create(
                object_name=topic_name,
                sf_id=sf_id or "",
                payload=payload,
                received_at=djtz.now(),
            )
        received += 1
        # Optionally: add routing to your internal pipeline (e.g., map to PendingChange)

    return {"received": received}

@shared_task(bind=True, name="ads_sync.sf_pubsub_publish")
def sf_pubsub_publish(self, topic_name: str, payload: dict):
    """Publish a Platform Event via Pub/Sub API."""
    client = PubSubClient()
    replay_ids = client.publish_platform_event(topic_name, payload)
    return {"replay_ids": replay_ids}
