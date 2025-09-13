# googleads_sync/salesforce/tasks_pubsub.py
from django.db import transaction
from django.utils import timezone as djtz
from celery import shared_task
from .pubsub_client import PubSubClient
from ..models import SalesforceEvent, ReplayState


@shared_task(bind=True, name="ads_sync.sf_pubsub_subscribe", autoretry_for=(Exception,), retry_backoff=15, retry_jitter=True, max_retries=7)
def sf_pubsub_subscribe(self, topic_name: str = "/data/LeadChangeEvent", replay_preset: str = "LATEST", batch: int = 1):
    """
    Long-lived subscriber for a single topic.
    - Reads last replay_id from DB (ReplayState) → uses CUSTOM replay if present
    - Persists replay_id after successful handling (at-least-once semantics)
    Tip: run on a dedicated Celery queue.
    """
    client = PubSubClient()

    # Read last known replay_id for this topic
    state, _ = ReplayState.objects.get_or_create(topic_name=topic_name)
    preset = "CUSTOM" if state.replay_id else replay_preset
    current_replay = state.replay_id

    received = 0
    for message in client.subscribe(topic_name=topic_name, replay_preset=preset, replay_id=current_replay, batch=batch):
        payload = message.get("payload", {})
        replay_id = message.get("replay_id", None)

        # Extract SF record Id (best-effort)
        sf_id = (
            payload.get("Id")
            or (payload.get("ChangeEventHeader", {}) or {}).get("recordIds", [""])[0]
            or ""
        )

        # 1) Business handling — store event (you can also route to PendingChange here)
        with transaction.atomic():
            SalesforceEvent.objects.create(
                object_name=topic_name,
                sf_id=sf_id,
                payload=payload,
                received_at=djtz.now(),
            )
            # 2) Advance replay_id only AFTER successful handling
            st = ReplayState.objects.select_for_update().get(pk=state.pk)
            st.set_replay(replay_id)

        received += 1

    return {"received": received, "topic": topic_name, "replay": state.replay_id_hex}


@shared_task(bind=True, name="ads_sync.sf_pubsub_publish", autoretry_for=(Exception,), retry_backoff=10, retry_jitter=True, max_retries=5)
def sf_pubsub_publish(self, topic_name: str, payload: dict):
    """Publish a Platform Event via Pub/Sub API."""
    client = PubSubClient()
    replay_ids = client.publish_platform_event(topic_name, payload)
    return {"replay_ids": replay_ids, "topic": topic_name}
