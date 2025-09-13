# googleads_sync/services/sf_bridge.py
from typing import Dict, Any

from googleads_sync.salesforce.pubsub_client import PubSubClient


def publish_sf_platform_event(topic: str, payload: Dict[str, Any]) -> str:
    """
    Публікує Platform Event у Salesforce через Pub/Sub API (gRPC+Avro).
    ВАЖЛИВО: schema для topic має існувати у SF. Payload має відповідати схемі.
    """
    client = PubSubClient()
    return client.publish_platform_event(topic, payload)
