# googleads_sync/salesforce/pubsub_client.py
import os
import grpc
import json
from typing import Dict, Any, Generator, Tuple
from fastavro import schemaless_reader, schemaless_writer
from io import BytesIO

from .client_rest import get_sf, soql_query
from .grpc_stubs import pubsub_api_pb2 as pb2
from .grpc_stubs import pubsub_api_pb2_grpc as pb2_grpc

PUBSUB_ENDPOINT = os.getenv("SF_PUBSUB_ENDPOINT", "api.pubsub.salesforce.com:7443")


def _auth_metadata() -> Tuple[Tuple[str, str], ...]:
    sf = get_sf()
    session_id = sf.session_id
    instance_url = f"https://{sf.sf_instance}"
    org_id = os.getenv("SF_ORG_ID")
    if not org_id:
        res = soql_query("SELECT Id FROM Organization")
        org_id = res["records"][0]["Id"]
        os.environ["SF_ORG_ID"] = org_id
    # ВАЖЛИВО: ключі нижнім регістром
    return (
        ("accesstoken", session_id),
        ("instanceurl", instance_url),
        ("tenantid", org_id),
    )


class PubSubClient:
    def __init__(self):
        self.channel = grpc.secure_channel(PUBSUB_ENDPOINT, grpc.ssl_channel_credentials())
        self.stub = pb2_grpc.PubSubStub(self.channel)
        self._schema_cache: Dict[str, Dict[str, Any]] = {}

    def get_schema(self, schema_id: str) -> Dict[str, Any]:
        if schema_id in self._schema_cache:
            return self._schema_cache[schema_id]
        req = pb2.SchemaRequest(schema_id=schema_id)
        resp = self.stub.GetSchema(req, metadata=_auth_metadata())
        schema_json = json.loads(resp.schema_json)
        self._schema_cache[schema_id] = schema_json
        return schema_json

    def get_latest_schema_id_for_topic(self, topic_name: str) -> str:
        req = pb2.TopicRequest(topic_name=topic_name)
        info = self.stub.GetTopic(req, metadata=_auth_metadata())
        return info.schema_id

    def subscribe(
        self,
        topic_name: str,
        replay_preset: str = "LATEST",
        replay_id: bytes | None = None,
        batch: int = 1,
    ) -> Generator[Dict[str, Any], None, None]:
        sub_req = pb2.SubscriptionRequest(
            topic_name=topic_name,
            num_requested=batch,
            replay_preset=getattr(pb2.ReplayPreset, replay_preset),
            replay_id=replay_id or b"",
        )
        # Salesforce очікує stream SubscriptionRequest → stream FetchResponse
        sub_stream = self.stub.Subscribe(iter([sub_req]), metadata=_auth_metadata())
        for fetch_resp in sub_stream:
            for event in fetch_resp.events:
                payload_bytes = event.event.payload
                schema_id = event.event.schema_id
                try:
                    schema = self.get_schema(schema_id)
                    decoded = schemaless_reader(BytesIO(payload_bytes), schema)
                except Exception:
                    decoded = {"_raw": payload_bytes.hex() if payload_bytes else None, "_schema_id": schema_id}
                yield {
                    "schema_id": schema_id,
                    "replay_id": event.replay_id,
                    "payload": decoded,
                }

    def publish_platform_event(self, topic_name: str, payload_dict: Dict[str, Any]) -> str:
        schema_id = self.get_latest_schema_id_for_topic(topic_name)
        schema = self.get_schema(schema_id)
        buf = BytesIO()
        schemaless_writer(buf, schema, payload_dict)
        data = buf.getvalue()
        req = pb2.PublishRequest(
            topic_name=topic_name,
            events=[pb2.ProducerEvent(schema_id=schema_id, payload=data)],
        )
        resp = self.stub.Publish(req, metadata=_auth_metadata())
        return ",".join(e.replay_id.hex() for e in resp.results)
