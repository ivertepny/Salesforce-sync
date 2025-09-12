# googleads_sync/services/google_ads_client.py
import os
from typing import Iterable
from google.ads.googleads.client import GoogleAdsClient


def _env(name: str, required: bool = True, default=None):
    val = os.getenv(name, default)
    if required and (val is None or val == ""):
        raise RuntimeError(f"Missing env var: {name}")
    return val


class GoogleAds:
    def __init__(self):
        """
        Ініціалізує Google Ads SDK через dict-конфіг.
        Для версій SDK >= 20:
          - 'use_proto_plus' обов'язковий
          - OAuth2 поля (client_id, client_secret, refresh_token) мають бути на верхньому рівні
        """
        config_dict = {
            "developer_token": _env("GOOGLE_ADS_DEVELOPER_TOKEN"),
            # OAuth2 — ВЕРХНІЙ рівень (не в "oauth2": {...})
            "client_id": _env("GOOGLE_ADS_CLIENT_ID"),
            "client_secret": _env("GOOGLE_ADS_CLIENT_SECRET"),
            "refresh_token": _env("GOOGLE_ADS_REFRESH_TOKEN"),
            # обов'язковий ключ для нових версій
            "use_proto_plus": True,
        }

        login_cid = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
        if login_cid:
            # без дефісів, напр. "1234567890"
            config_dict["login_customer_id"] = login_cid

        # з яким CID працювати в запитах
        self.customer_id = _env("GOOGLE_ADS_CUSTOMER_ID")

        # ініціалізація клієнта та сервісів
        self.client: GoogleAdsClient = GoogleAdsClient.load_from_dict(config_dict)
        self.ga_service = self.client.get_service("GoogleAdsService")
        self.campaign_service = self.client.get_service("CampaignService")

    def search_stream(self, gaql: str) -> Iterable:
        request = self.client.get_type("SearchGoogleAdsStreamRequest")
        request.customer_id = self.customer_id
        request.query = gaql
        stream = self.ga_service.search_stream(request=request)
        for batch in stream:
            for row in batch.results:
                yield row

    def pause_campaign(self, resource_name: str):
        op = self.client.get_type("CampaignOperation")()
        op.update.resource_name = resource_name
        status_enum = self.client.get_type("CampaignStatusEnum").CampaignStatus
        op.update.status = status_enum.PAUSED
        mask = self.client.get_type("FieldMask")
        mask.paths.append("status")
        op.update_mask.CopyFrom(mask)
        return self.campaign_service.mutate_campaigns(
            customer_id=self.customer_id,
            operations=[op],
        )
