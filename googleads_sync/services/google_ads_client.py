import os
from typing import Iterable
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

def _env(name: str, default: str | None = None):
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"Missing env var: {name}")
    return val

class GoogleAds:
    """Thin wrapper around the official Google Ads Python client (gRPC)."""

    def __init__(self):
        developer_token = _env("GOOGLE_ADS_DEVELOPER_TOKEN")
        client_id = _env("GOOGLE_ADS_CLIENT_ID")
        client_secret = _env("GOOGLE_ADS_CLIENT_SECRET")
        refresh_token = _env("GOOGLE_ADS_REFRESH_TOKEN")
        login_customer_id = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
        self.customer_id = _env("GOOGLE_ADS_CUSTOMER_ID")

        config_dict = {
            "developer_token": developer_token,
            "oauth2": {
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
            },
        }
        if login_customer_id:
            config_dict["login_customer_id"] = login_customer_id

        self.client = GoogleAdsClient.load_from_dict(config_dict)
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

    def mutate_campaigns(self, operations: list):
        try:
            response = self.campaign_service.mutate_campaigns(
                customer_id=self.customer_id,
                operations=operations,
            )
            return response
        except GoogleAdsException as ex:
            raise
