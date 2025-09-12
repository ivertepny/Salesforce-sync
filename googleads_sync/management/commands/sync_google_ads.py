from django.core.management.base import BaseCommand
from googleads_sync.tasks import sync_google_ads_pipeline

class Command(BaseCommand):
    help = "Kick off the Google Ads sync pipeline (pull + push)."

    def handle(self, *args, **options):
        task_id = sync_google_ads_pipeline.delay()
        self.stdout.write(self.style.SUCCESS(f"Pipeline started: {task_id}"))
