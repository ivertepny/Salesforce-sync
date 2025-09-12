# sync_google_ads.py
from django.core.management.base import BaseCommand
from googleads_sync.tasks import sync_google_ads_pipeline

class Command(BaseCommand):
    help = "Kick off the Google Ads sync pipeline (pull + push)."

    def handle(self, *args, **options):
        task_id = sync_google_ads_pipeline.delay()
        self.stdout.write(self.style.SUCCESS(f"Pipeline started: {task_id}"))



# from django.core.management.base import BaseCommand
# from googleads_sync.tasks import sync_google_ads_pipeline
#
# class Command(BaseCommand):
#     help = "Run Google Ads sync pipeline"
#
#     def handle(self, *args, **options):
#         # Було: відправляли у брокер
#         # task_id = sync_google_ads_pipeline.delay()
#
#         # Стане: виконуємо локально, без брокера
#         result = sync_google_ads_pipeline.apply(args=[], kwargs={})
#         self.stdout.write(self.style.SUCCESS(f"Done. Result: {result.get()}"))
