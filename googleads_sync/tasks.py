# googleads_sync/tasks.py
from celery import shared_task, chain, group
from .services.pipelines import pull_campaign_deltas, push_campaign_changes

@shared_task(bind=True, name="ads_sync.pull_campaign_deltas")
def pull_campaign_deltas_task(self):
    processed = pull_campaign_deltas()
    return {"processed": processed}

@shared_task(bind=True, name="ads_sync.push_campaign_changes")
def push_campaign_changes_task(self):
    processed = push_campaign_changes()
    return {"processed": processed}

@shared_task(bind=True, name="ads_sync.sync_google_ads_pipeline")
def sync_google_ads_pipeline(self):
    workflow = chain(
        pull_campaign_deltas_task.s(),
        push_campaign_changes_task.si(),  # immutable: не отримає попередній результат
    )
    # важливо: запускати асинхронно всередині задачі
    res = workflow.apply_async()
    return {"chain_id": res.id}

@shared_task(bind=True, name="ads_sync.nightly_full_reconcile")
def nightly_full_reconcile(self):
    g = group([
        pull_campaign_deltas_task.s(),
        push_campaign_changes_task.s(),
    ])
    # так само асинхронно
    res = g.apply_async()
    return {"group_id": res.id}

