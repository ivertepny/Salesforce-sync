# googleads_sync/tasks.py
from celery import shared_task, chain, group
from .services.pipelines import (
    pull_campaign_deltas,
    push_campaign_changes,
    pull_lead_deltas,       # ADD
    push_lead_changes,      # ADD
)

@shared_task(bind=True, name="ads_sync.pull_campaign_deltas")
def pull_campaign_deltas_task(self):
    processed = pull_campaign_deltas()
    return {"processed": processed}

@shared_task(bind=True, name="ads_sync.push_campaign_changes")
def push_campaign_changes_task(self, _prev=None, **_):
    processed = push_campaign_changes()
    return {"processed": processed}

# --- ADD: tasks for leads ---
@shared_task(bind=True, name="ads_sync.push_lead_changes")
def push_lead_changes_task(self, _prev=None, **_):
    processed = push_lead_changes()
    return {"processed": processed}

@shared_task(bind=True, name="ads_sync.pull_lead_deltas")
def pull_lead_deltas_task(self, _prev=None, **_):
    processed = pull_lead_deltas()
    return {"processed": processed}

@shared_task(bind=True, name="ads_sync.sync_google_ads_pipeline")
def sync_google_ads_pipeline(self):
    workflow = chain(
        pull_campaign_deltas_task.s(),
        push_campaign_changes_task.si(),   # immutable
        push_lead_changes_task.si(),       # SF → GA для lead
        pull_lead_deltas_task.si(),        # GA → SF (Platform Event)
    )
    res = workflow.apply_async()
    return {"chain_id": res.id}

@shared_task(bind=True, name="ads_sync.nightly_full_reconcile")
def nightly_full_reconcile(self):
    g = group([
        pull_campaign_deltas_task.s(),
        push_campaign_changes_task.si(),
        push_lead_changes_task.si(),
        pull_lead_deltas_task.si(),
    ])
    res = g.apply_async()
    return {"group_id": res.id}
