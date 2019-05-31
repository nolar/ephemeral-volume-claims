import asyncio
import datetime
import os

import kopf
import kubernetes.client.rest
import yaml

# Configs.
DEFAULT_EXPIRY_PERIOD = 1 * 30
DEFAULT_GRACE_PERIOD = 1 * 30
COUNTDOWN_STEPS = 10

# State.
PODS_CLAIMS = {}  # {('pod-name', 'claim-name'): datetime.datetime('last-seen')}
EXPIRY_TASKS = {}  # {'evc-name': asyncio.Task}
CLEANUP_TASKS = {}  # {'evc-name': asyncio.Task}


@kopf.on.create('zalando.org', 'v1', 'ephemeralvolumeclaims')
def create_fn(meta, spec, namespace, logger, body, **kwargs):

    name = meta.get('name')
    size = spec.get('size')
    if not size:
        raise kopf.HandlerFatalError(f"Size must be set. Got {size!r}.")

    path = os.path.join(os.path.dirname(__file__), 'pvc.yaml')
    tmpl = open(path, 'rt').read()
    text = tmpl.format(name=name, size=size)
    data = yaml.load(text)

    kopf.adopt(data, owner=body)

    api = kubernetes.client.CoreV1Api()
    obj = api.create_namespaced_persistent_volume_claim(
        namespace=namespace,
        body=data,
    )

    logger.info(f"PVC is created: %s", obj)

    return {'pvc-name': obj.metadata.name}


# @kopf.on.update('zalando.org', 'v1', 'ephemeralvolumeclaims')
# def update_fn(body, diff, spec, status, namespace, logger, **kwargs):
#
#     size = spec.get('size', None)
#     if not size:
#         raise kopf.HandlerFatalError(f"Size must be set. Got {size!r}.")
#
#     pvc_name = status['create_fn']['pvc-name']
#     pvc_patch = {'spec': {'resources': {'requests': {'storage': size}}}}
#
#     print(repr(pvc_patch))
#
#     api = kubernetes.client.CoreV1Api()
#     obj = api.patch_namespaced_persistent_volume_claim(
#         namespace=namespace,
#         name=pvc_name,
#         body=pvc_patch,
#     )
#
#     logger.info(f"PVC child is updated: %s", obj)


@kopf.on.field('zalando.org', 'v1', 'ephemeralvolumeclaims', field='metadata.labels')
def relabel(diff, status, namespace, **kwargs):

    labels_patch = {field[0]: new for op, field, old, new in diff}
    pvc_name = status['create_fn']['pvc-name']
    pvc_patch = {'metadata': {'labels': labels_patch}}

    api = kubernetes.client.CoreV1Api()
    obj = api.patch_namespaced_persistent_volume_claim(
        namespace=namespace,
        name=pvc_name,
        body=pvc_patch,
    )

#
# Watch all the pods, remember which EVCs/PVCs they refer (i.e. use).
# This also covers the cases when a pod is created before the EVC/PVC it uses.
# The global state of the EVC/PVC usage is checked in the expiry/cleanup countdowns.
#

@kopf.on.event('', 'v1', 'pods')
async def pod_event(spec, name, namespace, status, logger, **kwargs):
    pod_is_done = any(
        cond['last_transition_time']
        for cond in status.get('conditions', [])
        if cond['reason'] == 'PodCompleted' and cond['status'] == 'True'
    )
    pod_claims = [
        volume['persistent_volume_claim']['claim_name']
        for volume in spec.get('volumes', [])
        if volume.get('persistent_volume_claim')
    ]

    # Modify the global state for other pods to consider this pod too.
    # The global state is checked in the expiry & grace countdowns:
    # once they notice some specific state, they stop the countdown.
    now = datetime.datetime.utcnow().isoformat()
    for evc_name in pod_claims:
        PODS_CLAIMS[(evc_name, name)] = now if pod_is_done else None

    # Patch every involved EVC, to trigger an event-/cause-reaction in the context of that EVC.
    if pod_is_done:
        evc_patch = {'status': {'usage': {name: {'done': now}}}}
        for evc_name in pod_claims:
            patch_evc(namespace=namespace, name=evc_name, patch=evc_patch)

#
# Expiry countdown starts on creation of an EVC (or resumes on operator start),
# and continues until a pod appears, which uses this EVC/PVC.
# If the EVC is deleted for any reason, the countdown is force-stopped.
#

@kopf.on.create('zalando.org', 'v1', 'ephemeralvolumeclaims')
@kopf.on.update('zalando.org', 'v1', 'ephemeralvolumeclaims')
@kopf.on.resume('zalando.org', 'v1', 'ephemeralvolumeclaims')
async def start_expiry_countdown(meta, spec, uid, name, namespace, logger, **kwargs):
    expiry_period = spec.get('expiryPeriod', DEFAULT_EXPIRY_PERIOD)
    created = meta.get('creationTimestamp')
    created = datetime.datetime.fromisoformat(created.rstrip('Z'))
    deadline = created + datetime.timedelta(seconds=expiry_period)

    if uid not in EXPIRY_TASKS:
        loop = asyncio.get_running_loop()
        EXPIRY_TASKS[uid] = loop.create_task(expiry_countdown(
            namespace=namespace, name=name, logger=logger,
            deadline=deadline, interval=expiry_period / COUNTDOWN_STEPS,
        ))


@kopf.on.delete('zalando.org', 'v1', 'ephemeralvolumeclaims')
async def stop_expiry_countdown(uid, **kwargs):
    if uid in EXPIRY_TASKS:
        EXPIRY_TASKS[uid].cancel()
        del EXPIRY_TASKS[uid]


async def expiry_countdown(
    namespace: str,
    name: str,
    logger,
    deadline: datetime.datetime,
    interval: float,
):
    # Give it some time to handle all events & queues on the operator startup.
    # Otherwise, we start the countdown, and the pod events arrive an instant later.
    await asyncio.sleep(1.0)

    # If the EVC is used at start, the the expiry countdown is not needed at all.
    # This happens when operator restarts, and re-scans the existing pods/evcs.
    is_used = any(True for evc_name, pod_name in PODS_CLAIMS if evc_name == name)
    if is_used:
        return

    countdown = (deadline - datetime.datetime.utcnow()).total_seconds()
    while not is_used and countdown > 0:
        logger.info(f"--> Countdown to expiry: {countdown}s left.")
        await asyncio.sleep(min(interval, countdown))
        is_used = any(True for evc_name, pod_name in PODS_CLAIMS if evc_name == name)
        countdown = (deadline - datetime.datetime.utcnow()).total_seconds()

    if is_used:
        logger.info(f"--> Countdown to expiry: cancelled, as the EVC is now used.")
    else:
        logger.info(f"--> Countdown to expiry: reached! Deleting the EVC.")
        await delete_evc(namespace=namespace, name=name)

#
# Grace period countdown starts when all pods using this EVC are completed.
# For this, every pod event is watched, and all pod's claims are patched with
# that pod's completion timestamp (or none, if it is still running/pending).
#
# This triggers a handling cycle on the EVC - in the context of that EVC,
# where a background task can be started (proper logger, name, uid).
#

@kopf.on.field('zalando.org', 'v1', 'ephemeralvolumeclaims', field='status.usage')
async def start_grace_countdown(spec, new, uid, name, namespace, logger, **kwargs):
    grace_period = spec.get('gracePeriod', DEFAULT_GRACE_PERIOD)
    if not new:
        return

    # EVC is done as soon as all pods that do use or did use it are done.
    pods_ts = [pod_info.get('done') for pod_name, pod_info in new.items()]
    pods_ts = [datetime.datetime.fromisoformat(ts) if ts else None for ts in pods_ts]
    is_done = all(pods_ts)
    done_ts = max([ts for ts in pods_ts if ts]) if is_done else None
    deadline = done_ts + datetime.timedelta(seconds=grace_period) if done_ts else None

    # If the countdown is running, stop it, as it either will be replaced by a new one
    # (if a new pod completion postpones the deadline), or stopped (if a pod is running).
    if uid in CLEANUP_TASKS and not CLEANUP_TASKS[uid].done():
        CLEANUP_TASKS[uid].cancel()

    # Start a background task with a countdown and deletion. And finish the handler successfully.
    # Once abandoned, start a countdown to the actual deletion.
    # And if the EVC is not reclaimed, delete it after the grace period.
    if is_done:
        loop = asyncio.get_running_loop()
        CLEANUP_TASKS[uid] = loop.create_task(grace_countdown(
            namespace=namespace, name=name, logger=logger,
            deadline=deadline, interval=grace_period / COUNTDOWN_STEPS,
        ))


@kopf.on.delete('zalando.org', 'v1', 'ephemeralvolumeclaims')
async def stop_grace_countdown(uid, **kwargs):
    if uid in CLEANUP_TASKS:
        CLEANUP_TASKS[uid].cancel()
        del CLEANUP_TASKS[uid]


async def grace_countdown(
    namespace: str,
    name: str,
    logger,
    deadline: datetime.datetime,
    interval: float,
):

    # EVC is done as soon as all pods that do use or did use it are done.
    is_used = not all(ts for (evc_name, _), ts in PODS_CLAIMS.items() if evc_name == name)
    countdown = (deadline - datetime.datetime.utcnow()).total_seconds()
    while not is_used and countdown > 0:
        logger.info(f"--> Countdown to cleanup: {countdown}s left.")
        await asyncio.sleep(min(interval, countdown))
        is_used = not all(ts for (evc_name, _), ts in PODS_CLAIMS.items() if evc_name == name)
        countdown = (deadline - datetime.datetime.utcnow()).total_seconds()

    if is_used:
        logger.info(f"--> Countdown to cleanup: cancelled, as the EVC is used.")
    else:
        logger.info(f"--> Countdown to cleanup: reached! Deleting the EVC.")
        await delete_evc(namespace=namespace, name=name)


def patch_evc(*, namespace, name, patch):
    try:
        api = kubernetes.client.CustomObjectsApi()
        api.patch_namespaced_custom_object(
            group='zalando.org',
            version='v1',
            plural='ephemeralvolumeclaims',
            namespace=namespace,
            name=name,
            body=patch,
        )
    except kubernetes.client.rest.ApiException as e:
        if e.status == 404:
            pass  # it is probably a regular PVC, not an EVC.
        else:
            raise


async def delete_evc(namespace, name):
    try:
        api = kubernetes.client.CustomObjectsApi()
        api.delete_namespaced_custom_object(
            group='zalando.org',
            version='v1',
            plural='ephemeralvolumeclaims',
            namespace=namespace,
            name=name,
            body={},
        )
    except kubernetes.client.rest.ApiException as e:
        if e.status == 404:
            pass  # already absent, nothing to delete
        else:
            raise
