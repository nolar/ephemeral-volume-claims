import datetime
import os
from typing import Collection, Mapping, Any

import iso8601
import kopf
import kubernetes.client.rest
import yaml

# How to name the annotations put on the pods (only those with volume claims).
ANNOTATIONS_PREFIX = 'ephemeralvolumeclaims.zalando.org'
USAGE_STARTED_FLAG = f'{ANNOTATIONS_PREFIX}/usage-started'
USAGE_STOPPED_FLAG = f'{ANNOTATIONS_PREFIX}/usage-stopped'

# How often to log the countdown to expiry/cleanup (seconds).
COUNTDOWN_INTERVAL = float(os.environ.get('COUNTDOWN_INTERVAL', 10))

# How long an EVC/PVC is allowed to be not used by any pod on creation.
EXPIRY_PERIOD = float(os.environ.get('EXPIRY_PERIOD', 30))

# How long an EVC/PVC is allowed to be orphaned after used at least once.
GRACE_PERIOD = float(os.environ.get('GRACE_PERIOD', 30))


####################################################################################################
#
# Create children PVCs and propagate changes from EVCs to PVCs.
# This is the simple and straightforward event-driven functionality.
#
@kopf.on.create('zalando.org', 'v1', 'ephemeralvolumeclaims')
def create_fn(
        spec: kopf.Spec, name: str, namespace: str, patch: kopf.Patch,
        logger: kopf.ObjectLogger, **_: Any):

    # Parse the specification.
    size = spec.get('size')
    if not size:
        raise kopf.PermanentError(f"Size must be set. Got {size!r}.")

    # Render a template.
    path = os.path.join(os.path.dirname(__file__), 'pvc.yaml')
    tmpl = open(path, 'rt').read()
    text = tmpl.format(name=name, size=size)
    data = yaml.safe_load(text)

    # Take ownership, propagate labels, etc -- as the framework believes is right.
    kopf.adopt(data)

    # Actually create it and notify the users (also via `kubectl get evcs`).
    create_pvc(namespace=namespace, data=data)
    logger.info("PVC is created.")
    patch.status['state'] = "Created"
    patch.status['reason'] = None


# TODO: It was for field changes originally. But PVC sizes are not changeable. Find another example.
# @kopf.on.update('zalando.org', 'v1', 'ephemeralvolumeclaims')
# def update_fn(body, diff, spec, status, namespace, logger, **_: Any):
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
def propagate_labels(labels: Mapping[str, str], name: str, namespace: str, **_: Any):

    # TODO: use diff-DSL to flat-walk over labels (including deletions, incl. new [labels] section).
    pvc_patch = {'metadata': {'labels': dict(labels)}}

    api = kubernetes.client.CoreV1Api()
    api.patch_namespaced_persistent_volume_claim(
        namespace=namespace,
        name=name,
        body=pvc_patch,
    )


####################################################################################################
#
# Watch all pods in all namespaces. Assume that all claims they used are EVCs.
# Report to the EVCs that they are used by these pods. But only once per pod
# (in the assumption that the pod's volumes/claims are not changed at runtime).
#
# This also covers the cases when a pod is created before the EVC/PVC. On pod
# event, the relevant EVC was not patched (it was absent). But the pods are
# discovered in EVC's on-creation handler.
#
def has_volume_claims(spec: kopf.Spec, **_: Any):
    return any(
        volume.get('persistentVolumeClaim', {}).get('claimName') is not None
        for volume in spec.get('volumes', [])
    )


@kopf.on.event('', 'v1', 'pods',
               when=has_volume_claims,
               annotations={USAGE_STARTED_FLAG: kopf.ABSENT})
def report_pod2evc_usage(
        spec: kopf.Spec, name: str, namespace: str, patch: kopf.Patch, **_: Any):

    # Patch every involved EVC, to trigger an event-/cause-reaction in the context of that EVC.
    # Regular PVCs with no EVCs are silently ignored by the patching method.
    now = datetime.datetime.utcnow().isoformat() + 'Z'
    evc_patch = {'status': {'usage': {name: {'used': now}}}}
    for evc_name in get_claim_names(spec):
        patch_evc(namespace=namespace, name=evc_name, patch=evc_patch)

    # Exclude this pod from further event handling. So, the patching is done only once.
    patch.metadata.annotations[USAGE_STARTED_FLAG] = 'yes'


def get_pod_completion_timestamp(status: kopf.Status):
    for cond in status.get('conditions', []):
        if cond['reason'] == 'PodCompleted' and cond['status'] == 'True':
            return cond['last_transition_time'].isoformat()
    return None


@kopf.on.create('zalando.org', 'v1', 'ephemeralvolumeclaims')
def identify_existing_pods(namespace: str, patch: kopf.Patch, **_: Any):
    api = kubernetes.client.CoreV1Api()
    pods = api.list_namespaced_pod(namespace=namespace)
    usage_patch = patch.status.setdefault('usage', {})
    for pod in pods.items:
        usage_patch[pod.metadata.name] = {
            'used': pod.metadata.creation_timestamp.isoformat(),
            'done': get_pod_completion_timestamp(pod.status.to_dict()),
        }


####################################################################################################
#
# Similarly, when a pod achieves the "completed" state (success or failure),
# patch all involved EVCs with the pods' completion information.
# But only once (assuming that the pods is not restarted once completed).
#
# When pods are deleted, deliver this information to relevant EVCs
# as if the pod has completed and does not need EVCs/PVCs anymore.
#
@kopf.on.event('', 'v1', 'pods',
               annotations={USAGE_STOPPED_FLAG: kopf.ABSENT},
               when=lambda status, **_: status.get('phase') in ['Failed', 'Succeeded'])
def report_pod2evc_completion(
        spec: kopf.Spec, name: str, namespace: str, patch: kopf.Patch, **_: Any):

    # Patch every involved EVC, to trigger an event-/cause-reaction in the context of that EVC.
    # Regular PVCs with no EVCs are silently ignored by the patching method.
    now = datetime.datetime.utcnow().isoformat() + 'Z'
    evc_patch = {'status': {'usage': {name: {'done': now}}}}
    for evc_name in get_claim_names(spec):
        patch_evc(namespace=namespace, name=evc_name, patch=evc_patch)

    # Exclude this pod from further event handling. So, the patching is done only once.
    patch.metadata.annotations[USAGE_STOPPED_FLAG] = 'yes'


@kopf.on.event('', 'v1', 'pods',
               when=lambda type, **_: type == 'DELETED')
def report_pod2evc_deletion(
        spec: kopf.Spec, name: str, namespace: str, **_: Any):

    now = datetime.datetime.utcnow().isoformat() + 'Z'
    evc_patch = {'status': {'usage': {name: {'done': now}}}}
    for evc_name in get_claim_names(spec):
        patch_evc(namespace=namespace, name=evc_name, patch=evc_patch)


####################################################################################################
#
# Expiry countdown starts on creation of an EVC (or on operator restart),
# and continues until at least one pod appears that uses this EVC/PVC.
#
# Give it some time (1s) to catch all events & queues on the operator startup.
# Otherwise, it could be so, that we start the countdown, and the pod events
# arrive an instant later, leading to some unnecessary work & logs.
#
@kopf.daemon('zalando.org', 'v1', 'ephemeralvolumeclaims',
             when=lambda status, **_: not status.get('usage'),
             initial_delay=1.0)
def expiry_wait(
        name: str, namespace: str, meta: kopf.Meta, patch: kopf.Patch,
        logger: kopf.ObjectLogger, stopped: kopf.SyncDaemonStopperChecker, **_: Any):

    # Do the countdown. The daemon will be stopped if the precondition is changed.
    # The deadline is fixed (not moving), so we do not do any extra checks on it.
    created = iso8601.parse_date(meta.get('creationTimestamp'))
    deadline = created + datetime.timedelta(seconds=EXPIRY_PERIOD)
    deadline = deadline.replace(tzinfo=None)  # assume that naive time is in UTC
    interval = COUNTDOWN_INTERVAL
    countdown = (deadline - datetime.datetime.utcnow()).total_seconds()
    while not stopped and countdown > 0:
        logger.info(f"--> Countdown to expiry: {countdown} seconds left.")
        stopped.wait(min(interval, countdown))  # instead of time.sleep()
        countdown = (deadline - datetime.datetime.utcnow()).total_seconds()

    # Explain the reason of stopping the countdown.
    if kopf.DaemonStoppingReason.FILTERS_MISMATCH in stopped.reason:
        logger.info(f"--> Countdown to expiry: stopped: the EVC/PVC is now used.")
    elif stopped:
        logger.info(f"--> Countdown to expiry: stopped: the daemon or operator is exiting.")
    else:
        logger.info(f"--> Countdown to expiry: reached: deleting the PVC.")
        delete_pvc(namespace=namespace, name=name)
        patch.status['state'] = "Deleted"
        patch.status['reason'] = "Expired"


####################################################################################################
#
# Grace period countdown starts when all pods using this EVC are completed.
# For this, every pod event is watched, and all pod's EVCs are patched with
# that pod's completion timestamp (or none, if it is still running/pending).
#
# This change in the ``status.usage`` field triggers a reaction on the EVC side,
# where the countdown daemon is either started or stopped (maybe few times).
#
def get_deadline(status: kopf.Status) -> datetime.datetime:
    usage = status.get('usage', {})
    pods_ts = [pod_info.get('done') for pod_name, pod_info in usage.items()]
    pods_ts = [iso8601.parse_date(ts) if ts else None for ts in pods_ts]
    is_done = bool(usage) and all(pods_ts)  # read as: there are no started-but-not-yet-done pods.
    done_ts = max([ts for ts in pods_ts if ts]) if is_done else None
    deadline = done_ts + datetime.timedelta(seconds=GRACE_PERIOD) if done_ts else None
    return deadline.replace(tzinfo=None) if deadline else None  # assume naive time is in UTC


@kopf.daemon('zalando.org', 'v1', 'ephemeralvolumeclaims',
             when=lambda status, **_: get_deadline(status) is not None)
def cleanup_wait(
        name: str, namespace: str, status: kopf.Status, patch: kopf.Patch,
        logger: kopf.ObjectLogger, stopped: kopf.SyncDaemonStopperChecker, **_: Any):

    # Do the countdown. The daemon will be stopped if the precondition is changed.
    # However, the deadline moves if new pods appear & disappear during countdown.
    deadline = get_deadline(status)
    interval = COUNTDOWN_INTERVAL
    countdown = (deadline - datetime.datetime.utcnow()).total_seconds()
    while not stopped and deadline and countdown > 0:
        logger.info(f"--> Countdown to cleanup: {countdown} seconds left.")
        stopped.wait(min(interval, countdown))  # instead of time.sleep()
        deadline = get_deadline(status)
        countdown = (deadline - datetime.datetime.utcnow()).total_seconds() if deadline else 0

    # Explain the reason of stopping the countdown.
    if not deadline or kopf.DaemonStoppingReason.FILTERS_MISMATCH in stopped.reason:
        logger.info(f"--> Countdown to cleanup: stopped: the EVC/PVC is now used.")
    elif stopped:
        logger.info(f"--> Countdown to cleanup: stopped: the daemon is exiting.")
    else:
        logger.info(f"--> Countdown to cleanup: reached: deleting the PVC.")
        delete_pvc(namespace=namespace, name=name)
        patch.status['state'] = "Deleted"
        patch.status['reason'] = "Cleanup"


####################################################################################################
#
# Helper functions used in the handlers (to save the lines for readability).
#


def get_claim_names(spec: kopf.Spec) -> Collection[str]:
    return [
        volume['persistentVolumeClaim']['claimName']
        for volume in spec.get('volumes', [])
        if 'persistentVolumeClaim' in volume  # ignore secrets & alike.
    ]


def patch_evc(*, namespace: str, name: str, patch: Any) -> None:
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


def create_pvc(*, namespace: str, data: Any) -> None:
    try:
        api = kubernetes.client.CoreV1Api()
        api.create_namespaced_persistent_volume_claim(
            namespace=namespace,
            body=data,
        )
    except kubernetes.client.rest.ApiException as e:
        if e.status == 409:
            pass  # already exists, nothing to create
        else:
            raise


def delete_pvc(*, namespace: str, name: str) -> None:
    try:
        api = kubernetes.client.CoreV1Api()
        api.delete_namespaced_persistent_volume_claim(
            namespace=namespace,
            name=name,
        )
    except kubernetes.client.rest.ApiException as e:
        if e.status == 404:
            pass  # already absent, nothing to delete
        else:
            raise


#
# The end.
#
####################################################################################################
