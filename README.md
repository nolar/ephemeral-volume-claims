# EphemeralVolumeClaim operator

A Kubernetes operator for `EphemeralVolumeClaim` custom resources,
which internally map to the same-named, same-labelled `PersistentVolumeClaim`,
but automatically delete themselves after a period of no use.

This is a demo project used as a step-by-step tutorial for
[Kopf, Kubernetes Operator Pythonic Framework](https://github.com/zalando-incubator/kopf).

The problem statement and step-by-step tutorial is described here:

* https://kopf.readthedocs.io/en/stable/walkthrough/problem/


## Installation

```shell
pip install kopf kubernetes
```

Apply a custom resource definition:

```shell
kubectl apply -f crd.yaml
```

Run it locally:

```shell
kopf run --namespace=default --verbose ephemeral.py
```

Or deploy it to your cluster with few YAML files and your CI/CD tool:

* https://kopf.readthedocs.io/en/latest/deployment/


## Usage

Example resource:

```yaml
# evc.yaml
kind: EphemeralVolumeClaim
apiVersion: zalando.org/v1
metadata:
  name: my-claim-1
spec:
  size: 1Gi
```

Then, it can be used the same way as a PersistentVolumeClaim — by its name.
Under the hood, it actually creates a PVC with the same name:

```yaml
# pod.yaml
apiVersion: v1
kind: Pod
metadata:
  name: my-pod
  labels:
    application: evc-demo
spec:
  restartPolicy: Never
  volumes:
  - name: my-volume
    persistentVolumeClaim:
      claimName: my-claim-1     # <<< Use the EVC/PVC here.
  containers:
  - name: the-only-one
    image: busybox
    command: ["sh", "-x", "-c"]
    args:
    - |
      mount | grep /vol
      sleep 10s
      #false
    volumeMounts:
    - mountPath: /vol
      name: my-volume 
```


## Configuration

The expiry period (`EXPIRY_PERIOD` env var; default is 30 seconds) defines
how long a PVC is allowed to exist without being used by any pod.

The grace period (`GRACE_PERIOD` env var; default is 30 seconds) defines
how long a PVC is allowed to exist after it was used by a pod, by that pod
is either deleted or completed (and is not going to be restarted).

The countdown interval (`COUNTDOWN_INTERVAL`, default is 10 seconds) defines
how often to log a message about time left to PVC deletion for both reasons.
