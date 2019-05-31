# EphemeralVolumeClaim operator

A Kubernetes operator for `EphemeralVolumeClaim` custom resources,
which internally map to the same-named, same-labelled `PersistentVolumeClaim`,
but automatically delete themselves after a period of no use.

This is a demo project used as a step-by-step tutorial for
[Kopf, Kubernetes Operator Pythonic Framework](https://github.com/zalando-incubator/kopf).

The problem statement is described with more details here:

* https://kopf.readthedocs.io/en/stable/walkthrough/problem/
