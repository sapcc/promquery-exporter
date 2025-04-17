# PromQuery Exporter
Sonmetimes we need to aggregate metrics from another prometheus system.
This exporter is built to fetch cross region info of snapmirror relationships and re-export it to Maia.

# Example Config
```
metrics:
  - export_name: netapp_snapmirror_labels:maia
    query: |
      netapp_snapmirror_endpoint_labels:enhanced{region="eu-de-1", project_id!="", share_id!="", share_name!=""} * 
        on(source_cluster, source_vserver, source_volume, destination_cluster, destination_vserver, destination_volume) group_right(project_id, share_id, share_name) netapp_snapmirror_labels:enhanced{share_id=""}
    help: "maia netapp snapmirror labels"
  - export_name: netapp_snapmirror_labels:maia
    query: | 
      netapp_snapmirror_labels:enhanced{region="eu-de-1", share_id!="", destination_volume!~"share_[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}"}
    help: "maia netapp snapmirror labels"
label_rules:
  - type: drop
    label_name: namespace
  - type: drop
    label_name: pod
  - type: drop
    label_name: app
  - type: drop
    label_name: name
  - type: drop
    label_name: container
  - type: drop
    label_name: prometheus
  - type: drop
    label_prefix: linkerd_
  - type: drop
    label_prefix: kubernetes_
  - type: drop
    label_prefix: pod_
  - type: fill
    label_name: unhealthy_reason
```
