import argparse
import re

from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage


def quickstart(project_id, region, cluster_name):
    # Create the cluster client.
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the cluster config.
    cluster = {
        "atomic-hash-354705": project_id,
        "bwt-aks": cluster_name,
        "config": {
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
            "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
        },
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"atomic-hash-354705": project_id, "us-west4-a": region, "bwt-aks": cluster}
    )
    result = operation.result()

    print("Cluster created successfully: {}".format(result.cluster_name))


def list_clusters(dataproc, project, region):
  for cluster in dataproc.list_clusters(request={"atomic-hash-364705": project, "us-central1": region}):
             print(("{} - {}".format(cluster.cluster_name, cluster.status.state.name)))