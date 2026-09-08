# creates the kubernetes cluster
cluster:
    kind get clusters 2>/dev/null | grep -qx pdf-extract || kind create cluster --config kind-config.yaml

# builds docker images and loads them
images: cluster
    nix build -o result/docker-api '.#docker-api'
    nix build -o result/docker-worker '.#docker-worker'
    kind load image-archive result/docker-api -n pdf-extract
    kind load image-archive result/docker-worker -n pdf-extract

workload restart="false": cluster images
    #!/usr/bin/env bash
    set -euo pipefail
    kubectl apply -f manifests/
    if [ "{{ restart }}" = "true" ]; then
      echo "restarting"
      kubectl rollout restart deploy/api
      kubectl rollout restart deploy/cache
      kubectl rollout restart deploy/garage
      kubectl rollout restart statefulset/worker
    fi

debug restart="false": cluster images workload
    #!/usr/bin/env bash
    set -euo pipefail
    kubectl apply -f debug/
    if [ "{{ restart }}" = "true" ]; then
      kubectl rollout restart deploy/api
      kubectl rollout restart deploy/cache
      kubectl rollout restart deploy/garage
      kubectl rollout restart statefulset/worker
    fi

