# PDF Extraction Service

This project contains code and configuration to deploy a text extraction service
for PDFs in a kubernetes cluster.

## Usage

> *Note*: [nix](https://github.com/NixOS/nix) is used to make building the
> Python applications and container images reproducible.

Steps to run the service:
- `nix develop` (or use [direnv](https://github.com/direnv/direnv))
- `just workload`
- `kubectl port-forward svc/api 8888:http`

For example, you can use one of the commands below to test the service:
- `curl -F "file=@<path-to-file>" localhost:8888/queue-extraction-job`
- `curl -F "file=@$HOME/$(cd; nix run 'nixpkgs#fzf')" localhost:8888/queue-extraction-job`
