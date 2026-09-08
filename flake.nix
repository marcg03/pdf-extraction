{
  description = "A flake with a kubernetes devShell";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  inputs.systems.url = "github:nix-systems/default";
  inputs.flake-utils = {
    url = "github:numtide/flake-utils";
    inputs.systems.follows = "systems";
  };

  outputs =
    { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        python = pkgs.python314;
        api = python.pkgs.callPackage ./nix/api.nix { };
        api-app = python.pkgs.toPythonApplication api;
        worker = python.pkgs.callPackage ./nix/worker.nix { };
        worker-app = python.pkgs.toPythonApplication worker;
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            kind
            kubectl
            jq
            valkey
            (python.withPackages (_: api.dependencies ++ worker.dependencies))
            ruff
            just
          ];
          shellHook = ''
            export KIND_EXPERIMENTAL_PROVIDER=podman
            export KUBECONFIG=$PWD/.kube/config
            mkdir -p $PWD/.kube
          '';
        };

        packages.api = api-app;
        packages.worker = worker-app;
        packages.docker-api = pkgs.callPackage ./nix/docker.nix { app = api-app; };
        packages.docker-worker = pkgs.callPackage ./nix/docker.nix { app = worker-app; };
      }
    );
}
