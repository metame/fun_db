{
  description = "Example of a project using nix and riot";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = inputs@{ self, flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin" ];
      perSystem = { config, self', inputs', pkgs, system, ... }:
        let
          inherit (pkgs) ocamlPackages mkShell lib;
        in
          {

            ## it's unlikely devShells will need changes
            devShells = {
              default = mkShell {
                inputsFrom = [
                  ## adds all the propagatedBuildInputs below into the shell
                  self'.packages.default
                ];
                ## buildInputs defines packages you need at dev time, not build time
                buildInputs = with ocamlPackages; [
                  dune_3
                  ocaml
                  utop
                  ocamlformat
                ];
                packages = builtins.attrValues {
                  inherit (ocamlPackages) ocaml-lsp ocamlformat-rpc-lib;
                };
              };
            };
            packages = {
              ## unless this is a monorepo, there's only a single package named "default"
              ## NOTE: the serde flake is a good example of how to build up a monorepo
              ## https://github.com/serde-ml/serde/blob/main/flake.nix
              default = ocamlPackages.buildDunePackage {
                version = "dev";
                ## this needs to match the package name in dune-project
                pname = "fun_db";
                propagatedBuildInputs = with ocamlPackages; [
                  ## For deps in ocamlPackages (the typical case):
                  ## use the same name as in opam/dune-project.
                  ## No versions are specified, the version fetched/built is based on
                  ## the version of nixpkgs specified above.
                  ## You can search on https://search.nixos.org/
                  ## to find the version of odoc in nixpkgs use query ocamlPackages.odoc
                ];
                src = ./.;
              };
            };
          };
    };
}
