let
  moz_overlay = import (builtins.fetchTarball https://github.com/mozilla/nixpkgs-mozilla/archive/master.tar.gz);
  pkgs = import <nixpkgs> { overlays = [ moz_overlay ]; };
  channel = pkgs.rustChannelOf {
     channel = "nightly";
  };
  rust = (channel.rust.override {
    targets = [ "wasm32-unknown-unknown" ];
    extensions = ["rust-src"];
  });
in
  with pkgs;
  stdenv.mkDerivation {
    name = "rust-dev-env";
    buildInputs = [
      rust
      llvmPackages.libclang
      llvmPackages.libcxxClang
      clang
      pkg-config openssl
    ];
    LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
    BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${llvmPackages.libclang.lib}/lib/clang/${lib.getVersion clang}/include";
}
