# Tracing binaries for WASM

Included are binaries that are compiled to WASM with tracing enabled for Kusama, Westend, and Polkadot.


## Usage with Polkadot Archive

- outline the targets you want to trace in your TOML configuration file
```toml
wasm_tracing_targets = 'support,executive,frame_executive,frame_support'
```
- pass `polkadot-archive` the `---wasm_runtime_overrides` flag with the path to this folder
that contains the WASM binaries for the chain that you want to index.

For instance, if I wanted to run tracing against Westend:
```bash
./polkadot-archive -c my_config.toml --chain=westend --wasm_runtime_overrides ./wasm-tracing/westend/
```


## Currently Supported Versions across Polkadot/Kusama/Westend
- [x] 0.9.3
- [x] 0.9.2
- [x] 0.9.1
- [x] 0.9.0
- [x] 0.8.30
- [x] 0.8.29
- [x] 0.8.28
- [x] 0.8.27
- [x] 0.8.26-1
- [x] 0.8.25

