#![cfg_attr(not(feature = "std"), no_std)]

// Make the WASM binary available.
#[cfg(feature = "std")]
include!(concat!(env!("OUT_DIR"), "/wasm_binary.rs"));

/// Wasm binary unwrapped. If built with `SKIP_WASM_BUILD`, the function panics.
#[cfg(feature = "std")]
pub fn wasm_binary_unwrap() -> &'static [u8] {
	WASM_BINARY.expect(
		"Development wasm binary is not available. Testing is only \
						supported with the flag disabled.",
	)
}

#[cfg(not(feature = "std"))]
use sp_io::wasm_tracing;

#[cfg(not(feature = "std"))]
use sp_runtime::print;

sp_core::wasm_export_functions! {
	fn test_trace_handler() {
		sp_io::init_tracing();
		let id = wasm_tracing::enter_span(Default::default());
		print("Tracing!");
		wasm_funcs::call_events_and_spans();
		wasm_tracing::exit(id);
	}
}

// check marks it as unused because we call these functions from the resulting wasm blob
#[allow(unused)]
mod wasm_funcs {
	pub fn call_events_and_spans() {
		generate_events();
		generate_spans();
	}

	fn generate_events() {
		tracing::event!(
			target: "test_wasm",
			tracing::Level::INFO,
			"im_an_event",
		);
	}

	fn generate_spans() {
		tracing::info_span!(
			target: "test_wasm",
			"im_a_span",
			some_info = "some_pertinent_information"
		);
		{
			nested_function();
		}
	}

	fn nested_function() {
		tracing::info_span!(
			target: "test_wasm",
			"im_a_nested_span",
		);
	}
}
