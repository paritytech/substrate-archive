#![deny(warnings)]
#![recursion_limit = "128"]
#![cfg_attr(feature = "nightly", feature(proc_macro_diagnostic))]

extern crate proc_macro;

mod background_job;
mod diagnostic_shim;

use proc_macro::TokenStream;
use proc_macro2::Span;
use syn::{parse_macro_input, ItemFn};

use diagnostic_shim::*;

/// The attribute macro for creating background jobs.
///
/// # Examples
///
/// ```ignore
/// // I cant be asynchronous because I'll block the executor
/// #[background_job]
/// fn perform_heavy_computation(foo: HeavyComputation) -> Result<(), PerformError> {
///     foo.compute()?;
///     Ok(())
/// }
/// ````
///
/// ```ignore
/// #[background_job]
/// async fn crawl_for_new_info(website: Website) -> Result<(), PerformError> {
///     let content = website.get_content().await?;
///     content.modify().send_to_actor_pipeline();
/// }
/// ````
#[proc_macro_attribute]
pub fn background_job(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(
            Span::call_site(),
            "coil::background_job does not take arguments",
        )
        .to_compile_error()
        .into();
    }

    let item = parse_macro_input!(item as ItemFn);
    emit_errors(background_job::expand(item))
}

fn emit_errors(result: Result<proc_macro2::TokenStream, Diagnostic>) -> TokenStream {
    result
        .map(Into::into)
        .unwrap_or_else(|e| e.to_compile_error().into())
}
