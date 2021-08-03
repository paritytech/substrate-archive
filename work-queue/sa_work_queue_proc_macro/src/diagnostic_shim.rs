use proc_macro2::{Span, TokenStream};

pub trait DiagnosticShim {
    fn error<T: Into<String>>(self, msg: T) -> Diagnostic;
}

#[cfg(feature = "nightly")]
impl DiagnosticShim for Span {
    fn error<T: Into<String>>(self, msg: T) -> Diagnostic {
        self.unstable().error(msg)
    }
}

#[cfg(not(feature = "nightly"))]
impl DiagnosticShim for Span {
    fn error<T: Into<String>>(self, msg: T) -> Diagnostic {
        Diagnostic::error(self, msg)
    }
}

#[cfg(feature = "nightly")]
pub use proc_macro::Diagnostic;

#[cfg(not(feature = "nightly"))]
pub struct Diagnostic {
    span: Span,
    message: String,
}

#[cfg(not(feature = "nightly"))]
impl Diagnostic {
    fn error<T: Into<String>>(span: Span, msg: T) -> Self {
        Diagnostic {
            span,
            message: msg.into(),
        }
    }

    pub(crate) fn help<T: Into<String>>(mut self, msg: T) -> Self {
        self.message += &format!("\nhelp: {}", msg.into());
        self
    }
}

pub trait DiagnosticExt {
    fn to_compile_error(self) -> TokenStream;
}

#[cfg(feature = "nightly")]
impl DiagnosticExt for Diagnostic {
    fn to_compile_error(self) -> TokenStream {
        self.emit();
        "".parse().unwrap()
    }
}

#[cfg(not(feature = "nightly"))]
impl DiagnosticExt for Diagnostic {
    fn to_compile_error(self) -> TokenStream {
        syn::Error::new(self.span, self.message).to_compile_error()
    }
}
