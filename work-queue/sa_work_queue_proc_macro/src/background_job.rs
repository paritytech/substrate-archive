use crate::diagnostic_shim::*;
use proc_macro2::TokenStream;
use quote::quote;
use std::borrow::Cow;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;

pub fn expand(item: syn::ItemFn) -> Result<TokenStream, Diagnostic> {
    let job = BackgroundJob::try_from(item)?;

    let attrs = job.attrs;
    let vis = job.visibility;
    let fn_token = job.fn_token;
    let name = job.name;
    let env_pat = &job.args.env_arg.pat;
    let env_type = &job.args.env_arg.ty;
    let connection_arg = &job.args.connection_arg;
    let pool_pat = connection_arg.pool_pat();
    let pool_ty = connection_arg.pool_ty();
    let fn_args = job.args.iter();
    let struct_def = job.args.struct_def();
    let struct_assign = job.args.struct_assign();
    let arg_names_0 = job.args.names();
    let return_type = job.return_type;
    let body = connection_arg.wrap(job.body);
    let (impl_generics, ty_generics, where_clause) = job.generics.split_for_impl();

    // FIXME: this proc-macro needs some love ...
    // I should probably split the `Job Trait` into `Async Job` and `Sync Job`
    // Or leave the Job trait as it is and create two proc macros, one for async and one for sync
    // this if-else chain is ugly.
    // Or maybe there is a better way to accomplish this with proc-macros...
    let res = if job.generics_exist {
        quote! {
            #(#attrs)*
            #vis #fn_token #name #impl_generics (#(#fn_args),*) -> #name :: Job #ty_generics #where_clause {
                #name :: Job {
                    #(#struct_assign),*
                }
            }

            #[coil::async_trait::async_trait]
            impl #impl_generics coil::Job for #name :: Job #ty_generics #where_clause {
                type Environment = #env_type;
                const JOB_TYPE: &'static str = stringify!(#name);

                #fn_token perform(self, #env_pat: &Self::Environment, #pool_pat: &#pool_ty) #return_type {
                    let Self { #(#arg_names_0),* } = self;
                    #body
                }
            }

            pub(crate) mod #name {
                use super::*;

                #[derive(coil::Serialize, coil::Deserialize)]
                #[serde(crate = "coil::serde")]
                pub struct Job #ty_generics {
                    #(#struct_def),*
                }
            }
        }
    } else {
        quote! {
            #(#attrs)*
            #vis #fn_token #name (#(#fn_args),*) -> #name :: Job {
                #name :: Job {
                    #(#struct_assign),*
                }
            }

            #[coil::async_trait::async_trait]
            impl coil::Job for #name :: Job {
                type Environment = #env_type;
                const JOB_TYPE: &'static str = stringify!(#name);

                #fn_token perform(self, #env_pat: &Self::Environment, #pool_pat: &#pool_ty) #return_type {
                    let Self { #(#arg_names_0),* } = self;
                    #body
                }
            }

            pub(crate) mod #name {
                use super::*;

                #[derive(coil::Serialize, coil::Deserialize)]
                #[serde(crate = "coil::serde")]
                pub struct Job {
                    #(#struct_def),*
                }

                coil::register_job!(Job);
            }
        }
    };

    Ok(res)
}

struct BackgroundJob {
    attrs: Vec<syn::Attribute>,
    visibility: syn::Visibility,
    fn_token: syn::Token![fn],
    name: syn::Ident,
    args: JobArgs,
    return_type: syn::ReturnType,
    body: Vec<syn::Stmt>,
    generics: syn::Generics,
    generics_exist: bool,
}

impl BackgroundJob {
    fn try_from(item: syn::ItemFn) -> Result<Self, Diagnostic> {
        let syn::ItemFn {
            attrs,
            vis,
            sig,
            block,
        } = item;

        let mut generics_exist = false;
        if let Some(constness) = sig.constness {
            return Err(constness
                .span
                .error("#[coil::background_job] cannot be used on const functions"));
        }

        if let Some(unsafety) = sig.unsafety {
            return Err(unsafety
                .span
                .error("#[coil::background_job] cannot be used on unsafe functions"));
        }

        if let Some(abi) = sig.abi {
            return Err(abi
                .span()
                .error("#[coil::background_job] cannot be used on functions with an abi"));
        }

        if !sig.generics.params.is_empty() {
            generics_exist = true;
        }
        /*
            if let Some(where_clause) = sig.generics.where_clause {
                return Err(where_clause.where_token.span.error(
                    "#[coil::background_job] cannot be used on functions with a where clause",
                ));
            }
        */
        let fn_token = sig.fn_token;
        let return_type = sig.output.clone();
        let ident = sig.ident.clone();
        let generics = sig.generics.clone();
        let job_args = JobArgs::try_from(sig)?;

        Ok(Self {
            attrs,
            visibility: vis,
            fn_token,
            name: ident,
            args: job_args,
            return_type,
            body: block.stmts,
            generics,
            generics_exist,
        })
    }
}

struct JobArgs {
    env_arg: EnvArg,
    connection_arg: ConnectionArg,
    args: Punctuated<syn::PatType, syn::Token![,]>,
}

impl JobArgs {
    fn iter(&self) -> <&Self as IntoIterator>::IntoIter {
        self.into_iter()
    }

    fn try_from(decl: syn::Signature) -> Result<Self, Diagnostic> {
        let mut env_arg = None;
        let mut connection_arg = ConnectionArg::None;
        let mut args = Punctuated::new();

        for fn_arg in decl.inputs {
            let pat_type = match fn_arg {
                syn::FnArg::Receiver(..) => {
                    return Err(fn_arg.span().error("Background jobs cannot take self"));
                }
                syn::FnArg::Typed(pat_type) => pat_type,
            };

            if let syn::Pat::Ident(syn::PatIdent {
                by_ref: None,
                subpat: None,
                ..
            }) = *pat_type.pat
            {
                // ok
            } else {
                return Err(pat_type
                    .pat
                    .span()
                    .error("#[coil::background_job] cannot yet handle patterns"));
            }

            let span = pat_type.span();
            match (&env_arg, &connection_arg, Arg::try_from(pat_type)?) {
                (None, _, Arg::Env(arg)) => env_arg = Some(arg),
                (Some(_), _, Arg::Env(_)) => {
                    return Err(
                        span.error("Background jobs cannot take references as arguments")
                            .help("If this argument is a database connection, the type must be `&PgConnection`")
                    );
                }
                (_, ConnectionArg::None, Arg::Connection(arg)) => connection_arg = arg,
                (_, _, Arg::Connection(_)) => {
                    return Err(
                        span.error("Multiple database connection arguments")
                            .help("To take a connection pool as an argument instead of a single connection, use the type `&dyn coil::db::DieselPoolObj`")
                    );
                }
                (_, _, Arg::Normal(pat_type)) => args.push(pat_type),
            }
        }

        Ok(Self {
            env_arg: env_arg.unwrap_or_default(),
            connection_arg,
            args,
        })
    }

    fn struct_def(&self) -> impl Iterator<Item = proc_macro2::TokenStream> + '_ {
        self.args.iter().map(|arg| quote::quote!(pub(super) #arg))
    }

    fn struct_assign(&self) -> impl Iterator<Item = syn::FieldValue> + '_ {
        self.names().map(|ident| syn::parse_quote!(#ident: #ident))
    }

    fn names(&self) -> impl Iterator<Item = syn::Ident> + '_ {
        self.args.iter().map(|arg| match &*arg.pat {
            syn::Pat::Ident(pat_ident) => pat_ident.ident.clone(),
            _ => unreachable!(),
        })
    }
}

impl<'a> IntoIterator for &'a JobArgs {
    type Item = <&'a Punctuated<syn::PatType, syn::Token![,]> as IntoIterator>::Item;
    type IntoIter = <&'a Punctuated<syn::PatType, syn::Token![,]> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        (&self.args).into_iter()
    }
}

enum Arg {
    Env(EnvArg),
    Connection(ConnectionArg),
    Normal(syn::PatType),
}

impl Arg {
    fn try_from(pat_type: syn::PatType) -> Result<Self, Diagnostic> {
        if let syn::Type::Reference(type_ref) = *pat_type.ty {
            if let Some(mutable) = type_ref.mutability {
                return Err(mutable.span.error("Unexpected `mut`"));
            }
            let pat = pat_type.pat;
            let ty = type_ref.elem;
            if ConnectionArg::is_connection_arg(&ty) {
                Ok(Arg::Connection(ConnectionArg::from_arg(pat, ty)))
            } else {
                Ok(Arg::Env(EnvArg { pat, ty }))
            }
        } else {
            Ok(Arg::Normal(pat_type))
        }
    }
}

struct EnvArg {
    pat: Box<syn::Pat>,
    ty: Box<syn::Type>,
}

impl Default for EnvArg {
    fn default() -> Self {
        Self {
            pat: syn::parse_quote!(_),
            ty: syn::parse_quote!(()),
        }
    }
}

enum ConnectionArg {
    None,
    Pool(Box<syn::Pat>, Box<syn::Type>),
}

impl ConnectionArg {
    fn is_pool(ty: &syn::Type) -> bool {
        if let syn::Type::Path(syn::TypePath { path, .. }) = ty {
            path_ends_with(path, "PgPool")
        } else {
            false
        }
    }

    fn is_connection_arg(ty: &syn::Type) -> bool {
        /* Self::is_single_connection(ty) ||*/
        Self::is_pool(ty)
    }

    fn from_arg(pat: Box<syn::Pat>, ty: Box<syn::Type>) -> Self {
        if Self::is_pool(&ty) {
            ConnectionArg::Pool(pat, ty)
        } else {
            ConnectionArg::None
        }
    }

    fn pool_pat(&self) -> Cow<'_, syn::Pat> {
        match self {
            ConnectionArg::None => Cow::Owned(syn::parse_quote!(_)),
            ConnectionArg::Pool(pat, _) => Cow::Borrowed(pat),
        }
    }

    fn pool_ty(&self) -> Cow<'_, syn::Type> {
        if let ConnectionArg::Pool(_, ty) = self {
            Cow::Borrowed(ty)
        } else {
            Cow::Owned(syn::parse_quote!(sqlx::PgPool))
        }
    }

    fn wrap(&self, body: Vec<syn::Stmt>) -> TokenStream {
        let body = quote!(#(#body)*);
        // can wrap conection if we want to do fancy stuff in the future
        body
    }
}

fn path_ends_with(path: &syn::Path, needle: &str) -> bool {
    path.segments
        .last()
        .map(|s| s.arguments.is_empty() && s.ident == needle)
        .unwrap_or(false)
}
