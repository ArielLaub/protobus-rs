//! Calls a service on the bus by name.

use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use prost::Message;
use prost_reflect::DynamicMessage;

use crate::context::Context;
use crate::errors::{Error, ErrorKind, Result};
use crate::message_factory::MessageFactory;
use crate::options::{CallOptions, StreamOptions};
use crate::stream::{Decoder, Stream};
use crate::wire;

struct Resolved {
    contract: String,
    methods: HashSet<String>,
    streaming: HashSet<String>,
}

struct Inner {
    context: Context,
    service_name: String,
    resolved: OnceLock<Resolved>,
}

/// Calls a service on the bus by name.
///
/// Generated `<Service>Proxy` types wrap one with a typed method per rpc. Used
/// directly, it calls methods by name: typed, with dynamic messages, or over
/// raw payload bytes.
///
/// ```no_run
/// # async fn run(ctx: protobus::Context) -> protobus::Result<()> {
/// let calc = protobus::ServiceProxy::new(&ctx, "Calculator.Service");
/// calc.init()?;
/// let mut request = ctx.factory().new_message("Calculator.AddRequest")?;
/// request.set_field_by_name("a", prost_reflect::Value::I32(20));
/// request.set_field_by_name("b", prost_reflect::Value::I32(22));
/// let reply = calc.call("add", &request, &Default::default()).await?;
/// # Ok(()) }
/// ```
///
/// The proxy may be constructed with an instance name (`Combat.Player.player6`)
/// for a service registered under one: requests route to
/// `REQUEST.<instance name>.<method>`, and the envelope names the contract
/// method, which is what the service validates against.
///
/// Cheap to clone, and safe to call from any number of tasks at once.
#[derive(Clone)]
pub struct ServiceProxy {
    inner: Arc<Inner>,
}

impl ServiceProxy {
    pub fn new(context: &Context, service_name: impl Into<String>) -> Self {
        ServiceProxy {
            inner: Arc::new(Inner {
                context: context.clone(),
                service_name: service_name.into(),
                resolved: OnceLock::new(),
            }),
        }
    }

    pub fn service_name(&self) -> &str {
        &self.inner.service_name
    }

    /// The contract the name resolved to, once initialised.
    pub fn contract_service_name(&self) -> Option<&str> {
        self.inner.resolved.get().map(|r| r.contract.as_str())
    }

    pub fn is_initialized(&self) -> bool {
        self.inner.resolved.get().is_some()
    }

    /// The methods the contract declares, once initialised.
    pub fn methods(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .inner
            .resolved
            .get()
            .map(|r| r.methods.iter().cloned().collect())
            .unwrap_or_default();
        names.sort();
        names
    }

    pub fn is_streaming(&self, method: &str) -> bool {
        self.inner.resolved.get().is_some_and(|r| r.streaming.contains(method))
    }

    fn factory(&self) -> &MessageFactory {
        self.inner.context.factory()
    }

    fn resolve_contract(&self) -> Result<String> {
        let name = &self.inner.service_name;
        let mut candidate = name.as_str();
        loop {
            if self.factory().has_service(candidate) {
                // Said out loud, because trimming is a guess.
                if candidate != name {
                    log::info!(
                        target: "protobus",
                        "service proxy '{name}' resolved to contract '{candidate}'; requests will route to REQUEST.{name}.*"
                    );
                }
                return Ok(candidate.to_string());
            }
            match candidate.rfind('.') {
                Some(cut) if cut > 0 => candidate = &candidate[..cut],
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidServiceName,
                        format!(
                            "no service in the schema matches '{name}' or any prefix of it; the schema must declare \
                             the service this proxy addresses"
                        ),
                    ))
                }
            }
        }
    }

    /// Resolve the contract and the methods it declares. Fails when no known
    /// service matches the name or a prefix of it, and on a second call.
    pub fn init(&self) -> Result<()> {
        let already = || {
            log::error!(target: "protobus", "already initialized service proxy {}", self.inner.service_name);
            Error::new(ErrorKind::AlreadyInitialized, "already initialized")
        };
        if self.is_initialized() {
            return Err(already());
        }
        let contract = self.resolve_contract()?;
        let service = self.factory().service(&contract)?;
        let methods = service.methods().map(|m| m.name().to_string()).collect();
        let streaming = service
            .methods()
            .filter(|m| m.is_server_streaming())
            .map(|m| m.name().to_string())
            .collect();
        self.inner
            .resolved
            .set(Resolved {
                contract,
                methods,
                streaming,
            })
            .map_err(|_| already())
    }

    /// The full contract method and the routing key for a call.
    fn require(&self, method: &str, stream: bool) -> Result<(String, String)> {
        let name = &self.inner.service_name;
        let Some(r) = self.inner.resolved.get() else {
            return Err(Error::new(
                ErrorKind::NotInitialized,
                format!("service proxy {name} is not initialized"),
            ));
        };
        if !r.methods.contains(method) {
            return Err(Error::new(
                ErrorKind::UnknownMethod,
                format!("service '{}' declares no method '{method}'", r.contract),
            ));
        }
        if stream != r.streaming.contains(method) {
            return Err(Error::new(
                ErrorKind::InvalidRequest,
                format!(
                    "{}.{method} is {}",
                    r.contract,
                    if stream {
                        "unary; use call()"
                    } else {
                        "server-streaming; use call_stream()"
                    }
                ),
            ));
        }
        Ok((format!("{}.{method}", r.contract), format!("REQUEST.{name}.{method}")))
    }

    /// Encode a typed request, checking it is the method's request type.
    fn encode_typed<Req: Message + prost::Name>(&self, full: &str, request: &Req) -> Result<Vec<u8>> {
        let expected = self.factory().method(full)?.input();
        let actual = Req::full_name();
        if expected.full_name() != actual {
            return Err(Error::new(
                ErrorKind::InvalidRequest,
                format!("the request of {full} must be {}, got {actual}", expected.full_name()),
            ));
        }
        self.factory().encode(request).map_err(|e| match e.kind() {
            ErrorKind::CustomTypeRange => e,
            // Type and error only: the request is application data.
            _ => Error::new(
                ErrorKind::InvalidRequest,
                format!("failed building request for {full}: {e}"),
            ),
        })
    }

    // ---- unary -------------------------------------------------------------------

    /// Call a unary method with a serialized request; returns the serialized
    /// response, or nothing when `rpc` is false. Nothing checks either
    /// payload's type.
    pub async fn call_raw(&self, method: &str, payload: Vec<u8>, options: &CallOptions) -> Result<Vec<u8>> {
        let (full, routing_key) = self.require(method, false)?;
        let content = MessageFactory::build_encoded_request(&full, payload, options.actor.as_deref());
        let reply = self
            .inner
            .context
            .publish_message(content, &routing_key, options)
            .await?;
        match reply {
            Some(reply) => result_payload(&full, &reply),
            None => {
                log::debug!(target: "protobus", "non-rpc call to {full} confirmed");
                Ok(Vec::new())
            }
        }
    }

    /// Call a unary method with generated message types, as generated proxies
    /// do. A service error fails as a remote error; a delivery failure with
    /// its publish error unchanged. With `rpc` false the call completes with a
    /// default response once the request is confirmed.
    pub async fn call_typed<Req, Res>(&self, method: &str, request: &Req, options: &CallOptions) -> Result<Res>
    where
        Req: Message + prost::Name,
        Res: Message + prost::Name + Default,
    {
        let (full, _) = self.require(method, false)?;
        let payload = self.encode_typed(&full, request)?;
        let reply = self.call_raw(method, payload, options).await?;
        if !options.rpc {
            return Ok(Res::default());
        }
        self.factory()
            .decode::<Res>(&reply)
            .map_err(|e| invalid_response(&full, &e))
    }

    /// Call a unary method with a dynamic message of its request type; the
    /// reply is a dynamic message of its response type.
    pub async fn call(&self, method: &str, request: &DynamicMessage, options: &CallOptions) -> Result<DynamicMessage> {
        let (full, _) = self.require(method, false)?;
        let descriptor = self.factory().method(&full)?;
        let payload = self.factory().request_payload(&full, request)?;
        let reply = self.call_raw(method, payload, options).await?;
        if !options.rpc {
            return Ok(DynamicMessage::new(descriptor.output()));
        }
        self.factory()
            .decode_message(descriptor.output().full_name(), &reply)
            .map_err(|e| invalid_response(&full, &e))
    }

    // ---- streaming ---------------------------------------------------------------

    fn open_stream<T>(
        &self,
        method: &str,
        payload: Vec<u8>,
        options: &StreamOptions,
        decode: Decoder<T>,
    ) -> Result<Stream<T>> {
        let (full, routing_key) = self.require(method, true)?;
        let content = MessageFactory::build_encoded_request(&full, payload, options.actor.as_deref());
        let chunks = self
            .inner
            .context
            .publish_streaming_message(content, &routing_key, options)?;
        Ok(Stream::new(chunks, full, decode))
    }

    /// A server-streaming call with a serialized request, yielding serialized chunks.
    pub fn call_stream_raw(&self, method: &str, payload: Vec<u8>, options: &StreamOptions) -> Result<Stream<Vec<u8>>> {
        self.open_stream(method, payload, options, Arc::new(|data| Ok(data.to_vec())))
    }

    /// A server-streaming call with generated message types, as generated
    /// proxies make it.
    pub fn call_stream_typed<Req, Res>(
        &self,
        method: &str,
        request: &Req,
        options: &StreamOptions,
    ) -> Result<Stream<Res>>
    where
        Req: Message + prost::Name,
        Res: Message + prost::Name + Default + 'static,
    {
        let (full, _) = self.require(method, true)?;
        let payload = self.encode_typed(&full, request)?;
        let factory = self.factory().clone();
        self.open_stream(
            method,
            payload,
            options,
            Arc::new(move |data| factory.decode::<Res>(data)),
        )
    }

    /// A server-streaming call with a dynamic message of the request type.
    pub fn call_stream(
        &self,
        method: &str,
        request: &DynamicMessage,
        options: &StreamOptions,
    ) -> Result<Stream<DynamicMessage>> {
        let (full, _) = self.require(method, true)?;
        let descriptor = self.factory().method(&full)?;
        let payload = self.factory().request_payload(&full, request)?;
        let factory = self.factory().clone();
        let output = descriptor.output().full_name().to_string();
        self.open_stream(
            method,
            payload,
            options,
            Arc::new(move |data| factory.decode_message(&output, data)),
        )
    }
}

fn invalid_response(full_method: &str, cause: &Error) -> Error {
    Error::new(
        ErrorKind::InvalidResponse,
        format!("failed parsing result for {full_method}: {cause}"),
    )
}

/// The result payload of a reply, or the service's error as a remote error.
pub(crate) fn result_payload(full_method: &str, reply: &[u8]) -> Result<Vec<u8>> {
    match wire::decode_response(reply) {
        Err(_) => Err(Error::new(
            ErrorKind::InvalidResponse,
            format!("failed parsing result for {full_method}"),
        )),
        Ok(wire::Response::Error(e)) => Err(Error::remote(e.message, &e.code, e.method)),
        Ok(wire::Response::Result(r)) => Ok(r.data),
    }
}
