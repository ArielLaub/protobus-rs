//! The schemas a context knows, and the encoding of requests, replies and
//! events.
//!
//! Three kinds of schema are visible through one factory: generated code
//! registers the descriptor set compiled into the program, so a program built
//! with `protobus-build` needs nothing loaded at runtime; `.proto` files can
//! be loaded as text (the `proto-text` feature, on by default); and compiled
//! descriptor sets can be loaded from files. The dynamic API
//! ([`ServiceProxy::call`](crate::ServiceProxy::call), dynamic handlers)
//! decodes into [`DynamicMessage`]s of the factory's types.

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use prost::Message;
use prost_reflect::{
    DescriptorPool, DynamicMessage, MessageDescriptor, MethodDescriptor, ReflectMessage, ServiceDescriptor,
};
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet, OneofDescriptorProto,
};

use crate::custom_types;
use crate::errors::{Error, ErrorKind, Result};
use crate::wire;

/// The file declaring the built-in custom types.
pub const TYPES_PROTO_NAME: &str = "protobus/types.proto";

/// The file extensions [`MessageFactory::load`] takes for descriptor sets.
pub const DESCRIPTOR_SET_EXTENSIONS: [&str; 4] = ["binpb", "desc", "pb", "protoset"];

fn custom_type_message(name: &str, wire: Type) -> DescriptorProto {
    DescriptorProto {
        name: Some(name.into()),
        field: vec![FieldDescriptorProto {
            name: Some("value".into()),
            number: Some(1),
            label: Some(Label::Optional as i32),
            r#type: Some(wire as i32),
            json_name: Some("value".into()),
            oneof_index: Some(0),
            proto3_optional: Some(true),
            ..Default::default()
        }],
        oneof_decl: vec![OneofDescriptorProto {
            name: Some("_value".into()),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// `protobus/types.proto`, as a descriptor.
fn types_file() -> FileDescriptorProto {
    FileDescriptorProto {
        name: Some(TYPES_PROTO_NAME.into()),
        syntax: Some("proto3".into()),
        message_type: vec![
            custom_type_message("bigint", Type::Bytes),
            custom_type_message("timestamp", Type::Int64),
        ],
        ..Default::default()
    }
}

/// A pool holding only the built-in custom types.
pub(crate) fn builtin_pool() -> DescriptorPool {
    let mut pool = DescriptorPool::new();
    pool.add_file_descriptor_proto(types_file())
        .expect("the built-in custom types are a valid schema");
    pool
}

fn schema_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Schema, message)
}

/// The schemas a context knows. Cheap to clone: every clone is the same
/// factory. Thread-safe.
#[derive(Clone)]
pub struct MessageFactory {
    pool: Arc<RwLock<DescriptorPool>>,
}

impl Default for MessageFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageFactory {
    pub fn new() -> Self {
        MessageFactory {
            pool: Arc::new(RwLock::new(builtin_pool())),
        }
    }

    /// A snapshot of the descriptor pool.
    pub fn pool(&self) -> DescriptorPool {
        self.pool.read().unwrap().clone()
    }

    // ---- loading -----------------------------------------------------------------

    /// Register a serialized `FileDescriptorSet`, as generated code does with
    /// the one compiled into the program. It must include the imports of the
    /// files it carries, except files already registered, such as
    /// `protobus/types.proto`, which is always known. Idempotent per file
    /// name: a file already registered keeps its descriptor.
    pub fn register_descriptor_set(&self, serialized: &[u8]) -> Result<()> {
        let set =
            FileDescriptorSet::decode(serialized).map_err(|e| schema_error(format!("not a FileDescriptorSet: {e}")))?;
        self.register_files(set)
    }

    /// Register a `FileDescriptorSet`.
    pub fn register_files(&self, set: FileDescriptorSet) -> Result<()> {
        // Built on a copy, so a set that fails leaves the factory as it was.
        let mut pool = self.pool();
        let fresh: Vec<FileDescriptorProto> = set
            .file
            .into_iter()
            .filter(|f| pool.get_file_by_name(f.name()).is_none())
            .collect();
        if fresh.is_empty() {
            return Ok(());
        }
        let names: Vec<String> = fresh.iter().map(|f| f.name().to_string()).collect();
        pool.add_file_descriptor_protos(fresh)
            .map_err(|e| schema_error(format!("invalid schema: {e}")))?;
        *self.pool.write().unwrap() = pool;
        log::debug!(target: "protobus", "registered schema {}", names.join(", "));
        Ok(())
    }

    /// Load schemas for the dynamic API from each location: a directory of
    /// `.proto` files (recursively; a file is named by its path relative to
    /// the directory, which is what its imports refer to), a directory or file
    /// of compiled descriptor sets ([`DESCRIPTOR_SET_EXTENSIONS`]), or a single
    /// `.proto` file. A schema that uses `bigint` or `timestamp` without
    /// importing them gets the import added, as in every port.
    pub fn load<P: AsRef<Path>>(&self, locations: &[P]) -> Result<()> {
        let mut proto_dirs: Vec<PathBuf> = Vec::new();
        let mut descriptor_sets: Vec<PathBuf> = Vec::new();
        for location in locations {
            let location = location.as_ref();
            if location.is_dir() {
                let mut found = Vec::new();
                collect_files(location, &mut found)?;
                found.sort();
                if found.iter().any(|f| has_extension(f, &["proto"])) {
                    proto_dirs.push(location.to_path_buf());
                }
                descriptor_sets.extend(
                    found
                        .into_iter()
                        .filter(|f| has_extension(f, &DESCRIPTOR_SET_EXTENSIONS)),
                );
            } else if location.is_file() {
                if has_extension(location, &["proto"]) {
                    self.load_proto_file(location)?;
                } else {
                    descriptor_sets.push(location.to_path_buf());
                }
            } else {
                return Err(schema_error(format!(
                    "schema location {} does not exist",
                    location.display()
                )));
            }
        }
        if !proto_dirs.is_empty() {
            self.load_proto_dirs(&proto_dirs)?;
        }
        for path in descriptor_sets {
            let bytes = std::fs::read(&path)
                .map_err(|e| schema_error(format!("could not read descriptor set {}: {e}", path.display())))?;
            self.register_descriptor_set(&bytes)?;
        }
        Ok(())
    }

    #[cfg(feature = "proto-text")]
    fn load_proto_dirs(&self, dirs: &[PathBuf]) -> Result<()> {
        log::info!(target: "protobus", "loading .proto files from {} location(s)", dirs.len());
        let mut set = protobus_schema::compile_dirs(dirs, &[])?;
        protobus_schema::strip_source_info(&mut set);
        self.register_files(set)
    }

    #[cfg(feature = "proto-text")]
    fn load_proto_file(&self, path: &Path) -> Result<()> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| schema_error(format!("could not read {}: {e}", path.display())))?;
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        self.parse(&text, &name)
    }

    #[cfg(not(feature = "proto-text"))]
    fn load_proto_dirs(&self, dirs: &[PathBuf]) -> Result<()> {
        Err(schema_error(format!(
            "{} holds .proto files; loading them as text needs protobus's `proto-text` feature. \
             Load a compiled descriptor set instead.",
            dirs[0].display()
        )))
    }

    #[cfg(not(feature = "proto-text"))]
    fn load_proto_file(&self, path: &Path) -> Result<()> {
        self.load_proto_dirs(&[path.to_path_buf()])
    }

    /// Add a schema from its text. `name` is what other schemas would import
    /// it as. Idempotent per name.
    #[cfg(feature = "proto-text")]
    pub fn parse(&self, proto: &str, name: &str) -> Result<()> {
        let source = protobus_schema::Source {
            name: name.to_string(),
            text: proto.to_string(),
        };
        let mut set = protobus_schema::compile(&[source], &[])?;
        protobus_schema::strip_source_info(&mut set);
        self.register_files(set)
    }

    // ---- lookups -----------------------------------------------------------------

    pub fn has_service(&self, full_name: &str) -> bool {
        self.pool.read().unwrap().get_service_by_name(full_name).is_some()
    }

    pub fn has_type(&self, full_name: &str) -> bool {
        self.pool.read().unwrap().get_message_by_name(full_name).is_some()
    }

    pub fn service(&self, full_name: &str) -> Result<ServiceDescriptor> {
        self.pool
            .read()
            .unwrap()
            .get_service_by_name(full_name)
            .ok_or_else(|| Error::new(ErrorKind::InvalidServiceName, format!("no such service {full_name}")))
    }

    pub fn message_type(&self, full_name: &str) -> Result<MessageDescriptor> {
        self.pool
            .read()
            .unwrap()
            .get_message_by_name(full_name)
            .ok_or_else(|| schema_error(format!("no such message type {full_name}")))
    }

    /// Method names a service declares, in declaration order.
    pub fn service_method_names(&self, service_full_name: &str) -> Result<Vec<String>> {
        Ok(self
            .service(service_full_name)?
            .methods()
            .map(|m| m.name().to_string())
            .collect())
    }

    /// Split `<package>.<Service>.<method>` from the right, so a dotted
    /// package splits into service `com.example.Calc` and method `add`.
    pub fn split_method_name(full_name: &str) -> Result<(&str, &str)> {
        match full_name.rfind('.') {
            Some(i) if i > 0 && i < full_name.len() - 1 => Ok((&full_name[..i], &full_name[i + 1..])),
            _ => Err(Error::new(
                ErrorKind::InvalidMethodName,
                format!("'{full_name}' is not a fully-qualified method name (<package>.<Service>.<method>)"),
            )),
        }
    }

    pub fn method(&self, full_name: &str) -> Result<MethodDescriptor> {
        let (service_name, method_name) = Self::split_method_name(full_name)?;
        let unknown = |what: String| Error::new(ErrorKind::UnknownMethod, what);
        let service = self
            .pool
            .read()
            .unwrap()
            .get_service_by_name(service_name)
            .ok_or_else(|| unknown(format!("no such service '{service_name}'")))?;
        let found = service.methods().find(|m| m.name() == method_name);
        found.ok_or_else(|| unknown(format!("service '{service_name}' declares no method '{method_name}'")))
    }

    /// Whether the method is declared server-streaming. Unknown methods are
    /// treated as unary.
    pub fn is_streaming_method(&self, full_name: &str) -> bool {
        self.method(full_name).map(|m| m.is_server_streaming()).unwrap_or(false)
    }

    // ---- messages ----------------------------------------------------------------

    /// A new, empty dynamic message of a type the factory knows.
    pub fn new_message(&self, type_full_name: &str) -> Result<DynamicMessage> {
        Ok(DynamicMessage::new(self.message_type(type_full_name)?))
    }

    /// Check the custom-type values in an encoded message of the named type. A
    /// type the factory does not know is not checked.
    pub fn validate(&self, type_full_name: &str, encoded: &[u8]) -> Result<()> {
        let descriptor = self.pool.read().unwrap().get_message_by_name(type_full_name);
        match descriptor {
            Some(d) => custom_types::validate_encoded(&d, encoded),
            None => Ok(()),
        }
    }

    /// Serialize a typed message after checking its custom-type values.
    pub fn encode<T: Message + prost::Name>(&self, message: &T) -> Result<Vec<u8>> {
        let encoded = message.encode_to_vec();
        self.validate(&T::full_name(), &encoded)?;
        Ok(encoded)
    }

    /// Decode a typed message, checking its custom-type values.
    pub fn decode<T: Message + prost::Name + Default>(&self, data: &[u8]) -> Result<T> {
        let name = T::full_name();
        self.validate(&name, data)?;
        T::decode(data).map_err(|e| Error::new(ErrorKind::InvalidMessage, format!("{name} did not decode: {e}")))
    }

    /// Serialize a dynamic message after checking its custom-type values.
    pub fn encode_message(&self, message: &DynamicMessage) -> Result<Vec<u8>> {
        let encoded = message.encode_to_vec();
        custom_types::validate_encoded(&message.descriptor(), &encoded)?;
        Ok(encoded)
    }

    /// Decode a dynamic message of the named type, checking its custom-type values.
    pub fn decode_message(&self, type_full_name: &str, data: &[u8]) -> Result<DynamicMessage> {
        let descriptor = self.message_type(type_full_name)?;
        custom_types::validate_encoded(&descriptor, data)?;
        DynamicMessage::decode(descriptor, data).map_err(|e| {
            Error::new(
                ErrorKind::InvalidMessage,
                format!("{type_full_name} did not decode: {e}"),
            )
        })
    }

    // ---- envelopes ---------------------------------------------------------------

    /// Encode a request around an already-serialized payload.
    pub fn build_encoded_request(method_full_name: &str, payload: Vec<u8>, actor: Option<&str>) -> Vec<u8> {
        wire::encode_request(&wire::Request {
            method: method_full_name.to_string(),
            actor: actor.map(str::to_string),
            data: payload,
        })
    }

    /// Serialize a dynamic message as the payload of a request, checking it is
    /// of the method's request type.
    pub fn request_payload(&self, method_full_name: &str, request: &DynamicMessage) -> Result<Vec<u8>> {
        let method = self.method(method_full_name)?;
        let (expected, actual) = (method.input(), request.descriptor());
        if expected.full_name() != actual.full_name() {
            return Err(Error::new(
                ErrorKind::InvalidRequest,
                format!(
                    "the request of {method_full_name} must be {}, got {}",
                    expected.full_name(),
                    actual.full_name()
                ),
            ));
        }
        self.encode_message(request)
    }

    /// Encode a request from a dynamic message, which must be of the method's
    /// request type.
    pub fn build_request(
        &self,
        method_full_name: &str,
        request: &DynamicMessage,
        actor: Option<&str>,
    ) -> Result<Vec<u8>> {
        Ok(Self::build_encoded_request(
            method_full_name,
            self.request_payload(method_full_name, request)?,
            actor,
        ))
    }

    /// Encode a successful reply whose payload is already serialized.
    pub fn build_encoded_response(method_full_name: &str, payload: Vec<u8>) -> Vec<u8> {
        wire::encode_response(&wire::Response::Result(wire::ResponseResult {
            method: method_full_name.to_string(),
            data: payload,
        }))
    }

    /// Encode an error reply. No method lookup: an error carries the method
    /// only as a label, so a failure that is about an unknown method can still
    /// be reported.
    pub fn build_error_response(method_full_name: &str, error: &Error) -> Vec<u8> {
        wire::encode_response(&wire::Response::Error(wire::ResponseError {
            method: method_full_name.to_string(),
            message: error.describe().to_string(),
            code: error.code().unwrap_or("").to_string(),
        }))
    }

    /// Encode an event around an already-serialized payload; `type` is the
    /// payload's full message name.
    pub fn build_encoded_event(r#type: &str, payload: Vec<u8>, topic: &str) -> Vec<u8> {
        wire::encode_event(&wire::Event {
            r#type: r#type.to_string(),
            topic: topic.to_string(),
            data: payload,
        })
    }
}

fn has_extension(path: &Path, extensions: &[&str]) -> bool {
    path.extension()
        .and_then(|x| x.to_str())
        .is_some_and(|x| extensions.contains(&x))
}

fn collect_files(dir: &Path, into: &mut Vec<PathBuf>) -> Result<()> {
    let entries = std::fs::read_dir(dir).map_err(|e| schema_error(format!("could not read {}: {e}", dir.display())))?;
    for entry in entries {
        let path = entry
            .map_err(|e| schema_error(format!("could not read {}: {e}", dir.display())))?
            .path();
        if path.is_dir() {
            collect_files(&path, into)?;
        } else {
            into.push(path);
        }
    }
    Ok(())
}
