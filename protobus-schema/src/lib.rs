//! Compiles protobus schemas.
//!
//! A protobus schema is an ordinary `.proto` file shared verbatim by every
//! port. It may use the custom types (`bigint`, `timestamp`, and any declared
//! with [`CustomType`]) as field types without importing them: this crate adds
//! the import each use implies, then compiles the files with
//! [protox](https://docs.rs/protox), so no `protoc` is needed.
//!
//! The runtime (`protobus`, for schemas loaded as text) and the code generator
//! (`protobus-build`) both compile through here, so they agree on every schema.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet};
use protox::file::{File, FileResolver};

/// The file declaring the built-in custom types, as schemas import it.
pub const TYPES_PROTO_NAME: &str = "protobus/types.proto";

/// The text of [`TYPES_PROTO_NAME`].
pub const TYPES_PROTO: &str = include_str!("../proto/protobus/types.proto");

/// The built-in custom type names.
pub const BUILTIN_TYPES: [&str; 2] = ["bigint", "timestamp"];

/// The wire type a custom type's single `value` field is declared with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireType {
    Bytes,
    Int64,
    Uint64,
    String,
    Int32,
    Uint32,
    Double,
}

impl WireType {
    /// The proto scalar name: `bytes`, `int64`, ...
    pub fn as_str(self) -> &'static str {
        match self {
            WireType::Bytes => "bytes",
            WireType::Int64 => "int64",
            WireType::Uint64 => "uint64",
            WireType::String => "string",
            WireType::Int32 => "int32",
            WireType::Uint32 => "uint32",
            WireType::Double => "double",
        }
    }

    pub fn parse(text: &str) -> Option<WireType> {
        Some(match text {
            "bytes" => WireType::Bytes,
            "int64" => WireType::Int64,
            "uint64" => WireType::Uint64,
            "string" => WireType::String,
            "int32" => WireType::Int32,
            "uint32" => WireType::Uint32,
            "double" => WireType::Double,
            _ => return None,
        })
    }
}

/// A custom type of your own: a root-level message
/// `<name> { optional <wire> value = 1; }`, like the built-ins. Every port that
/// shares the schema must declare it the same way.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CustomType {
    pub name: String,
    pub wire: WireType,
}

impl CustomType {
    pub fn new(name: impl Into<String>, wire: WireType) -> Self {
        CustomType {
            name: name.into(),
            wire,
        }
    }

    /// Parse `NAME:WIRE`, as the command line and `build.rs` spell it.
    pub fn parse(spec: &str) -> Result<Self, SchemaError> {
        let (name, wire) = spec
            .split_once(':')
            .ok_or_else(|| SchemaError(format!("custom type '{spec}' is not NAME:WIRE")))?;
        let valid = !name.is_empty()
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && !name.starts_with(|c: char| c.is_ascii_digit());
        if !valid {
            return Err(SchemaError(format!(
                "custom type name '{name}' is not a proto identifier"
            )));
        }
        if BUILTIN_TYPES.contains(&name) {
            return Err(SchemaError(format!("'{name}' is a built-in custom type")));
        }
        let wire = WireType::parse(wire).ok_or_else(|| {
            SchemaError(format!(
                "custom type '{name}' has wire type '{wire}'; use bytes, int64, uint64, string, int32, uint32 or double"
            ))
        })?;
        Ok(CustomType::new(name, wire))
    }

    /// The file this type is declared in, as schemas import it.
    pub fn file_name(&self) -> String {
        format!("protobus/custom/{}.proto", self.name)
    }

    /// The text of [`file_name`](Self::file_name).
    pub fn proto(&self) -> String {
        format!(
            "syntax = \"proto3\";\n\nmessage {} {{\n  optional {} value = 1;\n}}\n",
            self.name,
            self.wire.as_str()
        )
    }
}

/// A schema that could not be read or compiled.
#[derive(Debug, Clone)]
pub struct SchemaError(pub String);

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SchemaError {}

/// One schema file: the name its importers use, and its text.
#[derive(Clone, Debug)]
pub struct Source {
    pub name: String,
    pub text: String,
}

/// Every `.proto` under each directory, recursively, named by its path
/// relative to the directory it was found in (which is what imports refer to).
/// A name found under two directories keeps the first.
pub fn read_dirs<P: AsRef<Path>>(dirs: &[P]) -> Result<Vec<Source>, SchemaError> {
    let mut sources = Vec::new();
    let mut seen = BTreeSet::new();
    for dir in dirs {
        let dir = dir.as_ref();
        if !dir.is_dir() {
            return Err(SchemaError(format!("proto directory {} does not exist", dir.display())));
        }
        let mut found = Vec::new();
        walk(dir, &mut found)?;
        found.sort();
        for path in found {
            let relative = path.strip_prefix(dir).unwrap_or(&path);
            let name = relative
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");
            if !seen.insert(name.clone()) {
                continue;
            }
            let text = std::fs::read_to_string(&path)
                .map_err(|e| SchemaError(format!("could not read {}: {e}", path.display())))?;
            sources.push(Source { name, text });
        }
    }
    Ok(sources)
}

fn walk(dir: &Path, into: &mut Vec<PathBuf>) -> Result<(), SchemaError> {
    let entries = std::fs::read_dir(dir).map_err(|e| SchemaError(format!("could not read {}: {e}", dir.display())))?;
    for entry in entries {
        let path = entry
            .map_err(|e| SchemaError(format!("could not read {}: {e}", dir.display())))?
            .path();
        if path.is_dir() {
            walk(&path, into)?;
        } else if path.extension().is_some_and(|x| x == "proto") {
            into.push(path);
        }
    }
    Ok(())
}

/// Compile every `.proto` under the directories. See [`compile`].
pub fn compile_dirs<P: AsRef<Path>>(dirs: &[P], custom: &[CustomType]) -> Result<FileDescriptorSet, SchemaError> {
    compile(&read_dirs(dirs)?, custom)
}

/// Drop the source locations and comments from a descriptor set: a code
/// generator wants them, a running program does not.
pub fn strip_source_info(set: &mut FileDescriptorSet) {
    for file in &mut set.file {
        file.source_code_info = None;
    }
}

/// Compile schemas into one descriptor set, imports included (the custom
/// types' files among them, when used). A file that uses a custom type without
/// declaring a message of that name gets the import added. The set keeps the
/// schemas' comments; see [`strip_source_info`].
pub fn compile(sources: &[Source], custom: &[CustomType]) -> Result<FileDescriptorSet, SchemaError> {
    // Name -> the file declaring it.
    let mut providers: BTreeMap<String, String> = BTreeMap::new();
    let mut files: BTreeMap<String, FileDescriptorProto> = BTreeMap::new();
    for name in BUILTIN_TYPES {
        providers.insert(name.to_string(), TYPES_PROTO_NAME.to_string());
    }
    files.insert(TYPES_PROTO_NAME.to_string(), parse(TYPES_PROTO_NAME, TYPES_PROTO)?);
    for t in custom {
        if BUILTIN_TYPES.contains(&t.name.as_str()) {
            return Err(SchemaError(format!("'{}' is a built-in custom type", t.name)));
        }
        providers.insert(t.name.clone(), t.file_name());
        files.insert(t.file_name(), parse(&t.file_name(), &t.proto())?);
    }

    let mut names = Vec::new();
    for source in sources {
        if files.contains_key(&source.name) {
            // A checkout that vendors protobus/types.proto: ours is the definition.
            continue;
        }
        let mut file = parse(&source.name, &source.text)?;
        inject_imports(&mut file, &providers);
        names.push(source.name.clone());
        files.insert(source.name.clone(), file);
    }

    let mut compiler = protox::Compiler::with_file_resolver(Resolver { files });
    compiler.include_imports(true);
    compiler.include_source_info(true);
    for name in &names {
        compiler
            .open_file(name)
            .map_err(|e| SchemaError(format!("{name}: {e}")))?;
    }
    Ok(compiler.file_descriptor_set())
}

fn parse(name: &str, text: &str) -> Result<FileDescriptorProto, SchemaError> {
    File::from_source(name, text)
        .map(|f| f.file_descriptor_proto().clone())
        .map_err(|e| SchemaError(format!("{name}: {e}")))
}

/// Add the import of each custom type the file uses and does not declare.
fn inject_imports(file: &mut FileDescriptorProto, providers: &BTreeMap<String, String>) {
    let mut declared = BTreeSet::new();
    let mut used = BTreeSet::new();
    for m in &file.message_type {
        scan(m, &mut declared, &mut used);
    }
    for e in &file.extension {
        if let Some(t) = &e.type_name {
            used.insert(t.trim_start_matches('.').to_string());
        }
    }
    for (name, provider) in providers {
        if !used.contains(name) || declared.contains(name) {
            continue;
        }
        if !file.dependency.iter().any(|d| d == provider) {
            file.dependency.push(provider.clone());
        }
    }
}

fn scan(message: &DescriptorProto, declared: &mut BTreeSet<String>, used: &mut BTreeSet<String>) {
    declared.insert(message.name().to_string());
    for f in &message.field {
        if let Some(t) = &f.type_name {
            used.insert(t.trim_start_matches('.').to_string());
        }
    }
    for n in &message.nested_type {
        scan(n, declared, used);
    }
}

struct Resolver {
    files: BTreeMap<String, FileDescriptorProto>,
}

impl FileResolver for Resolver {
    fn open_file(&self, name: &str) -> Result<File, protox::Error> {
        match self.files.get(name) {
            Some(file) => Ok(File::from_file_descriptor_proto(file.clone())),
            None => Err(protox::Error::file_not_found(name)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(name: &str, text: &str) -> Source {
        Source {
            name: name.into(),
            text: text.into(),
        }
    }

    #[test]
    fn adds_the_import_a_custom_type_implies() {
        let set = compile(
            &[source(
                "wallet.proto",
                "syntax = \"proto3\"; package w; message Balance { bigint amount = 1; map<string, timestamp> at = 2; }",
            )],
            &[],
        )
        .unwrap();
        let names: Vec<_> = set.file.iter().map(|f| f.name().to_string()).collect();
        assert_eq!(names, vec![TYPES_PROTO_NAME, "wallet.proto"]);
        let wallet = &set.file[1];
        assert_eq!(wallet.dependency, vec![TYPES_PROTO_NAME]);
        assert_eq!(wallet.message_type[0].field[0].type_name(), ".bigint");
    }

    #[test]
    fn leaves_a_schema_without_custom_types_alone() {
        let set = compile(
            &[source("a.proto", "syntax = \"proto3\"; message A { int32 a = 1; }")],
            &[],
        )
        .unwrap();
        assert_eq!(set.file.len(), 1);
        assert!(set.file[0].dependency.is_empty());
    }

    #[test]
    fn a_schema_declaring_the_name_itself_keeps_its_own() {
        let set = compile(
            &[source(
                "a.proto",
                "syntax = \"proto3\"; package p; message bigint { string s = 1; } message A { bigint b = 1; }",
            )],
            &[],
        )
        .unwrap();
        assert_eq!(set.file.len(), 1);
        assert_eq!(set.file[0].message_type[1].field[0].type_name(), ".p.bigint");
    }

    #[test]
    fn user_custom_types_are_root_messages() {
        let uuid = CustomType::parse("uuid:string").unwrap();
        let set = compile(
            &[source(
                "a.proto",
                "syntax = \"proto3\"; package p; message A { uuid id = 1; }",
            )],
            &[uuid],
        )
        .unwrap();
        let names: Vec<_> = set.file.iter().map(|f| f.name().to_string()).collect();
        assert_eq!(names, vec!["protobus/custom/uuid.proto", "a.proto"]);
        assert_eq!(set.file[1].message_type[0].field[0].type_name(), ".uuid");
    }

    #[test]
    fn comments_survive_until_stripped() {
        let text = "syntax = \"proto3\";\n// Holds money.\nmessage Wallet { bigint amount = 1; }\n";
        let mut set = compile(&[source("w.proto", text)], &[]).unwrap();
        let info = set.file[1].source_code_info.as_ref().expect("source info is kept");
        assert!(info.location.iter().any(|l| l.leading_comments() == " Holds money.\n"));
        strip_source_info(&mut set);
        assert!(set.file.iter().all(|f| f.source_code_info.is_none()));
    }

    #[test]
    fn refuses_bad_custom_type_specs() {
        assert!(CustomType::parse("uuid").is_err());
        assert!(CustomType::parse("uuid:text").is_err());
        assert!(CustomType::parse("bigint:bytes").is_err());
        assert!(CustomType::parse("9x:bytes").is_err());
    }

    #[test]
    fn imports_between_schemas_resolve_by_relative_name() {
        let set = compile(
            &[
                source(
                    "b/user.proto",
                    "syntax = \"proto3\"; package b; import \"a/id.proto\"; message User { a.Id id = 1; }",
                ),
                source(
                    "a/id.proto",
                    "syntax = \"proto3\"; package a; message Id { string v = 1; }",
                ),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(set.file.len(), 2);
    }

    #[test]
    fn reports_the_file_a_syntax_error_is_in() {
        let err = compile(&[source("bad.proto", "syntax = \"proto3\"; message {")], &[]).unwrap_err();
        assert!(err.to_string().starts_with("bad.proto"), "{err}");
    }
}
