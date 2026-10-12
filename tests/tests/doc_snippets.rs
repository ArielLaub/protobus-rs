//! Compiles the Rust snippets in the README and the docs.
//!
//! A fenced `rust` block preceded by `<!-- doc-check: compile -->` is written
//! into a scratch crate as a module of its own and type-checked against the
//! real library and the examples' schemas, so the pages track the API.
//! `crate::proto` in a snippet is the code generated from `examples/proto`,
//! and a snippet sees the `pub` items of the snippets before it on its page.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

const MARKER: &str = "<!-- doc-check: compile -->";

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf()
}

struct Snippet {
    page: String,
    line: usize,
    code: String,
}

fn snippets(page: &Path) -> Vec<Snippet> {
    let text = std::fs::read_to_string(page).unwrap();
    let name = page.strip_prefix(root()).unwrap().to_string_lossy().into_owned();
    let lines: Vec<&str> = text.lines().collect();
    let mut found = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        if lines[i].trim() != MARKER {
            i += 1;
            continue;
        }
        let open = i + 1;
        assert!(
            lines.get(open).is_some_and(|l| l.trim() == "```rust"),
            "{name}:{}: the doc-check marker must be followed by a ```rust block",
            i + 1
        );
        let close = (open + 1..lines.len())
            .find(|&j| lines[j].trim() == "```")
            .unwrap_or_else(|| panic!("{name}:{}: unterminated code block", open + 1));
        found.push(Snippet {
            page: name.clone(),
            line: open + 2,
            code: lines[open + 1..close].join("\n"),
        });
        i = close + 1;
    }
    found
}

fn module_name(page: &str) -> String {
    page.trim_end_matches(".md")
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

#[test]
fn the_documented_snippets_compile() {
    let root = root();
    let mut pages: Vec<PathBuf> = std::fs::read_dir(root.join("docs"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|x| x == "md"))
        .collect();
    pages.sort();
    pages.insert(0, root.join("README.md"));

    let mut source = String::from(
        "#![allow(dead_code, unused_imports, unused_variables, clippy::all)]\n\n\
         pub mod proto {\n    protobus::include_protos!();\n}\n",
    );
    let mut total = 0;
    for page in &pages {
        let found = snippets(page);
        if found.is_empty() {
            continue;
        }
        let page_module = module_name(&found[0].page);
        writeln!(source, "\nmod {page_module} {{").unwrap();
        for (index, snippet) in found.iter().enumerate() {
            total += 1;
            writeln!(source, "    // {}:{}", snippet.page, snippet.line).unwrap();
            writeln!(source, "    pub mod snippet_{} {{", index + 1).unwrap();
            // What the page defined above is in scope, as it is for its reader.
            for earlier in 1..=index {
                writeln!(source, "        use super::snippet_{earlier}::*;").unwrap();
            }
            for line in snippet.code.lines() {
                writeln!(source, "        {line}").unwrap();
            }
            source.push_str("    }\n");
        }
        source.push_str("}\n");
    }
    assert!(
        total >= 15,
        "only {total} snippets are marked for checking: the marker is being missed"
    );

    let scratch = root.join("target/doc-snippets");
    std::fs::create_dir_all(scratch.join("src")).unwrap();
    let path = |p: &str| root.join(p).to_string_lossy().replace('\\', "/");
    let manifest = format!(
        "[package]\nname = \"doc-snippets\"\nversion = \"0.0.0\"\nedition = \"2021\"\npublish = false\n\n[workspace]\n\n\
         [dependencies]\nprotobus = {{ path = \"{}\" }}\n\
         tokio = {{ version = \"1\", features = [\"rt-multi-thread\", \"macros\", \"time\", \"sync\"] }}\n\n\
         [build-dependencies]\nprotobus-build = {{ path = \"{}\" }}\n",
        path("protobus"),
        path("protobus-build")
    );
    let build = format!(
        "fn main() -> Result<(), Box<dyn std::error::Error>> {{\n    \
         protobus_build::configure().proto_dir(\"{}\").compile()?;\n    Ok(())\n}}\n",
        path("examples/proto")
    );
    // Written only when changed, so an unchanged run rebuilds nothing.
    for (file, contents) in [("Cargo.toml", &manifest), ("build.rs", &build), ("src/lib.rs", &source)] {
        let target = scratch.join(file);
        if std::fs::read_to_string(&target).ok().as_ref() != Some(contents) {
            std::fs::write(&target, contents).unwrap();
        }
    }
    // The workspace's lock file pins the same versions, already downloaded.
    std::fs::copy(root.join("Cargo.lock"), scratch.join("Cargo.lock")).unwrap();

    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let output = Command::new(cargo)
        .args(["check", "--quiet", "--message-format=short", "--manifest-path"])
        .arg(scratch.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", scratch.join("target"))
        .env_remove("RUSTFLAGS")
        .output()
        .expect("cargo runs");
    assert!(
        output.status.success(),
        "a documented snippet does not compile. Each is a module of {}, under a comment naming its page and line.\n\n{}",
        scratch.join("src/lib.rs").display(),
        String::from_utf8_lossy(&output.stderr)
    );
}
