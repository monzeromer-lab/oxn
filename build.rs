//! Assembles the Redis Lua scripts: each script body gets only the helpers
//! from `lua/includes/common.lua` it actually uses (transitively), not the
//! whole file.
//!
//! Redis re-runs a script's top level on every `EVALSHA`, so every
//! `local function` in the prelude is a closure allocated per call (~0.07µs
//! each, ~3.5µs for the full prelude). Including only what a script needs
//! cuts that to its handful of helpers on the hot paths.
//!
//! Output: `$OUT_DIR/lua/<name>.lua`, embedded by `src/backend/redis/scripts.rs`.

use std::collections::BTreeSet;
use std::path::Path;
use std::{env, fs};

struct Item {
    name: String,
    text: String,
}

/// The leading comment block, then one item per top-level `local function`
/// (through its closing `end` at column 0) or `local NAME = ...` line, each
/// with the comment lines directly above it.
fn split(common: &str) -> (String, Vec<Item>) {
    let mut lines = common.lines().peekable();
    let mut header = String::new();
    while let Some(l) = lines.peek() {
        if l.starts_with("--") {
            header.push_str(l);
            header.push('\n');
            lines.next();
        } else {
            break;
        }
    }
    let mut items = Vec::new();
    let mut comments = String::new();
    while let Some(line) = lines.next() {
        if line.starts_with("--") {
            comments.push_str(line);
            comments.push('\n');
            continue;
        }
        if line.trim().is_empty() {
            comments.clear();
            continue;
        }
        let mut text = std::mem::take(&mut comments);
        text.push_str(line);
        text.push('\n');
        let name = if let Some(rest) = line.strip_prefix("local function ") {
            for l in lines.by_ref() {
                text.push_str(l);
                text.push('\n');
                if l == "end" {
                    break;
                }
            }
            rest.split('(')
                .next()
                .unwrap_or_default()
                .trim()
                .to_string()
        } else if let Some(rest) = line.strip_prefix("local ") {
            rest.split(['=', ' '])
                .next()
                .unwrap_or_default()
                .trim()
                .to_string()
        } else {
            panic!("common.lua: unexpected top-level line: {line}");
        };
        items.push(Item { name, text });
    }
    (header, items)
}

fn identifiers(src: &str) -> BTreeSet<&str> {
    src.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|w| !w.is_empty())
        .collect()
}

fn main() {
    let lua = Path::new("src/backend/redis/lua");
    println!("cargo:rerun-if-changed={}", lua.display());
    let common = fs::read_to_string(lua.join("includes/common.lua")).expect("common.lua");
    println!(
        "cargo:rerun-if-changed={}",
        lua.join("includes/common.lua").display()
    );
    let (header, items) = split(&common);
    let out = Path::new(&env::var("OUT_DIR").expect("OUT_DIR")).join("lua");
    fs::create_dir_all(&out).expect("create OUT_DIR/lua");

    let mut entries: Vec<_> = fs::read_dir(lua)
        .expect("lua dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "lua"))
        .collect();
    entries.sort();
    for path in entries {
        println!("cargo:rerun-if-changed={}", path.display());
        let body = fs::read_to_string(&path).expect("script");
        // Transitive closure of the helpers the body references.
        let mut needed: BTreeSet<&str> = BTreeSet::new();
        let mut frontier: Vec<&str> = identifiers(&body).into_iter().collect();
        while let Some(id) = frontier.pop() {
            if let Some(item) = items.iter().find(|i| i.name == id)
                && needed.insert(item.name.as_str())
            {
                frontier.extend(identifiers(&item.text));
            }
        }
        let mut assembled = header.clone();
        for item in items.iter().filter(|i| needed.contains(i.name.as_str())) {
            assembled.push('\n');
            assembled.push_str(&item.text);
        }
        assembled.push('\n');
        assembled.push_str(&body);
        let name = path.file_name().expect("file name");
        fs::write(out.join(name), assembled).expect("write script");
    }
}
