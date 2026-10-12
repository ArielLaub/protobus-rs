//! An isolated RabbitMQ for tests: a fresh virtual host per test on the broker
//! named by `PROTOBUS_TEST_AMQP_URL`, created and deleted through the
//! management API at `PROTOBUS_TEST_MGMT_URL`.
//!
//! There is deliberately no default broker URL: localhost:5672 is routinely a
//! port-forward to a shared cluster, and a suite that declares and deletes
//! queues must never reach one by accident. Tests skip when the variables are
//! unset, and fail when they are set but the broker is unusable.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn base64(input: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in input.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, b)| n | (u32::from(*b) << (16 - 8 * i)));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(T[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn url_encode(text: &str) -> String {
    text.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// The management API, over a minimal HTTP/1.1 client.
struct Management {
    host: String,
    auth: String,
}

impl Management {
    /// `http://user:pass@host:port`
    fn new(url: &str) -> Self {
        let rest = url
            .strip_prefix("http://")
            .expect("PROTOBUS_TEST_MGMT_URL must be http://user:pass@host:port");
        let (userinfo, host) = rest
            .trim_end_matches('/')
            .rsplit_once('@')
            .expect("PROTOBUS_TEST_MGMT_URL needs credentials");
        Management {
            host: host.to_string(),
            auth: base64(userinfo.as_bytes()),
        }
    }

    /// The status and the body.
    fn request(&self, method: &str, path: &str, body: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(&self.host).expect("the management API is reachable");
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nAuthorization: Basic {}\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.host,
            self.auth,
            body.len()
        )
        .unwrap();
        let mut response = Vec::new();
        let _ = stream.read_to_end(&mut response);
        let response = String::from_utf8_lossy(&response).into_owned();
        let status = response
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_string())
            .unwrap_or_default();
        (status, body)
    }
}

/// Every string value of `"key":"..."` in a JSON document: enough for the
/// management API's lists, without a JSON parser.
fn string_values(json: &str, key: &str) -> Vec<String> {
    let needle = format!("\"{key}\":\"");
    json.match_indices(&needle)
        .filter_map(|(at, _)| {
            let rest = &json[at + needle.len()..];
            rest.find('"').map(|end| rest[..end].to_string())
        })
        .collect()
}

/// A fresh virtual host, deleted when dropped.
pub struct VHost {
    management: Management,
    name: String,
    url: String,
}

impl VHost {
    /// `None` when the broker variables are not both set: the test skips.
    pub fn create() -> Option<VHost> {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let (amqp, mgmt) = (var("PROTOBUS_TEST_AMQP_URL"), var("PROTOBUS_TEST_MGMT_URL"));
        let (Some(amqp), Some(mgmt)) = (amqp, mgmt) else {
            eprintln!("skipped: PROTOBUS_TEST_AMQP_URL and PROTOBUS_TEST_MGMT_URL are not both set");
            return None;
        };
        let management = Management::new(&mgmt);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!(
            "pbrs-{nanos:x}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let (status, _) = management.request("PUT", &format!("/api/vhosts/{name}"), "");
        assert!(matches!(status, 201 | 204), "could not create a vhost: HTTP {status}");
        let user = amqp
            .split("://")
            .nth(1)
            .and_then(|r| r.split(':').next())
            .unwrap_or("guest")
            .to_string();
        let grant = r#"{"configure":".*","write":".*","read":".*"}"#;
        let (status, _) = management.request("PUT", &format!("/api/permissions/{name}/{user}"), grant);
        assert!(
            matches!(status, 201 | 204),
            "could not grant permissions: HTTP {status}"
        );
        // Everything up to the authority, then the new vhost.
        let scheme = amqp.find("://").expect("PROTOBUS_TEST_AMQP_URL is a URL") + 3;
        let authority_end = amqp[scheme..].find('/').map_or(amqp.len(), |j| scheme + j);
        Some(VHost {
            management,
            url: format!("{}/{name}", &amqp[..authority_end]),
            name,
        })
    }

    /// The AMQP URL of this vhost.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Delete a queue, as an operator would.
    pub fn delete_queue(&self, queue: &str) {
        let (status, _) = self.management.request(
            "DELETE",
            &format!("/api/queues/{}/{}", self.name, url_encode(queue)),
            "",
        );
        assert!(matches!(status, 200 | 204), "could not delete {queue}: HTTP {status}");
    }

    pub fn queue_exists(&self, queue: &str) -> bool {
        self.management
            .request("GET", &format!("/api/queues/{}/{}", self.name, url_encode(queue)), "")
            .0
            == 200
    }

    /// Close every connection to this vhost from the broker's side. The
    /// management API learns of connections a few seconds late, so this waits
    /// until it has seen at least `at_least`. Returns how many were closed.
    pub fn close_connections(&self, at_least: usize) -> usize {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let (_, body) = self
                .management
                .request("GET", &format!("/api/vhosts/{}/connections", self.name), "");
            let names = string_values(&body, "name");
            if names.len() >= at_least {
                for name in &names {
                    self.management
                        .request("DELETE", &format!("/api/connections/{}", url_encode(name)), "");
                }
                return names.len();
            }
            assert!(
                Instant::now() < deadline,
                "the management API never listed {at_least} connection(s)"
            );
            std::thread::sleep(Duration::from_millis(250));
        }
    }
}

impl Drop for VHost {
    fn drop(&mut self) {
        self.management
            .request("DELETE", &format!("/api/vhosts/{}", self.name), "");
    }
}
