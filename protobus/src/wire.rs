//! The protobus envelopes: the five small protobuf messages every request,
//! reply and event travels inside.
//!
//! ```proto
//! message RequestContainer  { string method = 1; string actor = 2; bytes data = 3; }
//! message ResponseResult    { string method = 1; bytes data = 2; }
//! message ResponseError     { string method = 1; string message = 2; string code = 3; }
//! message ResponseContainer { oneof value { ResponseResult result = 1; ResponseError error = 2; } }
//! message EventContainer    { string type = 1; string topic = 2; bytes data = 3; }
//! ```
//!
//! In the TypeScript reference they are protobufjs decorator classes, so
//! presence works proto2-style: an empty string or empty bytes is still
//! written. This codec reproduces those bytes exactly rather than leaning on
//! proto3 generated code, which would omit them. Both forms decode identically
//! on every port.

use std::fmt;

/// A request. `actor` is `None` when the caller named none: then it is not
/// written, as TypeScript does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub method: String,
    pub actor: Option<String>,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseResult {
    pub method: String,
    pub data: Vec<u8>,
}

/// Only these three fields cross the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseError {
    pub method: String,
    pub message: String,
    pub code: String,
}

/// A reply: a result or an error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
    Result(ResponseResult),
    Error(ResponseError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub r#type: String,
    pub topic: String,
    pub data: Vec<u8>,
}

/// Bytes that are not a valid envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Malformed(pub String);

impl fmt::Display for Malformed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "protobus: malformed envelope: {}", self.0)
    }
}

impl std::error::Error for Malformed {}

// ---- encoding --------------------------------------------------------------------

/// The method and data are always written; the actor only when there is one.
pub fn encode_request(r: &Request) -> Vec<u8> {
    let mut out = Vec::with_capacity(r.method.len() + r.data.len() + 16);
    put_bytes(&mut out, 1, r.method.as_bytes());
    if let Some(actor) = &r.actor {
        put_bytes(&mut out, 2, actor.as_bytes());
    }
    put_bytes(&mut out, 3, &r.data);
    out
}

pub fn encode_response(r: &Response) -> Vec<u8> {
    let mut out = Vec::new();
    match r {
        Response::Result(result) => put_bytes(&mut out, 1, &encode_result(result)),
        Response::Error(e) => {
            let mut inner = Vec::new();
            put_bytes(&mut inner, 1, e.method.as_bytes());
            put_bytes(&mut inner, 2, e.message.as_bytes());
            put_bytes(&mut inner, 3, e.code.as_bytes());
            put_bytes(&mut out, 2, &inner);
        }
    }
    out
}

/// A bare `ResponseResult`, not wrapped in a container.
pub fn encode_result(r: &ResponseResult) -> Vec<u8> {
    let mut out = Vec::with_capacity(r.method.len() + r.data.len() + 8);
    put_bytes(&mut out, 1, r.method.as_bytes());
    put_bytes(&mut out, 2, &r.data);
    out
}

/// All three fields are always written.
pub fn encode_event(e: &Event) -> Vec<u8> {
    let mut out = Vec::with_capacity(e.r#type.len() + e.topic.len() + e.data.len() + 12);
    put_bytes(&mut out, 1, e.r#type.as_bytes());
    put_bytes(&mut out, 2, e.topic.as_bytes());
    put_bytes(&mut out, 3, &e.data);
    out
}

pub(crate) fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub(crate) fn put_bytes(out: &mut Vec<u8>, field: u32, value: &[u8]) {
    put_varint(out, (u64::from(field) << 3) | 2);
    put_varint(out, value.len() as u64);
    out.extend_from_slice(value);
}

// ---- decoding --------------------------------------------------------------------

/// An absent actor decodes as `Some("")`: a reader cannot tell it from an
/// explicit empty one, and no port distinguishes them.
pub fn decode_request(b: &[u8]) -> Result<Request, Malformed> {
    let mut r = Reader::new(b);
    let (mut method, mut actor, mut data) = (String::new(), String::new(), Vec::new());
    while r.more() {
        let tag = r.tag()?;
        match tag >> 3 {
            1 => method = r.string(tag)?,
            2 => actor = r.string(tag)?,
            3 => data = r.bytes(tag)?.to_vec(),
            _ => r.skip(tag)?,
        }
    }
    Ok(Request {
        method,
        actor: Some(actor),
        data,
    })
}

/// When both members are present the error wins, matching the TypeScript
/// reader, so the same bytes cannot mean success on one port and failure on
/// another. Neither is refused.
pub fn decode_response(b: &[u8]) -> Result<Response, Malformed> {
    let mut r = Reader::new(b);
    let (mut result, mut error) = (None, None);
    while r.more() {
        let tag = r.tag()?;
        match tag >> 3 {
            1 => result = Some(decode_result(r.bytes(tag)?)?),
            2 => error = Some(decode_error(r.bytes(tag)?)?),
            _ => r.skip(tag)?,
        }
    }
    if let Some(e) = error {
        return Ok(Response::Error(e));
    }
    result
        .map(Response::Result)
        .ok_or_else(|| Malformed("response carries neither a result nor an error".into()))
}

fn decode_result(b: &[u8]) -> Result<ResponseResult, Malformed> {
    let mut r = Reader::new(b);
    let (mut method, mut data) = (String::new(), Vec::new());
    while r.more() {
        let tag = r.tag()?;
        match tag >> 3 {
            1 => method = r.string(tag)?,
            2 => data = r.bytes(tag)?.to_vec(),
            _ => r.skip(tag)?,
        }
    }
    Ok(ResponseResult { method, data })
}

fn decode_error(b: &[u8]) -> Result<ResponseError, Malformed> {
    let mut r = Reader::new(b);
    let (mut method, mut message, mut code) = (String::new(), String::new(), String::new());
    while r.more() {
        let tag = r.tag()?;
        match tag >> 3 {
            1 => method = r.string(tag)?,
            2 => message = r.string(tag)?,
            3 => code = r.string(tag)?,
            _ => r.skip(tag)?,
        }
    }
    Ok(ResponseError { method, message, code })
}

pub fn decode_event(b: &[u8]) -> Result<Event, Malformed> {
    let mut r = Reader::new(b);
    let (mut r#type, mut topic, mut data) = (String::new(), String::new(), Vec::new());
    while r.more() {
        let tag = r.tag()?;
        match tag >> 3 {
            1 => r#type = r.string(tag)?,
            2 => topic = r.string(tag)?,
            3 => data = r.bytes(tag)?.to_vec(),
            _ => r.skip(tag)?,
        }
    }
    Ok(Event { r#type, topic, data })
}

/// A cursor over protobuf wire bytes.
pub(crate) struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(b: &'a [u8]) -> Self {
        Reader { b, pos: 0 }
    }

    pub(crate) fn more(&self) -> bool {
        self.pos < self.b.len()
    }

    pub(crate) fn varint(&mut self) -> Result<u64, Malformed> {
        let mut result = 0u64;
        let mut shift = 0;
        while shift < 64 {
            let Some(&x) = self.b.get(self.pos) else {
                return Err(Malformed("truncated varint".into()));
            };
            self.pos += 1;
            result |= u64::from(x & 0x7f) << shift;
            if x & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
        Err(Malformed("varint longer than ten bytes".into()))
    }

    pub(crate) fn tag(&mut self) -> Result<u32, Malformed> {
        let t = self.varint()?;
        if t >> 3 == 0 || t > u64::from(u32::MAX) {
            return Err(Malformed(format!("invalid field tag {t}")));
        }
        Ok(t as u32)
    }

    pub(crate) fn bytes(&mut self, tag: u32) -> Result<&'a [u8], Malformed> {
        if tag & 7 != 2 {
            return Err(Malformed(format!(
                "field {} has wire type {}, not 2",
                tag >> 3,
                tag & 7
            )));
        }
        let n = self.varint()?;
        if n > (self.b.len() - self.pos) as u64 {
            return Err(Malformed(format!("length {n} overruns the message")));
        }
        let n = n as usize;
        let v = &self.b[self.pos..self.pos + n];
        self.pos += n;
        Ok(v)
    }

    fn string(&mut self, tag: u32) -> Result<String, Malformed> {
        let raw = self.bytes(tag)?;
        String::from_utf8(raw.to_vec()).map_err(|_| Malformed(format!("field {} is not valid UTF-8", tag >> 3)))
    }

    pub(crate) fn skip(&mut self, tag: u32) -> Result<(), Malformed> {
        match tag & 7 {
            0 => self.varint().map(|_| ()),
            1 => self.advance(8),
            2 => self.bytes(tag).map(|_| ()),
            5 => self.advance(4),
            w => Err(Malformed(format!("unsupported wire type {w}"))),
        }
    }

    fn advance(&mut self, n: usize) -> Result<(), Malformed> {
        if n > self.b.len() - self.pos {
            return Err(Malformed("truncated field".into()));
        }
        self.pos += n;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! The golden values were produced by the TypeScript reference
    //! implementation (protobus 2.4.0, MessageFactory), not derived by hand;
    //! protobus-go, protobus-cpp, protobus-java and protobus-csharp pin the
    //! same bytes.
    use super::*;

    fn unhex(h: &str) -> Vec<u8> {
        (0..h.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap())
            .collect()
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    fn request(method: &str, actor: Option<&str>, data: &[u8]) -> Request {
        Request {
            method: method.into(),
            actor: actor.map(str::to_string),
            data: data.to_vec(),
        }
    }

    #[test]
    fn request_matches_typescript() {
        assert_eq!(
            hex(&encode_request(&request("T.Svc.add", None, &unhex("0801")))),
            "0a09542e5376632e6164641a020801"
        );
        // proto3 would drop an all-default payload; TypeScript always writes the data field, even empty.
        assert_eq!(
            hex(&encode_request(&request("T.Svc.add", Some("x"), b""))),
            "0a09542e5376632e6164641201781a00"
        );
    }

    #[test]
    fn response_matches_typescript() {
        let result = |data: &str| {
            Response::Result(ResponseResult {
                method: "T.Svc.add".into(),
                data: unhex(data),
            })
        };
        assert_eq!(
            hex(&encode_response(&result("0802"))),
            "0a0f0a09542e5376632e61646412020802"
        );
        assert_eq!(hex(&encode_response(&result(""))), "0a0d0a09542e5376632e6164641200");
        // An empty code is written explicitly, as TypeScript does.
        let error = |m: &str, c: &str| {
            Response::Error(ResponseError {
                method: "T.Svc.add".into(),
                message: m.into(),
                code: c.into(),
            })
        };
        assert_eq!(
            hex(&encode_response(&error("boom", ""))),
            "12130a09542e5376632e6164641204626f6f6d1a00"
        );
        assert_eq!(
            hex(&encode_response(&error("m", "C"))),
            "12110a09542e5376632e61646412016d1a0143"
        );
    }

    #[test]
    fn event_matches_typescript() {
        let e = Event {
            r#type: "T.Ev".into(),
            topic: "EVENT.T.Ev".into(),
            data: unhex("0a0179"),
        };
        assert_eq!(hex(&encode_event(&e)), "0a04542e4576120a4556454e542e542e45761a030a0179");
        assert_eq!(
            decode_event(&unhex("0a04542e4576120a4556454e542e542e45761a030a0179")).unwrap(),
            e
        );
    }

    #[test]
    fn decodes_typescript_request() {
        // The actor written as an explicit empty string.
        let r = decode_request(&unhex("0a09542e5376632e61646412001a020801")).unwrap();
        assert_eq!(r.method, "T.Svc.add");
        assert_eq!(r.actor.as_deref(), Some(""));
        assert_eq!(r.data, unhex("0801"));
    }

    #[test]
    fn decodes_typescript_responses() {
        match decode_response(&unhex("12110a09542e5376632e61646412016d1a0143")).unwrap() {
            Response::Error(e) => assert_eq!(
                (e.method.as_str(), e.message.as_str(), e.code.as_str()),
                ("T.Svc.add", "m", "C")
            ),
            other => panic!("{other:?}"),
        }
        match decode_response(&unhex("0a0d0a09542e5376632e6164641200")).unwrap() {
            Response::Result(r) => {
                assert_eq!(r.method, "T.Svc.add");
                assert!(r.data.is_empty());
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn error_wins_over_result() {
        let mut b = Vec::new();
        put_bytes(
            &mut b,
            1,
            &encode_result(&ResponseResult {
                method: "a.B.c".into(),
                data: vec![],
            }),
        );
        let mut err = Vec::new();
        put_bytes(&mut err, 1, b"a.B.c");
        put_bytes(&mut err, 2, b"no");
        put_bytes(&mut b, 2, &err);
        assert!(matches!(decode_response(&b).unwrap(), Response::Error(_)));
    }

    #[test]
    fn empty_response_is_refused() {
        assert!(decode_response(b"").is_err());
    }

    #[test]
    fn skips_unknown_fields() {
        let mut b = encode_request(&request("a.B.c", None, b"\x01"));
        put_varint(&mut b, 99 << 3); // field 99, wire type 0
        put_varint(&mut b, 7);
        assert_eq!(decode_request(&b).unwrap().method, "a.B.c");
    }

    #[test]
    fn rejects_malformed_input() {
        for h in ["0a0954", "00", "0801", "0aff", "0a01ff"] {
            assert!(decode_request(&unhex(h)).is_err(), "{h}");
        }
    }

    #[test]
    fn round_trips() {
        let input = request("pkg.sub.Service.method", Some("user:42"), &[0xab; 300]);
        assert_eq!(decode_request(&encode_request(&input)).unwrap(), input);
    }
}
