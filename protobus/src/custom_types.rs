//! Custom types: scalar-looking protobuf types with a fixed wire format,
//! shared by every protobus port.
//!
//! The built-ins are `bigint` (an unsigned integer up to 2^256-1, carried as
//! exactly 32 big-endian bytes) and `timestamp` (signed milliseconds since the
//! epoch). Both are one-field messages declared at the root of the type
//! namespace, so a shared schema writes `bigint amount = 1;` and every port
//! reads the same bytes. In Rust they are [`Bigint`] and [`Timestamp`],
//! converted to and from [`U256`] and [`SystemTime`].
//!
//! Every port refuses a negative or oversized bigint and refuses to decode one
//! wider than 32 bytes; a timestamp is refused beyond ±8.64e15 ms. protobus
//! checks every request, reply and event it encodes or decodes.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost_reflect::{Kind, MessageDescriptor};

use crate::errors::{Error, ErrorKind, Result};
use crate::wire::Reader;

/// Width of the bigint wire format, in bytes.
pub const BIGINT_BYTES: usize = 32;

/// The largest magnitude, in milliseconds, every port can represent (the range
/// of an ECMAScript `Date`).
pub const MAX_TIMESTAMP_MS: i64 = 8_640_000_000_000_000;

fn range(message: String) -> Error {
    Error::new(ErrorKind::CustomTypeRange, message)
}

// ---- U256 ------------------------------------------------------------------------

/// An unsigned 256-bit integer: the in-language value of a `bigint`.
///
/// Parsing and arithmetic refuse anything outside 0..2^256-1 rather than
/// wrapping: for financial and on-chain amounts, failing loudly is the only
/// safe behaviour.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct U256 {
    /// Little-endian 64-bit limbs.
    limbs: [u64; 4],
}

impl U256 {
    pub const ZERO: U256 = U256 { limbs: [0; 4] };
    pub const ONE: U256 = U256 { limbs: [1, 0, 0, 0] };
    pub const MAX: U256 = U256 { limbs: [u64::MAX; 4] };

    /// Big-endian bytes, at most 32. Shorter input is zero-extended on the left.
    pub fn from_be_bytes(bytes: &[u8]) -> Result<U256> {
        if bytes.len() > BIGINT_BYTES {
            return Err(range(format!(
                "bigint wire value is {} bytes; the protobus bigint wire format is at most {BIGINT_BYTES}",
                bytes.len()
            )));
        }
        let mut padded = [0u8; BIGINT_BYTES];
        padded[BIGINT_BYTES - bytes.len()..].copy_from_slice(bytes);
        let mut limbs = [0u64; 4];
        for (i, limb) in limbs.iter_mut().enumerate() {
            let start = BIGINT_BYTES - 8 * (i + 1);
            *limb = u64::from_be_bytes(padded[start..start + 8].try_into().unwrap());
        }
        Ok(U256 { limbs })
    }

    /// Exactly 32 big-endian bytes.
    pub fn to_be_bytes(self) -> [u8; BIGINT_BYTES] {
        let mut out = [0u8; BIGINT_BYTES];
        for (i, limb) in self.limbs.iter().enumerate() {
            let start = BIGINT_BYTES - 8 * (i + 1);
            out[start..start + 8].copy_from_slice(&limb.to_be_bytes());
        }
        out
    }

    pub fn is_zero(self) -> bool {
        self.limbs == [0; 4]
    }

    /// The value, when it fits.
    pub fn to_u64(self) -> Option<u64> {
        (self.limbs[1] == 0 && self.limbs[2] == 0 && self.limbs[3] == 0).then_some(self.limbs[0])
    }

    /// The value, when it fits.
    pub fn to_u128(self) -> Option<u128> {
        (self.limbs[2] == 0 && self.limbs[3] == 0)
            .then_some(u128::from(self.limbs[0]) | (u128::from(self.limbs[1]) << 64))
    }

    pub fn checked_add(self, o: U256) -> Option<U256> {
        let mut out = [0u64; 4];
        let mut carry = false;
        for (i, limb) in out.iter_mut().enumerate() {
            let (a, c1) = self.limbs[i].overflowing_add(o.limbs[i]);
            let (b, c2) = a.overflowing_add(u64::from(carry));
            *limb = b;
            carry = c1 || c2;
        }
        (!carry).then_some(U256 { limbs: out })
    }

    pub fn checked_sub(self, o: U256) -> Option<U256> {
        let mut out = [0u64; 4];
        let mut borrow = false;
        for (i, limb) in out.iter_mut().enumerate() {
            let (a, b1) = self.limbs[i].overflowing_sub(o.limbs[i]);
            let (b, b2) = a.overflowing_sub(u64::from(borrow));
            *limb = b;
            borrow = b1 || b2;
        }
        (!borrow).then_some(U256 { limbs: out })
    }

    pub fn checked_mul(self, o: U256) -> Option<U256> {
        let mut out = [0u64; 4];
        for i in 0..4 {
            let mut carry = 0u128;
            for j in 0..4 {
                let product = u128::from(self.limbs[i]) * u128::from(o.limbs[j]);
                if i + j >= 4 {
                    if product != 0 {
                        return None;
                    }
                    continue;
                }
                let sum = u128::from(out[i + j]) + product + carry;
                out[i + j] = sum as u64;
                carry = sum >> 64;
            }
            if carry != 0 {
                return None;
            }
        }
        Some(U256 { limbs: out })
    }

    /// `self` raised to `exp`, when it fits.
    pub fn checked_pow(self, exp: u32) -> Option<U256> {
        let mut result = U256::ONE;
        for _ in 0..exp {
            result = result.checked_mul(self)?;
        }
        Some(result)
    }

    /// Divide by a small divisor in place, returning the remainder.
    fn div_rem_small(&mut self, divisor: u64) -> u64 {
        let mut rem = 0u128;
        for limb in self.limbs.iter_mut().rev() {
            let cur = (rem << 64) | u128::from(*limb);
            *limb = (cur / u128::from(divisor)) as u64;
            rem = cur % u128::from(divisor);
        }
        rem as u64
    }

    /// `0x`-prefixed, lowercase, no leading zeros.
    pub fn to_hex(self) -> String {
        let bytes = self.to_be_bytes();
        let digits: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let trimmed = digits.trim_start_matches('0');
        format!("0x{}", if trimmed.is_empty() { "0" } else { trimmed })
    }
}

impl From<u64> for U256 {
    fn from(v: u64) -> Self {
        U256 { limbs: [v, 0, 0, 0] }
    }
}

impl From<u32> for U256 {
    fn from(v: u32) -> Self {
        U256::from(u64::from(v))
    }
}

impl From<u128> for U256 {
    fn from(v: u128) -> Self {
        U256 {
            limbs: [v as u64, (v >> 64) as u64, 0, 0],
        }
    }
}

impl Ord for U256 {
    fn cmp(&self, other: &Self) -> Ordering {
        for i in (0..4).rev() {
            match self.limbs[i].cmp(&other.limbs[i]) {
                Ordering::Equal => continue,
                o => return o,
            }
        }
        Ordering::Equal
    }
}

impl PartialOrd for U256 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for U256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_zero() {
            return f.write_str("0");
        }
        // Peel nineteen decimal digits at a time.
        const CHUNK: u64 = 10_000_000_000_000_000_000;
        let mut v = *self;
        let mut parts = Vec::new();
        while !v.is_zero() {
            parts.push(v.div_rem_small(CHUNK));
        }
        let mut out = parts.pop().unwrap().to_string();
        for part in parts.iter().rev() {
            out.push_str(&format!("{part:019}"));
        }
        f.write_str(&out)
    }
}

impl FromStr for U256 {
    type Err = Error;

    /// A decimal string, or hexadecimal with a `0x` prefix.
    fn from_str(text: &str) -> Result<U256> {
        let t = text.trim();
        let malformed = || range(format!("bigint value '{text}' is not a decimal or 0x-hex integer"));
        let too_big = || {
            range(format!(
                "bigint value {text} exceeds the maximum representable value 2^256-1"
            ))
        };
        if t.starts_with('-') {
            return Err(range(format!(
                "bigint value {text} is negative; the protobus bigint wire format is unsigned (0 .. 2^256-1)"
            )));
        }
        let (digits, radix) = match t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")) {
            Some(hex) => (hex, 16u32),
            None => (t, 10u32),
        };
        if digits.is_empty() {
            return Err(malformed());
        }
        let base = U256::from(u64::from(radix));
        let mut value = U256::ZERO;
        for c in digits.chars() {
            let d = c.to_digit(radix).ok_or_else(malformed)?;
            value = value
                .checked_mul(base)
                .and_then(|v| v.checked_add(U256::from(u64::from(d))))
                .ok_or_else(too_big)?;
        }
        Ok(value)
    }
}

// ---- bigint ----------------------------------------------------------------------

/// The `bigint` custom type: an unsigned integer up to 2^256-1, exactly 32
/// big-endian bytes on the wire.
///
/// ```
/// use protobus::{Bigint, U256};
/// let amount = Bigint::from(U256::from(10u64).checked_pow(30).unwrap());
/// assert_eq!(amount.to_u256().unwrap().to_string(), "1000000000000000000000000000000");
/// ```
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Bigint {
    #[prost(bytes = "vec", optional, tag = "1")]
    pub value: Option<Vec<u8>>,
}

impl Bigint {
    /// Parse a decimal or `0x`-hex string.
    pub fn parse(text: &str) -> Result<Bigint> {
        text.parse::<U256>().map(Bigint::from)
    }

    /// The value; an unset or empty bigint is zero. Fails for a wire value
    /// wider than 32 bytes.
    pub fn to_u256(&self) -> Result<U256> {
        U256::from_be_bytes(self.value.as_deref().unwrap_or(&[]))
    }
}

impl From<U256> for Bigint {
    fn from(v: U256) -> Self {
        Bigint {
            value: Some(v.to_be_bytes().to_vec()),
        }
    }
}

impl From<u64> for Bigint {
    fn from(v: u64) -> Self {
        Bigint::from(U256::from(v))
    }
}

impl From<u128> for Bigint {
    fn from(v: u128) -> Self {
        Bigint::from(U256::from(v))
    }
}

impl ::prost::Name for Bigint {
    const NAME: &'static str = "bigint";
    const PACKAGE: &'static str = "";

    fn full_name() -> String {
        "bigint".into()
    }

    fn type_url() -> String {
        "/bigint".into()
    }
}

// ---- timestamp -------------------------------------------------------------------

/// The `timestamp` custom type: signed milliseconds since the Unix epoch.
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Timestamp {
    #[prost(int64, optional, tag = "1")]
    pub value: Option<i64>,
}

fn check_millis(ms: i64) -> Result<i64> {
    if !(-MAX_TIMESTAMP_MS..=MAX_TIMESTAMP_MS).contains(&ms) {
        return Err(range(format!(
            "timestamp value {ms} ms is beyond ±8.64e15 ms, the range every port can represent"
        )));
    }
    Ok(ms)
}

impl Timestamp {
    /// A timestamp at `ms` milliseconds since the epoch.
    pub fn from_millis(ms: i64) -> Result<Timestamp> {
        check_millis(ms).map(|ms| Timestamp { value: Some(ms) })
    }

    /// The current time, at millisecond precision.
    pub fn now() -> Timestamp {
        Timestamp::try_from(SystemTime::now()).expect("the current time is representable")
    }

    /// Milliseconds since the epoch; an unset timestamp is the epoch.
    pub fn to_millis(&self) -> Result<i64> {
        check_millis(self.value.unwrap_or(0))
    }

    /// The instant; an unset timestamp is the epoch.
    pub fn to_system_time(&self) -> Result<SystemTime> {
        let ms = self.to_millis()?;
        Ok(if ms >= 0 {
            UNIX_EPOCH + Duration::from_millis(ms as u64)
        } else {
            UNIX_EPOCH - Duration::from_millis(ms.unsigned_abs())
        })
    }
}

impl TryFrom<SystemTime> for Timestamp {
    type Error = Error;

    /// Truncates toward negative infinity to the millisecond, as the other ports do.
    fn try_from(t: SystemTime) -> Result<Timestamp> {
        let ms = match t.duration_since(UNIX_EPOCH) {
            Ok(d) => i64::try_from(d.as_millis()).unwrap_or(i64::MAX),
            Err(e) => {
                let d = e.duration();
                let whole = i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
                if d.subsec_nanos() % 1_000_000 == 0 {
                    -whole
                } else {
                    -whole - 1
                }
            }
        };
        Timestamp::from_millis(ms)
    }
}

impl ::prost::Name for Timestamp {
    const NAME: &'static str = "timestamp";
    const PACKAGE: &'static str = "";

    fn full_name() -> String {
        "timestamp".into()
    }

    fn type_url() -> String {
        "/timestamp".into()
    }
}

// ---- validation ------------------------------------------------------------------

fn reach_cache() -> &'static Mutex<HashMap<String, bool>> {
    static CACHE: OnceLock<Mutex<HashMap<String, bool>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Whether values of this type can contain a built-in custom type, cycles included.
fn reaches(desc: &MessageDescriptor) -> bool {
    let name = desc.full_name().to_string();
    if let Some(&known) = reach_cache().lock().unwrap().get(&name) {
        return known;
    }
    let result = reaches_inner(desc, &mut Vec::new());
    reach_cache().lock().unwrap().insert(name, result);
    result
}

fn reaches_inner(desc: &MessageDescriptor, visiting: &mut Vec<String>) -> bool {
    let name = desc.full_name();
    if name == "bigint" || name == "timestamp" {
        return true;
    }
    if visiting.iter().any(|v| v == name) {
        return false;
    }
    visiting.push(name.to_string());
    desc.fields().any(|f| match f.kind() {
        Kind::Message(m) => reaches_inner(&m, visiting),
        _ => false,
    })
}

/// Check every `bigint` and `timestamp` in an encoded message of type `desc`,
/// recursively, without decoding it: a bigint is at most 32 bytes, a timestamp
/// within ±8.64e15 ms. Messages that cannot contain one are skipped after a
/// single cached descriptor walk.
pub fn validate_encoded(desc: &MessageDescriptor, bytes: &[u8]) -> Result<()> {
    if !reaches(desc) {
        return Ok(());
    }
    walk(desc, bytes, 0)
}

fn malformed(desc: &MessageDescriptor, what: impl fmt::Display) -> Error {
    Error::new(
        ErrorKind::InvalidMessage,
        format!("{} does not decode: {what}", desc.full_name()),
    )
}

fn walk(desc: &MessageDescriptor, bytes: &[u8], depth: usize) -> Result<()> {
    // prost refuses deeper nesting when it decodes; this walk must not recurse further than that.
    if depth > 100 {
        return Err(malformed(desc, "nested too deeply"));
    }
    let name = desc.full_name();
    let is_bigint = name == "bigint";
    let is_timestamp = name == "timestamp";
    let mut r = Reader::new(bytes);
    while r.more() {
        let tag = r.tag().map_err(|e| malformed(desc, e.0))?;
        let number = tag >> 3;
        if is_bigint && number == 1 && tag & 7 == 2 {
            U256::from_be_bytes(r.bytes(tag).map_err(|e| malformed(desc, e.0))?)?;
            continue;
        }
        if is_timestamp && number == 1 && tag & 7 == 0 {
            check_millis(r.varint().map_err(|e| malformed(desc, e.0))? as i64)?;
            continue;
        }
        if tag & 7 == 2 {
            let payload = r.bytes(tag).map_err(|e| malformed(desc, e.0))?;
            if let Some(field) = desc.get_field(number) {
                if let Kind::Message(child) = field.kind() {
                    if reaches(&child) {
                        walk(&child, payload, depth + 1)?;
                    }
                }
            }
            continue;
        }
        r.skip(tag).map_err(|e| malformed(desc, e.0))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn u256_parses_and_prints() {
        for s in [
            "0",
            "1",
            "18446744073709551616",
            "1000000000000000000000000000000",
            "115792089237316195423570985008687907853269984665640564039457584007913129639935",
        ] {
            assert_eq!(s.parse::<U256>().unwrap().to_string(), s);
        }
        assert_eq!("0xff".parse::<U256>().unwrap(), U256::from(255u64));
        assert_eq!(U256::from(255u64).to_hex(), "0xff");
        assert_eq!(U256::ZERO.to_hex(), "0x0");
        assert_eq!(U256::MAX.to_string().len(), 78);
        assert!(
            "115792089237316195423570985008687907853269984665640564039457584007913129639936"
                .parse::<U256>()
                .is_err()
        );
        assert!("-1".parse::<U256>().is_err());
        assert!("12a".parse::<U256>().is_err());
        assert!("".parse::<U256>().is_err());
        assert!("0x".parse::<U256>().is_err());
    }

    #[test]
    fn u256_arithmetic_is_checked() {
        let two70 = U256::from(2u64).checked_pow(70).unwrap();
        assert_eq!(two70.to_string(), "1180591620717411303424");
        assert_eq!(
            two70.checked_add(U256::ONE).unwrap().to_string(),
            "1180591620717411303425"
        );
        assert_eq!(two70.checked_sub(two70).unwrap(), U256::ZERO);
        assert!(U256::MAX.checked_add(U256::ONE).is_none());
        assert!(U256::ZERO.checked_sub(U256::ONE).is_none());
        assert!(U256::MAX.checked_mul(U256::from(2u64)).is_none());
        assert_eq!(
            U256::from(2u64).checked_pow(255).unwrap().checked_mul(U256::from(2u64)),
            None
        );
        assert!(U256::from(3u64) > U256::from(2u64));
        assert!(two70 > U256::from(u64::MAX));
        assert_eq!(U256::from(u128::MAX).to_u128(), Some(u128::MAX));
        assert_eq!(two70.to_u64(), None);
    }

    #[test]
    fn bigint_is_thirty_two_big_endian_bytes() {
        let b = Bigint::from(1u64);
        let bytes = b.value.clone().unwrap();
        assert_eq!(bytes.len(), 32);
        assert_eq!(bytes[31], 1);
        assert!(bytes[..31].iter().all(|&x| x == 0));
        assert_eq!(b.to_u256().unwrap(), U256::ONE);
        assert_eq!(Bigint::default().to_u256().unwrap(), U256::ZERO);
        assert_eq!(
            Bigint {
                value: Some(vec![1, 0])
            }
            .to_u256()
            .unwrap(),
            U256::from(256u64)
        );
        assert!(Bigint {
            value: Some(vec![0; 33])
        }
        .to_u256()
        .is_err());
        assert_eq!(Bigint::parse("0x10").unwrap().to_u256().unwrap(), U256::from(16u64));
    }

    #[test]
    fn timestamps_keep_milliseconds_either_side_of_the_epoch() {
        assert_eq!(
            Timestamp::from_millis(-14182940000).unwrap().to_millis().unwrap(),
            -14182940000
        );
        let before = UNIX_EPOCH - Duration::from_micros(1500);
        assert_eq!(Timestamp::try_from(before).unwrap().to_millis().unwrap(), -2);
        let t = Timestamp::from_millis(-2).unwrap().to_system_time().unwrap();
        assert_eq!(UNIX_EPOCH.duration_since(t).unwrap(), Duration::from_millis(2));
        assert!(Timestamp::from_millis(MAX_TIMESTAMP_MS + 1).is_err());
        assert!(Timestamp {
            value: Some(-MAX_TIMESTAMP_MS - 1)
        }
        .to_millis()
        .is_err());
        assert_eq!(Timestamp::default().to_millis().unwrap(), 0);
    }

    #[test]
    fn validation_walks_encoded_messages() {
        let pool = crate::message_factory::builtin_pool();
        let bigint = pool.get_message_by_name("bigint").unwrap();
        let timestamp = pool.get_message_by_name("timestamp").unwrap();
        validate_encoded(&bigint, &Bigint::from(7u64).encode_to_vec()).unwrap();
        let wide = Bigint {
            value: Some(vec![1; 33]),
        }
        .encode_to_vec();
        assert_eq!(
            validate_encoded(&bigint, &wide).unwrap_err().kind(),
            ErrorKind::CustomTypeRange
        );
        let late = Timestamp {
            value: Some(MAX_TIMESTAMP_MS + 1),
        }
        .encode_to_vec();
        assert_eq!(
            validate_encoded(&timestamp, &late).unwrap_err().kind(),
            ErrorKind::CustomTypeRange
        );
        validate_encoded(&timestamp, &Timestamp::from_millis(-5).unwrap().encode_to_vec()).unwrap();
    }
}
