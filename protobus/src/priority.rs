//! Validation of queue-level and per-message priorities, before anything
//! reaches the broker.

use crate::config::RECOMMENDED_MAX_PRIORITY;
use crate::errors::{Error, ErrorKind, Result};

/// Validate the queue-level `max_priority` (`x-max-priority`). The floor is 1:
/// `x-max-priority: 0` gives a single level, a plain queue with a priority
/// queue's overhead.
pub fn validate_max_priority(value: Option<i64>) -> Result<Option<u8>> {
    require(
        value,
        "maxPriority",
        1,
        &format!(
            "RabbitMQ maintains internal structures per priority level, so keep the range small: \
             {RECOMMENDED_MAX_PRIORITY} is the recommended value and gives {} levels.",
            RECOMMENDED_MAX_PRIORITY + 1
        ),
    )
}

/// Validate a per-message priority. 0 is valid and is RabbitMQ's default.
pub fn validate_priority(value: Option<i64>) -> Result<Option<u8>> {
    require(
        value,
        "priority",
        0,
        "A priority above the queue's x-max-priority is clamped by the broker, not rejected.",
    )
}

fn require(value: Option<i64>, label: &str, min: i64, extra: &str) -> Result<Option<u8>> {
    match value {
        None => Ok(None),
        Some(v) if v < min || v > 255 => Err(Error::new(
            ErrorKind::InvalidPriority,
            format!("{label} must be between {min} and 255, got {v}. {extra}"),
        )),
        Some(v) => Ok(Some(v as u8)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges() {
        assert_eq!(validate_max_priority(None).unwrap(), None);
        assert_eq!(validate_max_priority(Some(2)).unwrap(), Some(2));
        assert!(validate_max_priority(Some(0)).is_err());
        assert!(validate_max_priority(Some(256)).is_err());
        assert_eq!(validate_priority(Some(0)).unwrap(), Some(0));
        assert_eq!(validate_priority(Some(255)).unwrap(), Some(255));
        assert!(validate_priority(Some(-1)).is_err());
        assert!(validate_priority(Some(256)).is_err());
    }
}
