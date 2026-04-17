//! Job progress reporting.

use serde::{Deserialize, Serialize};

/// A progress marker reported by the handler.
///
/// BullMQ allows arbitrary JSON progress; we accept either a percentage or an
/// arbitrary `serde_json::Value`. Callers that want structured progress can
/// embed it in [`Progress::Arbitrary`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Progress {
    /// Percentage progress, clamped to `0.0..=100.0`.
    Percent(f64),
    /// Arbitrary JSON-serializable progress payload.
    Arbitrary(serde_json::Value),
}

impl Progress {
    /// Construct a percentage progress marker (clamped to `0.0..=100.0`).
    pub fn percent(p: f64) -> Self {
        Self::Percent(p.clamp(0.0, 100.0))
    }

    /// Construct a progress marker from any serializable type.
    pub fn from_serializable<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> {
        Ok(Self::Arbitrary(serde_json::to_value(value)?))
    }
}

impl From<u8> for Progress {
    fn from(v: u8) -> Self {
        Self::percent(f64::from(v))
    }
}

impl From<f64> for Progress {
    fn from(v: f64) -> Self {
        Self::percent(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_clamps() {
        assert_eq!(Progress::percent(-5.0), Progress::Percent(0.0));
        assert_eq!(Progress::percent(250.0), Progress::Percent(100.0));
        assert_eq!(Progress::percent(42.25), Progress::Percent(42.25));
    }

    #[test]
    fn from_u8_converts() {
        assert_eq!(Progress::from(50u8), Progress::Percent(50.0));
    }

    #[test]
    fn arbitrary_from_serializable() {
        #[derive(serde::Serialize)]
        struct S {
            step: &'static str,
            n: u32,
        }
        let p = Progress::from_serializable(&S {
            step: "uploading",
            n: 3,
        })
        .unwrap();
        match p {
            Progress::Arbitrary(v) => {
                assert_eq!(v["step"], "uploading");
                assert_eq!(v["n"], 3);
            }
            _ => panic!("expected Arbitrary"),
        }
    }

    #[test]
    fn serde_roundtrip_percent() {
        let p = Progress::percent(33.3);
        let j = serde_json::to_string(&p).unwrap();
        assert_eq!(j, "33.3");
        let back: Progress = serde_json::from_str(&j).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn serde_roundtrip_arbitrary_object() {
        let raw = r#"{"done":false,"pct":12}"#;
        let p: Progress = serde_json::from_str(raw).unwrap();
        assert!(matches!(p, Progress::Arbitrary(_)));
    }
}
