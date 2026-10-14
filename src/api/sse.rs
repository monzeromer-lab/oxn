//! `GET /v1/queues/{queue}/events` — the queue's events as server-sent
//! events.
//!
//! Each event is one SSE message: `id` is the event-stream entry id,
//! `event` the event type (`completed`, `failed`, …) and `data` the event
//! as JSON. A reconnecting client sends `Last-Event-ID` (browsers do it on
//! their own) and resumes right after the last event it saw, with nothing
//! lost while it was away (as far back as the stream is retained).

use std::time::Duration;

use futures::StreamExt;

use super::routes::error_response;
use super::{Api, ApiBody, ApiRequest, ApiResponse};
use crate::events::{EventRecord, StreamStart};

fn param<'a>(q: &'a [(String, String)], k: &str) -> Option<&'a str> {
    q.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str())
}

/// `1700000000000-0`: an event-stream entry id.
fn is_entry_id(s: &str) -> bool {
    let mut parts = s.splitn(2, '-');
    let (Some(ms), Some(seq)) = (parts.next(), parts.next()) else {
        return false;
    };
    !ms.is_empty()
        && !seq.is_empty()
        && ms.bytes().all(|b| b.is_ascii_digit())
        && seq.bytes().all(|b| b.is_ascii_digit())
}

fn frame(rec: &EventRecord) -> Vec<u8> {
    let data = serde_json::to_value(&rec.event).unwrap_or_default();
    let kind = data
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("unknown")
        .to_string();
    // The event's own fields (`id` is the job id), plus where it sits in
    // the stream.
    let mut v = match data {
        serde_json::Value::Object(m) => m,
        _ => serde_json::Map::new(),
    };
    v.insert("event_id".into(), rec.id.clone().into());
    v.insert("at_ms".into(), rec.at_ms.into());
    format!(
        "id: {}\nevent: {kind}\ndata: {}\n\n",
        rec.id,
        serde_json::Value::Object(v)
    )
    .into_bytes()
}

pub(crate) async fn events(
    api: &Api,
    queue: &str,
    q: &[(String, String)],
    req: &ApiRequest,
) -> Result<ApiResponse, ApiResponse> {
    let last = req
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("last-event-id"))
        .map(|(_, v)| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let start = match (last.as_deref(), param(q, "from")) {
        (Some(id), _) | (None, Some(id)) if is_entry_id(id) => StreamStart::After(id.to_string()),
        (None, None | Some("now")) => StreamStart::Now,
        (None, Some("beginning")) => StreamStart::Beginning,
        (Some(bad), _) | (None, Some(bad)) => {
            return Err(ApiResponse::error(
                400,
                "bad_request",
                format!("`{bad}` is not `now`, `beginning` or an event id"),
            ));
        }
    };
    let records = api
        .backend
        .subscribe(queue, start)
        .await
        .map_err(|e| error_response(&e))?;
    let events = records.map(|r| match r {
        Ok(rec) => frame(&rec),
        Err(e) => format!(
            "event: error\ndata: {}\n\n",
            serde_json::json!({ "message": e.to_string() })
        )
        .into_bytes(),
    });
    let every = api.config.sse_keepalive.max(Duration::from_millis(100));
    let keepalive = futures::stream::unfold((), move |()| async move {
        tokio::time::sleep(every).await;
        Some((b": keepalive\n\n".to_vec(), ()))
    });
    // `retry` tells browsers how soon to reconnect.
    let hello = futures::stream::once(async { b"retry: 2000\n: connected\n\n".to_vec() });
    let body = hello
        .chain(futures::stream::select(events, keepalive))
        .boxed();
    Ok(ApiResponse {
        status: 200,
        headers: vec![
            ("content-type".into(), "text/event-stream".into()),
            ("x-accel-buffering".into(), "no".into()),
        ],
        body: ApiBody::Stream(body),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_ids() {
        assert!(is_entry_id("1791939026044-0"));
        assert!(is_entry_id("1-12"));
        assert!(!is_entry_id("1791939026044"));
        assert!(!is_entry_id("now"));
        assert!(!is_entry_id("1-x"));
        assert!(!is_entry_id("-1"));
    }

    #[test]
    fn frames_carry_id_type_and_json() {
        let rec = EventRecord {
            id: "5-1".into(),
            at_ms: 5,
            event: crate::Event::Completed {
                id: crate::JobId::new("42"),
                return_value_json: Some("7".into()),
            },
        };
        let f = String::from_utf8(frame(&rec)).unwrap();
        assert!(f.starts_with("id: 5-1\nevent: completed\ndata: {"), "{f}");
        assert!(f.ends_with("\n\n"));
        let data: serde_json::Value =
            serde_json::from_str(f.lines().nth(2).unwrap().strip_prefix("data: ").unwrap())
                .unwrap();
        assert_eq!(data["type"], "completed");
        assert_eq!(data["id"], "42", "the job id");
        assert_eq!(data["event_id"], "5-1", "the stream entry id");
    }
}
