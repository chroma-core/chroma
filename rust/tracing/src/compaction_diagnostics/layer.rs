use super::{now_ms, State, Work};
use std::{collections::BTreeMap, sync::Arc, time::Instant};
use tracing::{
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    Subscriber,
};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

#[derive(Clone)]
pub struct CompactionDiagnostics {
    pub(super) state: Arc<State>,
    pub(super) allocator_stats: fn() -> serde_json::Value,
}

#[derive(Clone)]
struct Job {
    collection: String,
    attempt: String,
}

#[derive(Default)]
struct Fields {
    strings: BTreeMap<String, String>,
    counters: BTreeMap<String, u64>,
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if matches!(
            field.name(),
            "collection_id" | "segment" | "segment_id" | "otel.name"
        ) {
            self.strings.insert(
                field.name().into(),
                format!("{value:?}").trim_matches('"').into(),
            );
        }
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_debug(field, &value);
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name().starts_with("diagnostic.") {
            self.counters.insert(field.name().into(), value);
        }
    }
}

fn is_stage(name: &str) -> bool {
    matches!(
        name,
        "CompactionManager::compact"
            | "Apply materialized logs"
            | "segment_writer.finish()"
            | "Flush segment"
            | "Prefetch segment"
            | "Load old dimension maxes"
            | "Write offset values and compute block maxes"
            | "Write max blockfile"
            | "Commit max blockfile"
            | "Commit offset-value blockfile"
    )
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for CompactionDiagnostics {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        let span = ctx.span(id).expect("new span is registered");
        let inherited = span
            .parent()
            .and_then(|p| p.extensions().get::<Job>().cloned());
        let name = attrs.metadata().name();
        let job = if name == "CompactionManager::compact"
            || (name == "Prefetch segment" && inherited.is_none())
        {
            fields.strings.get("collection_id").map(|collection| Job {
                collection: collection
                    .trim_start_matches("CollectionUuid(")
                    .trim_end_matches(')')
                    .into(),
                attempt: uuid::Uuid::new_v4().to_string(),
            })
        } else {
            inherited
        };
        let Some(job) = job else {
            return;
        };
        span.extensions_mut().insert(job.clone());
        if !is_stage(name) {
            return;
        }
        let work = Work {
            collection_id: job.collection,
            job_attempt_id: job.attempt,
            stage: fields
                .strings
                .get("otel.name")
                .cloned()
                .unwrap_or_else(|| name.into()),
            segment: fields
                .strings
                .get("segment")
                .or_else(|| fields.strings.get("segment_id"))
                .cloned(),
            started_unix_ms: now_ms(),
            started: Instant::now(),
            counters: fields.counters,
        };
        self.state
            .work
            .lock()
            .unwrap()
            .insert(id.into_u64(), work.clone());
        self.state
            .emit("compaction_stage_start", serde_json::json!(work));
    }

    fn on_record(&self, id: &Id, record: &Record<'_>, _ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        record.record(&mut fields);
        if let Some(work) = self.state.work.lock().unwrap().get_mut(&id.into_u64()) {
            work.counters.extend(fields.counters);
        }
    }

    fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
        let work = self.state.work.lock().unwrap().remove(&id.into_u64());
        if let Some(work) = work {
            self.state.emit(
                "compaction_stage_finish",
                serde_json::json!({
                    "work": work, "elapsed_ms": work.started.elapsed().as_millis(),
                    "finish_reason": "span_closed",
                }),
            );
        }
    }
}
