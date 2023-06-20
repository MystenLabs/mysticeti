// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::future_simulator::SimulatorContext;
use crate::types::format_authority_index;
use std::env;
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::{Compact, Format, Writer};
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::fmt::{format, FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::FmtSubscriber;

pub fn setup_simulator_tracing() {
    let env_log = env::var("RUST_LOG");
    let env_log = env_log
        .as_ref()
        .map(String::as_str)
        .unwrap_or("mysticeti_core=info,mysticeti_core::block_store=warn");
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(env_log)
        .event_format(SimulatorFormat(
            format().with_timer(SimulatorTime).compact(),
        ))
        .finish();
    tracing::dispatcher::set_global_default(subscriber.into()).ok();
}

struct SimulatorFormat(Format<Compact, SimulatorTime>);

impl<S, N> FormatEvent<S, N> for SimulatorFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let current = SimulatorContext::current_node();
        if let Some(current) = current {
            write!(writer, "[{}] ", format_authority_index(current))?;
        } else {
            write!(writer, "[?] ")?;
        }
        self.0.format_event(ctx, writer, event)
    }
}

struct SimulatorTime;

impl FormatTime for SimulatorTime {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        write!(w, "+{:05}", SimulatorContext::time().as_millis())
    }
}
