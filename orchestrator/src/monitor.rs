// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use tokio::sync::mpsc;

#[must_use]
pub struct NodeMonitorHandle(mpsc::Sender<()>);

impl NodeMonitorHandle {
    pub fn new() -> (Self, mpsc::Receiver<()>) {
        let (sender, receiver) = mpsc::channel(1);
        (Self(sender), receiver)
    }
}
