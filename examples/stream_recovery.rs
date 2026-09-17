// SPDX-License-Identifier: 0BSD
//! FIN deadline/reset handling and recovery of partial `read_to_end` data.

use quietquic::conn::{ConnError, ReadToEndError, RecvStream, ResetOutcome, SendStream};
use std::time::Duration;

/// Apply an application deadline to transport acknowledgement. Timing out the
/// wait does not cancel FIN, so this policy explicitly requests a reset and
/// inspects the stable outcome.
#[allow(dead_code)]
async fn finish_with_deadline(
    send: &mut SendStream,
    deadline: Duration,
) -> Result<Option<ResetOutcome>, ConnError> {
    send.finish().await?;
    match tokio::time::timeout(deadline, send.wait_finished()).await {
        Ok(Ok(())) => Ok(None),
        Ok(Err(error)) => Err(error),
        Err(_) => send.reset(42).await.map(Some),
    }
}

/// `ReadToEndError` returns ownership of bytes collected before a limit,
/// reset, or connection failure. Process or persist that prefix before
/// deciding how the application protocol should recover.
#[allow(dead_code)]
async fn read_bounded(
    recv: &mut RecvStream,
    limit: usize,
) -> Result<Vec<u8>, (Vec<u8>, ConnError)> {
    recv.read_to_end(limit)
        .await
        .map_err(|ReadToEndError { prefix, error }| (prefix, error))
}

fn main() {
    println!("See the source for FIN timeout/reset and partial-read recovery patterns.");
}
