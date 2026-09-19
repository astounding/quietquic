// Deterministic stream audit regressions, included by endpoint::tests.

#[tokio::test]
async fn canceled_fin_wait_does_not_cancel_fin_and_later_wait_observes_ack() {
    let (mut pair, _a, _b, client, server) = ManualPair::connected().await;
    let (mut send, _recv) = pair.complete(client.open_bi()).unwrap();
    pair.complete(send.write_all(b"durable fin")).unwrap();
    let (_peer_send, _peer_recv) = pair.complete(server.accept_bi()).unwrap();

    // Finish locally, but do not transfer the FIN to the peer yet.
    let mut finish = Box::pin(send.finish());
    assert!(finish
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    pair.a.control();
    assert!(matches!(
        finish
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
    drop(finish);

    let mut wait = Box::pin(send.wait_finished());
    assert!(wait
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    pair.a.control();
    drop(wait);

    pair.drive();
    pair.complete(send.wait_finished()).unwrap();
}

#[tokio::test]
async fn reset_ordered_before_fin_ack_remains_the_stable_terminal_fact() {
    let (mut pair, _a, _b, client, server) = ManualPair::connected().await;
    let (mut send, _recv) = pair.complete(client.open_bi()).unwrap();
    pair.complete(send.write_all(b"reset wins")).unwrap();
    let (_peer_send, _peer_recv) = pair.complete(server.accept_bi()).unwrap();

    let mut finish = Box::pin(send.finish());
    assert!(finish
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    pair.a.control();
    assert!(matches!(
        finish
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
    drop(finish);

    // Deliver the FIN and let the peer create its ACK, but hold that ACK back.
    let acknowledgments = pair
        .b
        .core
        .conn_mut(server.handle())
        .unwrap()
        .conn_mut()
        .stats()
        .frame_tx
        .acks;
    pair.a.pump();
    while let Some(tx) = pair.a.pending_transmits.pop_front() {
        pair.b
            .core
            .handle_datagram(Instant::now(), pair.a.shared.local, &tx.contents);
    }
    pair.b.pump();
    // ACKs may be delayed. Drive the peer's actual protocol deadlines without
    // delivering its packets to the sender, rather than assuming an immediate
    // transmit or relying on the wall-clock speed of the test machine.
    tokio::time::timeout(Duration::from_secs(2), async {
        while pair
            .b
            .core
            .conn_mut(server.handle())
            .unwrap()
            .conn_mut()
            .stats()
            .frame_tx
            .acks
            == acknowledgments
        {
            let deadline = pair.b.core.next_protocol_timeout().unwrap();
            tokio::time::sleep_until(deadline.into()).await;
            pair.b.core.handle_timeout(Instant::now());
            pair.b.pump();
        }
    })
    .await
    .expect("peer must generate the withheld ACK");
    assert!(!pair.b.pending_transmits.is_empty());

    let mut reset = Box::pin(send.reset(41));
    assert!(reset
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    pair.a.control();
    assert!(matches!(
        reset.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(crate::conn::ResetOutcome::ResetRequested))
    ));
    drop(reset);

    pair.drive();
    assert_eq!(
        pair.complete(send.reset(99)).unwrap(),
        crate::conn::ResetOutcome::AlreadyReset { code: 41 }
    );
    assert_eq!(
        pair.complete(send.wait_finished()).unwrap_err(),
        crate::conn::ConnError::ClosedStream
    );
}

#[tokio::test]
async fn pending_and_accepted_streams_share_credit_and_drops_restore_it() {
    let (mut pair, _a, _b, client, server) = ManualPair::connected().await;
    let mut client_streams = Vec::new();
    for _ in 0..32 {
        client_streams.push(pair.complete(client.open_bi()).unwrap());
    }
    pair.complete(client_streams[31].0.write_all(b"materialize"))
        .unwrap();
    let mut blocked = Box::pin(client.open_bi());
    assert!(blocked
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    pair.a.control();

    // Accepting does not return credit while the accepted stream remains live.
    let mut accepted = Vec::new();
    for _ in 0..32 {
        accepted.push(pair.complete(server.accept_bi()).unwrap());
    }
    pair.drive();
    assert!(blocked
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());

    // Fully close both directions. Merely accepting did not return credit;
    // consuming both FINs does, and wakes the oldest pending opener.
    for (client_stream, server_stream) in client_streams.iter_mut().zip(accepted.iter_mut()) {
        pair.complete(client_stream.0.finish()).unwrap();
        pair.complete(server_stream.0.finish()).unwrap();
    }
    pair.drive();
    for (index, (client_stream, server_stream)) in client_streams
        .iter_mut()
        .zip(accepted.iter_mut())
        .enumerate()
    {
        assert!(pair
            .complete(client_stream.1.read_to_end(64))
            .unwrap()
            .is_empty());
        let received = pair.complete(server_stream.1.read_to_end(64)).unwrap();
        if index == 31 {
            assert_eq!(received, b"materialize");
        } else {
            assert!(received.is_empty());
        }
    }
    client_streams.clear();
    accepted.clear();
    pair.drive();
    let opened = pair.complete(blocked).unwrap();
    assert_eq!(opened.0.id(), StreamId::new(Side::Client, Dir::Bi, 32));
    drop((opened, accepted, client_streams));
}

#[tokio::test]
async fn try_open_reports_local_pressure_and_recovers() {
    let (mut pair, _a, _b, client, server) = ManualPair::connected().await;

    let mut queued = Vec::new();
    for _ in 0..COMMAND_CAPACITY {
        let mut accept = Box::pin(tokio::task::unconstrained(client.accept_bi()));
        assert!(accept
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        queued.push(accept);
    }
    assert_eq!(pair.a.command_rx.len(), COMMAND_CAPACITY);
    assert!(matches!(
        pair.complete(client.try_open_bi()).unwrap(),
        crate::conn::TryOpenOutcome::TemporarilyUnavailable
    ));
    drop(queued);
    pair.drive();
    assert!(matches!(
        pair.complete(client.try_open_bi()).unwrap(),
        crate::conn::TryOpenOutcome::Opened(_)
    ));

    let mut operations = Vec::new();
    for _ in 0..256 {
        let mut open = Box::pin(tokio::task::unconstrained(client.open_bi()));
        assert!(open
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        pair.a.control();
        operations.push(open);
    }
    assert!(matches!(
        pair.complete(client.try_open_bi()).unwrap(),
        crate::conn::TryOpenOutcome::TemporarilyUnavailable
    ));
    drop(operations);
    pair.drive();
    assert_eq!(client.available_open_budget(), 256);
    pair.b
        .core
        .conn_mut(server.handle())
        .unwrap()
        .conn_mut()
        .set_max_concurrent_streams(Dir::Bi, VarInt::from_u32(512));
    pair.b.pump();
    pair.drive();
    assert!(matches!(
        pair.complete(client.try_open_bi()).unwrap(),
        crate::conn::TryOpenOutcome::Opened(_)
    ));
}

#[tokio::test]
async fn canceled_handoff_wakes_are_coalesced_outside_the_cleanup_queue() {
    let (mut pair, _a, _b, client, _server) = ManualPair::connected().await;
    let (_send, mut recv) = pair.complete(client.open_bi()).unwrap();
    let mut queued = Vec::new();
    for _ in 0..COMMAND_CAPACITY {
        let mut accept = Box::pin(tokio::task::unconstrained(client.accept_bi()));
        assert!(accept
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        queued.push(accept);
    }
    assert_eq!(pair.a.command_rx.len(), COMMAND_CAPACITY);
    let cleanup_len = pair.a.cleanup_rx.len();

    for _ in 0..1024 {
        let mut read = Box::pin(tokio::task::unconstrained(recv.read(1)));
        assert!(read
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        drop(read);
    }
    assert_eq!(
        pair.a.cleanup_rx.len(),
        cleanup_len,
        "cancellation-only wakes must not consume unbounded cleanup slots"
    );
    let mut notified = Box::pin(pair.a.shared.wake.notified());
    assert!(notified
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_ready());
    let mut second = Box::pin(pair.a.shared.wake.notified());
    assert!(second
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    drop((second, notified, queued));
}

#[tokio::test]
async fn write_byte_budget_saturates_in_chunks_and_recovers_on_cancellation() {
    let (mut pair, _a, _b, client, server) = ManualPair::connected().await;
    let mut streams = Vec::new();
    for _ in 0..17 {
        streams.push(pair.complete(client.open_bi()).unwrap().0);
    }
    let payload = vec![7u8; 1024 * 1024];
    let (saturating, recovered) = streams.split_at_mut(16);
    let mut writes = Vec::new();
    for send in saturating {
        let mut write = Box::pin(send.write_all(&payload));
        assert!(write
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        writes.push(write);
    }
    assert_eq!(client.available_write_budget(), 0);
    assert_eq!(pair.a.command_rx.len(), 16);

    let recovered_id = recovered[0].id();
    let mut blocked = Box::pin(recovered[0].write_all(b"after budget"));
    assert!(blocked
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    assert_eq!(pair.a.command_rx.len(), 16, "budget blocks before enqueue");

    drop(writes);
    pair.a.control();
    assert_eq!(
        client.available_write_budget(),
        256 * 1024 - b"after budget".len()
    );
    assert!(blocked
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    pair.a.control();
    assert!(matches!(
        blocked
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
    drop(blocked);

    pair.drive();
    for _ in 0..17 {
        let (_send, mut recv) = pair.complete(server.accept_bi()).unwrap();
        if recv.id() == recovered_id {
            assert_eq!(pair.complete(recv.read(64)).unwrap(), b"after budget");
            return;
        }
    }
    panic!("recovered write stream was not accepted");
}
