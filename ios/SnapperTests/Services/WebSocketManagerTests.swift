import XCTest
@testable import Snapper

@MainActor
final class WebSocketManagerTests: XCTestCase {

    private func makeManager(
        fakeAuth: FakeAuthService = FakeAuthService(nextToken: "test-token"),
        fakeTask: FakeWebSocketTask = FakeWebSocketTask(),
        fakeSleeper: FakeSleeper = FakeSleeper()
    ) -> (WebSocketManager, FakeAuthService, FakeWebSocketTask, FakeSleeper) {
        let factory = FakeWebSocketTaskFactory(task: fakeTask)
        let manager = WebSocketManager(
            authService: fakeAuth,
            taskFactory: factory,
            sleeper: fakeSleeper
        )
        return (manager, fakeAuth, fakeTask, fakeSleeper)
    }

    private func frame(_ json: [String: Any]) -> URLSessionWebSocketTask.Message {
        let data = try! JSONSerialization.data(withJSONObject: json)
        return .string(String(data: data, encoding: .utf8)!)
    }

    func testTradeFrameDispatchedToLastTrade() {
        let (manager, _, _, _) = makeManager()

        manager.handleRawMessage(frame([
            "type": "trade",
            "sequence_id": 1,
            "public_id": "01961234-5678-7000-8000-000000000001",
            "timestamp": "2025-11-22T10:00:00Z",
            "session_id": "session-1",
            "instrument": "BTCUSD",
            "exchange": "kraken",
            "price": 50000.0,
            "volume": 0.5
        ]))

        XCTAssertNotNil(manager.state.lastTrade)
        XCTAssertEqual(manager.state.lastTrade?.instrument, "BTCUSD")
        XCTAssertEqual(manager.state.lastTrade?.price, 50000.0)
    }

    func testOrderEventFrameDispatchedToLastOrderEvent() {
        let (manager, _, _, _) = makeManager()

        manager.handleRawMessage(frame([
            "type": "order_event",
            "sequence_id": 2,
            "public_id": "01961234-5678-7000-8000-000000000002",
            "timestamp": "2025-11-22T10:00:00Z",
            "session_id": "session-1",
            "exchange_order_id": "ex-123",
            "client_order_id": "cli-456",
            "exchange": "kraken",
            "instrument": "BTCUSD",
            "event": "filled"
        ]))

        XCTAssertNotNil(manager.state.lastOrderEvent)
        XCTAssertEqual(manager.state.lastOrderEvent?.event, "filled")
        XCTAssertEqual(manager.state.lastOrderEvent?.clientOrderId, "cli-456")
    }

    func testUnparseableFrameLogsNoCrash() {
        let (manager, _, _, _) = makeManager()

        manager.handleRawMessage(.string("{not valid json"))
        manager.handleRawMessage(frame(["type": "trade"]))

        XCTAssertNil(manager.state.lastTrade)
        XCTAssertNil(manager.state.lastOrderEvent)
    }

    /// `sendJSON` bounces through a detached Task to call `task.send`
    /// asynchronously. Yield to give that Task a chance to run before
    /// asserting on `sentMessages`.
    private func drainSendTasks() async {
        for _ in 0..<10 { await Task.yield() }
    }

    func testSubscribePendsWhileConnectingReplaysOnConnect() async {
        let (manager, _, fakeTask, _) = makeManager()
        manager.connect()

        manager.subscribe(topics: ["orders.events.kraken."])
        await drainSendTasks()
        XCTAssertEqual(fakeTask.sentMessages.count, 0, "pending sub should NOT be sent while connecting")

        manager.handleRawMessage(frame([
            "type": "auth_complete",
            "sequence_id": 10,
            "public_id": "01961234-5678-7000-8000-000000000010",
            "timestamp": "2025-11-22T10:00:00Z",
            "session_id": "session-1",
            "available_topics": ["orders.events.kraken."],
            "user_role": "viewer",
            "ws_token_exp": "2025-11-22T11:00:00Z"
        ]))
        await drainSendTasks()

        XCTAssertGreaterThan(fakeTask.sentMessages.count, 0)
        let hasSubscribe = fakeTask.sentMessages.contains { msg in
            if case .string(let s) = msg, s.contains("subscribe") { return true }
            return false
        }
        XCTAssertTrue(hasSubscribe, "replayed subscribe frame missing from sent messages")
    }

    func testSubscribedTopicsPreservedAcrossReconnect() async {
        let task1 = FakeWebSocketTask()
        let task2 = FakeWebSocketTask()
        let factory = FakeWebSocketTaskFactory(tasks: [task1, task2])
        let manager = WebSocketManager(
            authService: FakeAuthService(nextToken: "t"),
            taskFactory: factory,
            sleeper: FakeSleeper()
        )
        manager.connect()

        manager.handleRawMessage(frame([
            "type": "auth_complete",
            "sequence_id": 1,
            "public_id": "01961234-5678-7000-8000-000000000020",
            "timestamp": "2025-11-22T10:00:00Z",
            "session_id": "session-1",
            "available_topics": [],
            "user_role": "viewer",
            "ws_token_exp": "2025-11-22T11:00:00Z"
        ]))

        manager.subscribe(topics: ["orders.events.kraken."])
        await drainSendTasks()
        XCTAssertGreaterThan(task1.sentMessages.count, 0, "first connect should have sent subscribe directly")

        // Simulate a user-initiated disconnect + reconnect — covers the
        // explicit-close path. Network-drop + auto-reconnect is covered by
        // the infinite-backoff tests in Commit 2.
        manager.disconnect()
        manager.connect()
        manager.handleRawMessage(frame([
            "type": "auth_complete",
            "sequence_id": 2,
            "public_id": "01961234-5678-7000-8000-000000000021",
            "timestamp": "2025-11-22T10:00:00Z",
            "session_id": "session-2",
            "available_topics": [],
            "user_role": "viewer",
            "ws_token_exp": "2025-11-22T11:00:00Z"
        ]))
        await drainSendTasks()

        let task2Subscribes = task2.sentMessages.contains { msg in
            if case .string(let s) = msg, s.contains("orders.events.kraken.") { return true }
            return false
        }
        XCTAssertTrue(task2Subscribes, "confirmed topic not replayed on reconnect — regression in dual-set tracking")
    }

    /// A stale `receive()` error from task A (resolved after the manager
    /// has already swapped to task B) must not mutate shared state.
    /// Without identity guarding on the error path, the old task would
    /// call `handleDisconnection()` and wipe out the live task B.
    func testStaleReceiveErrorDoesNotDisruptNewSocket() async {
        let task1 = FakeWebSocketTask()
        let task2 = FakeWebSocketTask()
        let factory = FakeWebSocketTaskFactory(tasks: [task1, task2])
        let manager = WebSocketManager(
            authService: FakeAuthService(nextToken: "t"),
            taskFactory: factory,
            sleeper: FakeSleeper()
        )

        manager.connect()
        manager.handleRawMessage(frame([
            "type": "auth_complete",
            "sequence_id": 1,
            "public_id": "01961234-5678-7000-8000-000000000040",
            "timestamp": "2025-11-22T10:00:00Z",
            "session_id": "s1",
            "available_topics": [],
            "user_role": "viewer",
            "ws_token_exp": "2025-11-22T11:00:00Z"
        ]))
        XCTAssertEqual(manager.connectionState, .connected)

        // Simulate a full reconnect swap BEFORE task1's receive() resumes.
        manager.disconnect()
        manager.connect()
        manager.handleRawMessage(frame([
            "type": "auth_complete",
            "sequence_id": 2,
            "public_id": "01961234-5678-7000-8000-000000000041",
            "timestamp": "2025-11-22T10:00:00Z",
            "session_id": "s2",
            "available_topics": [],
            "user_role": "viewer",
            "ws_token_exp": "2025-11-22T11:00:00Z"
        ]))
        XCTAssertEqual(manager.connectionState, .connected, "task2 should own connected state")

        // Now resume task1's receive() with a stale error. If the guard is
        // missing, this call will trip handleDisconnection() and flip state
        // back to .disconnected.
        task1.pumpError(URLError(.networkConnectionLost))
        await drainSendTasks()
        await drainSendTasks()

        XCTAssertEqual(manager.connectionState, .connected, "stale error from task1 must not disrupt task2")
    }

    func testAuthCompleteDecodedAndAppliedToState() {
        let (manager, _, _, _) = makeManager()
        manager.connect()

        manager.handleRawMessage(frame([
            "type": "auth_complete",
            "sequence_id": 5,
            "public_id": "01961234-5678-7000-8000-000000000030",
            "timestamp": "2025-11-22T10:00:00Z",
            "session_id": "session-1",
            "available_topics": ["trades.kraken.", "orders.events.kraken."],
            "user_role": "operator",
            "ws_token_exp": "2025-11-22T11:00:00Z"
        ]))

        XCTAssertEqual(manager.availableTopics.sorted(), ["orders.events.kraken.", "trades.kraken."])
        XCTAssertNotNil(manager.state.wsTokenExp)
        XCTAssertEqual(manager.connectionState, .connected)
    }

    // MARK: - Commit 2 (FE-2) coverage

    /// When `fetchFreshWsToken` returns nil during `reauth_required`,
    /// the manager must flip to `.authFailed`, cancel the socket, clear
    /// `shouldReconnect`, and call `AuthService.logout()` exactly once.
    func testAuthFailureTriggersLogout() async {
        let fakeAuth = FakeAuthService(nextToken: nil)
        let fakeTask = FakeWebSocketTask()
        let factory = FakeWebSocketTaskFactory(task: fakeTask)
        let manager = WebSocketManager(
            authService: fakeAuth,
            taskFactory: factory,
            sleeper: FakeSleeper()
        )
        manager.connect()

        manager.handleRawMessage(frame(["type": "reauth_required"]))
        await drainSendTasks()
        await drainSendTasks()

        if case .authFailed(let reason) = manager.connectionState {
            XCTAssertFalse(reason.isEmpty, "authFailed state must carry a reason string")
        } else {
            XCTFail("expected .authFailed state, got \(manager.connectionState)")
        }
        let logoutCalls = await fakeAuth.logoutCalls
        XCTAssertEqual(logoutCalls, 1, "authService.logout() must be invoked exactly once")
    }

    /// Direct unit test on `nextReconnectDelay()` — per plan §D5 the
    /// delay should be bounded by `[baseInterval * 2^n, baseInterval * 2^n * 1.3]`
    /// up to a 300s cap, with infinite-attempt semantics (no hard attempt
    /// ceiling). Exposed `internal` via `@testable import`.
    func testReconnectBackoffExponentialWithJitter() {
        let fakeTask = FakeWebSocketTask()
        let factory = FakeWebSocketTaskFactory(task: fakeTask)
        let manager = WebSocketManager(
            authService: FakeAuthService(),
            taskFactory: factory,
            sleeper: FakeSleeper()
        )
        let base: TimeInterval = 3

        // Drive attempts forward via disconnect() + connect()? Simpler —
        // the test-only setter `setReconnectAttemptsForTesting` is
        // exposed via @testable below. Here we force-dispatch by
        // directly mutating via repeated `handleDisconnection` calls
        // that we reach through the public surface.
        //
        // We use the simplest reachable path: set should-reconnect false
        // to avoid the async-after fire, set reconnectAttempts through a
        // dedicated helper, and read the returned delay.

        let cases: [Int] = [1, 5, 10, 50]
        for attempts in cases {
            manager.setReconnectAttemptsForTesting(attempts)
            let delay = manager.nextReconnectDelay()
            let idealized = min(base * pow(2.0, Double(attempts - 1)), 300)
            let lower = idealized
            let upper = idealized * 1.3
            XCTAssertGreaterThanOrEqual(delay, lower, "delay should be at least the base for attempts=\(attempts)")
            XCTAssertLessThanOrEqual(delay, upper, "delay should not exceed jittered upper bound for attempts=\(attempts)")
        }
    }

    /// `handleDisconnection()` must keep incrementing `reconnectAttempts`
    /// past the pre-Plan-1 hard cap of 10 — infinite retry semantics.
    func testReconnectAttemptsIncrementsUnbounded() async {
        let fakeTask = FakeWebSocketTask()
        let factory = FakeWebSocketTaskFactory(tasks: [fakeTask, FakeWebSocketTask(), FakeWebSocketTask()])
        let manager = WebSocketManager(
            authService: FakeAuthService(),
            taskFactory: factory,
            sleeper: FakeSleeper()
        )
        // Pre-seed to a value well past the old maxReconnectAttempts=10.
        manager.setReconnectAttemptsForTesting(11)

        // Drive one more disconnect; the attempt counter must advance.
        let previous = manager.reconnectAttempts
        manager.connect()
        // Simulate a wire-level drop that would previously have been
        // suppressed at attempts >= 10.
        manager.forceHandleDisconnectionForTesting()
        XCTAssertEqual(manager.reconnectAttempts, previous + 1, "reconnectAttempts must keep advancing past the old cap")
    }

    /// Proactive refresh should request a sleep equal to 80% of the
    /// advertised TTL. Uses `FakeSleeper` so no wall-clock dependency.
    func testProactiveRefreshFiresAt80Percent() async {
        let fakeAuth = FakeAuthService(nextToken: "fresh")
        let fakeSleeper = FakeSleeper()
        let fakeTask = FakeWebSocketTask()
        let factory = FakeWebSocketTaskFactory(task: fakeTask)
        let manager = WebSocketManager(
            authService: fakeAuth,
            taskFactory: factory,
            sleeper: fakeSleeper
        )
        manager.connect()

        // TTL = 100s — so 80% fire target = 80s.
        let exp = Date(timeIntervalSinceNow: 100)
        let isoFormatter = ISO8601DateFormatter()
        let expString = isoFormatter.string(from: exp)
        manager.handleRawMessage(frame([
            "type": "auth_complete",
            "sequence_id": 1,
            "public_id": "01961234-5678-7000-8000-000000000050",
            "timestamp": "2025-11-22T10:00:00Z",
            "session_id": "s1",
            "available_topics": [],
            "user_role": "viewer",
            "ws_token_exp": expString
        ]))
        // Yield enough for the Task { try await sleeper.sleep(seconds:) }
        // to register its interval.
        await drainSendTasks()
        await drainSendTasks()

        let requested = await fakeSleeper.requestedIntervals
        XCTAssertEqual(requested.count, 1, "proactive refresh should sleep exactly once per auth_complete")
        XCTAssertEqual(requested.first ?? -1, 80, accuracy: 1.0, "scheduled interval must equal 80% of TTL")
    }
}

