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
}
