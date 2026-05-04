import Foundation

/// Auth dependency consumed by `APIClient` and `WebSocketManager`.
///
/// Production uses `AuthService.shared` (conformance added via extension).
/// Tests inject `FakeAuthService` (actor) to drive reauth + logout paths
/// deterministically without hitting a network.
protocol AuthRefreshing: AnyObject, Sendable {
    func fetchFreshWsToken() async -> String?
    func logout() async
}

/// Abstraction over `URLSessionWebSocketTask`.
///
/// Must expose exactly the surface `WebSocketManager` invokes; keep this
/// protocol minimal to avoid fake-test drift. `cancel` and `resume` are
/// synchronous nonisolated requirements on `URLSessionWebSocketTask`, so
/// this protocol mirrors that — any fake must be `@unchecked Sendable`
/// with internal synchronization.
///
/// `Sendable` is required because the manager hands tasks to detached
/// `Task` closures for async `receive()` loops.
protocol WebSocketTaskProtocol: AnyObject, Sendable {
    func send(_ message: URLSessionWebSocketTask.Message) async throws
    func receive() async throws -> URLSessionWebSocketTask.Message
    func cancel(with closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?)
    func resume()
}

extension URLSessionWebSocketTask: WebSocketTaskProtocol {}

/// Factory for creating `WebSocketTaskProtocol` instances.
///
/// Production uses `URLSessionWebSocketTaskFactory` wrapping a concrete
/// `URLSession`. Tests inject `FakeWebSocketTaskFactory` that hands out
/// pumpable `FakeWebSocketTask` instances.
protocol WebSocketTaskFactory: Sendable {
    func makeTask(request: URLRequest) -> WebSocketTaskProtocol
}

struct URLSessionWebSocketTaskFactory: WebSocketTaskFactory {
    let session: URLSession

    func makeTask(request: URLRequest) -> WebSocketTaskProtocol {
        return session.webSocketTask(with: request)
    }
}

/// Abstraction over `Task.sleep` used by proactive token refresh (Commit 2).
///
/// Production uses `TaskSleeper` (wraps `Task.sleep`). Tests inject
/// `FakeSleeper` (actor) that records requested intervals and resolves
/// immediately so refresh scheduling can be asserted without wall-clock
/// dependency.
protocol Sleeper: Sendable {
    func sleep(seconds: TimeInterval) async throws
}

struct TaskSleeper: Sleeper {
    func sleep(seconds: TimeInterval) async throws {
        try await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
    }
}
