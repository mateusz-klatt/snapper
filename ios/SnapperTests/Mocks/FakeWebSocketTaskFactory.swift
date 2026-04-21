import Foundation
@testable import Snapper

/// Hands out pre-built `FakeWebSocketTask` instances. The round-robin
/// `index` counter is NSLock-serialized to honor `Sendable`, matching
/// the pattern documented for `FakeWebSocketTask`.
final class FakeWebSocketTaskFactory: @unchecked Sendable, WebSocketTaskFactory {
    let tasks: [FakeWebSocketTask]
    private let lock = NSLock()
    private var index = 0

    init(tasks: [FakeWebSocketTask]) {
        self.tasks = tasks
    }

    /// Convenience for single-connect tests.
    convenience init(task: FakeWebSocketTask) {
        self.init(tasks: [task])
    }

    func makeTask(request: URLRequest) -> WebSocketTaskProtocol {
        lock.withLock {
            let task = tasks[min(index, tasks.count - 1)]
            index += 1
            return task
        }
    }
}
