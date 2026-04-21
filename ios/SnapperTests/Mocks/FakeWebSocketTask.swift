import Foundation
@testable import Snapper

/// `@unchecked Sendable` + NSLock fake for `WebSocketTaskProtocol`.
///
/// Actor isolation is unworkable: the protocol's `cancel` and `resume`
/// are synchronous nonisolated requirements. Actor conformance would need
/// `nonisolated` wrappers that cannot safely touch isolated state.
/// NSLock-serialized mutable state is the honest pragmatic choice —
/// production `URLSessionWebSocketTask` has the same constraint.
///
/// Tests drive inbound frames by calling `pumpInbound(_:)`. A pending
/// `receive()` caller is parked on an async continuation and resumed
/// once a frame lands in the queue.
final class FakeWebSocketTask: @unchecked Sendable, WebSocketTaskProtocol {
    private let lock = NSLock()
    private var inboundQueue: [URLSessionWebSocketTask.Message] = []
    private var waiter: CheckedContinuation<URLSessionWebSocketTask.Message, Error>?
    private(set) var sentMessages: [URLSessionWebSocketTask.Message] = []
    private(set) var resumeCount = 0
    private(set) var cancelCount = 0
    private(set) var lastCancelCode: URLSessionWebSocketTask.CloseCode?

    func pumpInbound(_ msg: URLSessionWebSocketTask.Message) {
        let resumed: CheckedContinuation<URLSessionWebSocketTask.Message, Error>? = lock.withLock {
            if let w = waiter {
                waiter = nil
                return w
            }
            inboundQueue.append(msg)
            return nil
        }
        resumed?.resume(returning: msg)
    }

    func pumpError(_ error: Error) {
        let resumed: CheckedContinuation<URLSessionWebSocketTask.Message, Error>? = lock.withLock {
            let w = waiter
            waiter = nil
            return w
        }
        resumed?.resume(throwing: error)
    }

    func send(_ message: URLSessionWebSocketTask.Message) async throws {
        lock.withLock { sentMessages.append(message) }
    }

    func receive() async throws -> URLSessionWebSocketTask.Message {
        return try await withCheckedThrowingContinuation { continuation in
            // Single atomic critical section: either consume a queued
            // message OR install the waiter. Splitting the lock would leave
            // a gap where `pumpInbound` enqueues after our empty-check but
            // before waiter install — the continuation would then hang
            // forever even though a message is sitting in the queue.
            let immediate: URLSessionWebSocketTask.Message? = lock.withLock {
                if !inboundQueue.isEmpty {
                    return inboundQueue.removeFirst()
                }
                waiter = continuation
                return nil
            }
            if let msg = immediate {
                continuation.resume(returning: msg)
            }
        }
    }

    func cancel(with closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?) {
        let resumed: CheckedContinuation<URLSessionWebSocketTask.Message, Error>? = lock.withLock {
            cancelCount += 1
            lastCancelCode = closeCode
            let w = waiter
            waiter = nil
            return w
        }
        resumed?.resume(throwing: CancellationError())
    }

    func resume() {
        lock.withLock { resumeCount += 1 }
    }
}
