import Foundation
@testable import Snapper

/// Actor-based `Sleeper` stub used by Commit 2 proactive-refresh tests.
/// Records requested intervals and resolves immediately so scheduling
/// behavior can be asserted without wall-clock dependency.
actor FakeSleeper: Sleeper {
    var requestedIntervals: [TimeInterval] = []

    func sleep(seconds: TimeInterval) async throws {
        requestedIntervals.append(seconds)
    }
}
