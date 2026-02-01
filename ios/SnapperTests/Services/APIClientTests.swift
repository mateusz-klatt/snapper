import XCTest
@testable import Snapper

final class APIClientTests: XCTestCase {

    func testAPIErrorDescription() {
        XCTAssertEqual(APIError.invalidURL.errorDescription, "Invalid URL")
        XCTAssertEqual(APIError.invalidResponse.errorDescription, "Invalid response from server")
        XCTAssertEqual(APIError.httpError(404).errorDescription, "HTTP error: 404")
        XCTAssertEqual(APIError.serverError("Test error").errorDescription, "Test error")
        XCTAssertEqual(APIError.decodingError.errorDescription, "Failed to decode response")
    }
}
