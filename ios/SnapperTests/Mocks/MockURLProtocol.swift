import Foundation
@testable import Snapper

class MockURLProtocol: URLProtocol {

    static let stubURL = "https://test.com"

    private static let _lock = NSLock()
    private nonisolated(unsafe) static var _handler: ((URLRequest) throws -> (HTTPURLResponse, Data?))?

    static var requestHandler: ((URLRequest) throws -> (HTTPURLResponse, Data?))? {
        get { _lock.withLock { _handler } }
        set { _lock.withLock { _handler = newValue } }
    }

    override class func canInit(with request: URLRequest) -> Bool {
        /* Always intercept: all requests are handled by the mock */
        return true
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        return request
    }

    override func startLoading() {
        guard let handler = MockURLProtocol.requestHandler else {
            fatalError("Request handler is not set")
        }

        do {
            let (response, data) = try handler(request)

            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)

            if let data = data {
                client?.urlProtocol(self, didLoad: data)
            }

            client?.urlProtocolDidFinishLoading(self)
        } catch {
            /* Forward the error to the URL loading system */
            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    override func stopLoading() {
        /* No-op: cancellation is not needed for mock responses */
    }
}

extension MockURLProtocol {

    static func jsonResponse(statusCode: Int, json: [String: Any]) -> (HTTPURLResponse, Data?) {
        let data = try? JSONSerialization.data(withJSONObject: json)
        let response = HTTPURLResponse(
            url: URL(string: stubURL)!,
            statusCode: statusCode,
            httpVersion: nil,
            headerFields: [AppConfig.HTTPHeader.contentType: AppConfig.ContentType.json]
        )!
        return (response, data)
    }

    static func errorResponse(statusCode: Int, message: String) -> (HTTPURLResponse, Data?) {
        let json = ["detail": message]
        return jsonResponse(statusCode: statusCode, json: json)
    }

    static func networkError() -> Error {
        return NSError(
            domain: NSURLErrorDomain,
            code: NSURLErrorNotConnectedToInternet,
            userInfo: [NSLocalizedDescriptionKey: "Network unavailable"]
        )
    }
}
