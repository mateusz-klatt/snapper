import Foundation

class MockURLProtocol: URLProtocol {

    static var requestHandler: ((URLRequest) throws -> (HTTPURLResponse, Data?))?

    override class func canInit(with request: URLRequest) -> Bool {

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

            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    override func stopLoading() {

    }
}

extension MockURLProtocol {

    static func jsonResponse(statusCode: Int, json: [String: Any]) -> (HTTPURLResponse, Data?) {
        let data = try? JSONSerialization.data(withJSONObject: json)
        let response = HTTPURLResponse(
            url: URL(string: "https://test.com")!,
            statusCode: statusCode,
            httpVersion: nil,
            headerFields: ["Content-Type": "application/json"]
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
