//
//  CocoaMQTTServerNameTests.swift
//  CocoaMQTTTests
//
//  Regression coverage for SNI peer name: the SNI / TLS peer name must be
//  settable independently of the connect host so callers can drive the
//  TCP connect at an IP literal while keeping cert validation on the
//  original FQDN.
//

import XCTest
@testable import CocoaMQTT
#if IS_SWIFT_PACKAGE
@testable import CocoaMQTTWebSocket
#endif

final class CocoaMQTTServerNameTests: XCTestCase {

    // MARK: - Native TLS path (CocoaMQTTSocket)

    func testSocketServerNameDefaultsToNil() {
        let socket = CocoaMQTTSocket()
        XCTAssertNil(socket.serverName)
    }

    func testSocketServerNameRoundTrips() {
        let socket = CocoaMQTTSocket()
        socket.serverName = "broker.example.com"
        XCTAssertEqual(socket.serverName, "broker.example.com")
        socket.serverName = nil
        XCTAssertNil(socket.serverName)
    }

    func testCocoaMQTTForwardsServerNameToSocket() {
        let socket = CocoaMQTTSocket()
        let mqtt = CocoaMQTT(clientID: "test-3", host: "1.2.3.4", port: 8883, socket: socket)
        XCTAssertNil(mqtt.serverName)

        mqtt.serverName = "broker.example.com"
        XCTAssertEqual(socket.serverName, "broker.example.com")
        XCTAssertEqual(mqtt.serverName, "broker.example.com")
    }

    func testCocoaMQTT5ForwardsServerNameToSocket() {
        let socket = CocoaMQTTSocket()
        let mqtt = CocoaMQTT5(clientID: "test-5", host: "1.2.3.4", port: 8883, socket: socket)
        XCTAssertNil(mqtt.serverName)

        mqtt.serverName = "broker.example.com"
        XCTAssertEqual(socket.serverName, "broker.example.com")
        XCTAssertEqual(mqtt.serverName, "broker.example.com")
    }

    // MARK: - Native TLS path: SNI feeds into startTLS settings

    func testTLSStartSettingsInjectsServerNameAsPeerName() {
        let socket = CocoaMQTTSocket()
        socket.enableSSL = true
        socket.serverName = "broker.example.com"
        let settings = socket.tlsStartSettings()
        XCTAssertEqual(
            settings[kCFStreamSSLPeerName as String] as? String,
            "broker.example.com",
        )
    }

    func testTLSStartSettingsOmitsPeerNameWhenServerNameNil() {
        let socket = CocoaMQTTSocket()
        socket.enableSSL = true
        let settings = socket.tlsStartSettings()
        XCTAssertNil(settings[kCFStreamSSLPeerName as String])
    }

    func testTLSStartSettingsServerNameOverridesUserSslSettings() {
        let socket = CocoaMQTTSocket()
        socket.enableSSL = true
        socket.sslSettings = [
            kCFStreamSSLPeerName as String: "first.example.com" as NSString,
        ]
        socket.serverName = "second.example.com"
        let settings = socket.tlsStartSettings()
        XCTAssertEqual(
            settings[kCFStreamSSLPeerName as String] as? String,
            "second.example.com",
        )
    }

    func testTLSStartSettingsPreservesUnrelatedUserSslSettings() {
        let socket = CocoaMQTTSocket()
        socket.enableSSL = true
        socket.sslSettings = [
            kCFStreamSSLIsServer as String: NSNumber(value: false),
        ]
        socket.serverName = "broker.example.com"
        let settings = socket.tlsStartSettings()
        XCTAssertEqual(settings[kCFStreamSSLIsServer as String] as? NSNumber, NSNumber(value: false))
        XCTAssertEqual(settings[kCFStreamSSLPeerName as String] as? String, "broker.example.com")
    }

    // MARK: - WebSocket path

    func testWebSocketServerNameMirrorsInHeader() {
        let ws = CocoaMQTTWebSocket()
        XCTAssertNil(ws.serverName)
        XCTAssertNil(ws.headers[CocoaMQTTWebSocket.sniHeaderKey])

        ws.serverName = "broker.example.com"
        XCTAssertEqual(ws.headers[CocoaMQTTWebSocket.sniHeaderKey], "broker.example.com")

        ws.serverName = nil
        XCTAssertNil(ws.headers[CocoaMQTTWebSocket.sniHeaderKey])
    }

    func testWebSocketBuildConnectionReceivesServerNameInHeaders() throws {
        let recorder = RecordingWSConnectionBuilder()
        let ws = CocoaMQTTWebSocket(uri: "/mqtt", builder: recorder)
        ws.serverName = "broker.example.com"

        try ws.connect(toHost: "127.0.0.1", onPort: 1234)

        XCTAssertEqual(
            recorder.lastHeaders[CocoaMQTTWebSocket.sniHeaderKey],
            "broker.example.com",
        )
    }

    func testWebSocketBuildConnectionOmitsSniHeaderWhenServerNameNil() throws {
        let recorder = RecordingWSConnectionBuilder()
        let ws = CocoaMQTTWebSocket(uri: "/mqtt", builder: recorder)

        try ws.connect(toHost: "127.0.0.1", onPort: 1234)

        XCTAssertNil(recorder.lastHeaders[CocoaMQTTWebSocket.sniHeaderKey])
    }
}

// MARK: - WS builder/connection stubs

private final class RecordingWSConnectionBuilder: CocoaMQTTWebSocketConnectionBuilder {
    var lastURL: URL?
    var lastHeaders: [String: String] = [:]

    func buildConnection(forURL url: URL, withHeaders headers: [String: String]) throws -> CocoaMQTTWebSocketConnection {
        lastURL = url
        lastHeaders = headers
        return StubWSConnection()
    }
}

private final class StubWSConnection: NSObject, CocoaMQTTWebSocketConnection {
    var delegate: CocoaMQTTWebSocketConnectionDelegate?
    var queue: DispatchQueue = DispatchQueue(label: "stub.ws")
    func connect() {}
    func disconnect() {}
    func write(data: Data, handler: @escaping (Error?) -> Void) { handler(nil) }
}
