//
//  CocoaMQTTMessage.swift
//  CocoaMQTT
//
//  Created by Feng Lee<feng@eqmtt.io> on 14/8/3.
//  Copyright (c) 2015 emqx.io. All rights reserved.
//

import Foundation

/// MQTT Message
public class CocoaMQTTMessage: NSObject {

    public var qos = CocoaMQTTQoS.qos1

    public var topic: String

    public var payload: [UInt8]

    public var retained = false

    /// The `duplicated` property show that this message maybe has be received before
    ///
    /// - note: Readonly property
    public var duplicated = false

    /// Send this QoS 1 PUBLISH once and skip client-side retransmit.
    ///
    /// When `true` and `qos == .qos1`, the client transmits the PUBLISH a
    /// single time with no inflight bookkeeping, no awaiting timer, and no
    /// persistent-storage write. PUBACK arrival from the broker is still
    /// surfaced to the application via the existing `didPublishAck`
    /// delegate callback, so callers can observe delivery completion
    /// without the duplicate-PUBLISH amplification of the standard retry
    /// loop. The flag has no effect for QoS 0 or QoS 2.
    ///
    /// Recommended companions:
    /// - For MQTT 5, set
    ///   `MqttPublishProperties.messageExpiryInterval` so the broker bounds
    ///   how long it will hold the message for offline subscribers (the
    ///   client side no longer keeps a copy to resend).
    /// - On connect, set `MqttConnectProperties.receiveMaximum` so the
    ///   broker enforces a server-side cap on concurrent unacknowledged
    ///   publishes - replacing the natural rate-limit that the inflight
    ///   window provided for normal QoS 1 traffic.
    public var fireAndObserve = false

    /// Return the payload as a utf8 string if possible
    ///
    /// It will return nil if the payload is not a valid utf8 string
    public var string: String? {
        NSString(bytes: payload, length: payload.count, encoding: String.Encoding.utf8.rawValue) as String?
    }

    public init(topic: String, string: String, qos: CocoaMQTTQoS = .qos1, retained: Bool = false) {
        self.topic = topic
        self.payload = [UInt8](string.utf8)
        self.qos = qos
        self.retained = retained
    }

    public init(topic: String, payload: [UInt8], qos: CocoaMQTTQoS = .qos1, retained: Bool = false) {
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.retained = retained
    }
}

extension CocoaMQTTMessage {

    public override var description: String {
        return "CocoaMQTTMessage(topic: \(topic), qos: \(qos), payload: \(payload.summary))"
    }
}

// For test
extension CocoaMQTTMessage {

    var t_pub_frame: FramePublish {
        var frame = FramePublish(topic: topic, payload: payload, qos: qos, msgid: 0)
        frame.retained = retained
        frame.dup = duplicated
        frame.fireAndObserve = fireAndObserve
        return frame
    }

}
