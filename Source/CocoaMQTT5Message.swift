//
//  CocoaMQTT5Message.swift
//  CocoaMQTT
//
//  Created by Created by liwei wang on 2021/11/10.
//  Copyright (c) 2015 emqx.io. All rights reserved.
//

import Foundation

/// MQTT Message
public class CocoaMQTT5Message: NSObject {

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
    /// - Set `MqttPublishProperties.messageExpiryInterval` so the broker
    ///   bounds how long it will hold the message for offline subscribers
    ///   (the client side no longer keeps a copy to resend).
    /// - On connect, set `MqttConnectProperties.receiveMaximum` so the
    ///   broker enforces a server-side cap on concurrent unacknowledged
    ///   publishes - replacing the natural rate-limit that the inflight
    ///   window provided for normal QoS 1 traffic.
    public var fireAndObserve = false

    /// 3.1.3.2.3 Payload Format Indicator
    public var isUTF8EncodedData: Bool = true
    /// 3.1.3.2.2 Will Delay Interval
    public var willDelayInterval: UInt32? = 0
    /// 3.1.3.2.4 Message Expiry Interval
    public var willExpiryInterval: UInt32? = .max
    /// 3.1.3.2.5 Content Type
    public var contentType: String?
    /// 3.1.3.2.6 Response Topic
    public var willResponseTopic: String?
    /// 3.1.3.2.7 Correlation Data
    public var willCorrelationData: [UInt8]?
    /// 3.1.3.2.8 User Property
    public var willUserProperty: [String: String]?

    /// Return the payload as a utf8 string if possible
    ///
    /// It will return nil if the payload is not a valid utf8 string
    public var string: String? {
        NSString(bytes: payload, length: payload.count, encoding: String.Encoding.utf8.rawValue) as String?
    }

    public var properties: [UInt8] {

        var properties = [UInt8]()
        var retVal = [UInt8]()

        /// 3.1.3.2.2 Property Length
        if let willDelayInterval = self.willDelayInterval {
            properties += getMQTTPropertyData(type: CocoaMQTTPropertyName.willDelayInterval.rawValue, value: willDelayInterval.byteArrayLittleEndian)
        }

        /// 3.1.3.2.4 Message Expiry Interval
        if let willExpiryInterval = self.willExpiryInterval {
            properties += getMQTTPropertyData(type: CocoaMQTTPropertyName.willExpiryInterval.rawValue, value: willExpiryInterval.byteArrayLittleEndian)
        }

        /// 3.1.3.2.3 Payload Format Indicator
        if isUTF8EncodedData {
            properties += [1, 1]
        } else {
            properties += [1, 0]
        }

        /// 3.1.3.2.5 Content Type
        if var contentType = self.contentType {
            if isUTF8EncodedData {
                contentType = contentType.stringUTF8
            }
            properties += getMQTTPropertyData(type: CocoaMQTTPropertyName.contentType.rawValue, value: contentType.bytesWithLength)
        }

        /// 3.1.3.2.6 Response Topic
        if var willResponseTopic = self.willResponseTopic {
            if isUTF8EncodedData {
                willResponseTopic = willResponseTopic.stringUTF8
            }
            properties += getMQTTPropertyData(type: CocoaMQTTPropertyName.responseTopic.rawValue, value: willResponseTopic.bytesWithLength)
        }

        /// 3.1.3.2.7 Correlation Data
        if let willCorrelationData = self.willCorrelationData {
            let buff = UInt16(willCorrelationData.count).hlBytes + willCorrelationData
            properties += getMQTTPropertyData(type: CocoaMQTTPropertyName.correlationData.rawValue, value: buff)
        }

        /// 3.1.3.2.8 User Property
        if let willUserProperty = self.willUserProperty {
            willUserProperty.forEach { element in
                properties.append(UInt8(CocoaMQTTPropertyName.userProperty.rawValue))
                if isUTF8EncodedData {
                    let key = element.key.stringUTF8
                    properties += key .bytesWithLength
                    let value = element.value.stringUTF8
                    properties += value.bytesWithLength
                } else {
                    properties += element.key.bytesWithLength
                    properties += element.value.bytesWithLength
                }
            }
        }

        retVal += properties
        return retVal
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

    public init(topic: String, payload: [String: Any], qos: CocoaMQTTQoS = .qos1, retained: Bool = false) throws {
        let data = try JSONSerialization.data(withJSONObject: payload)
        self.topic = topic
        self.payload = [UInt8](data)
        self.qos = qos
        self.retained = retained
    }
}

extension CocoaMQTT5Message {

    public override var description: String {
        return "CocoaMQTT5Message(topic: \(topic), qos: \(qos), payload: \(payload.summary))"
    }
}

// For test
extension CocoaMQTT5Message {

    var t_pub_frame: FramePublish {
        var frame = FramePublish(topic: topic, payload: payload, qos: qos, msgid: 0)
        frame.retained = retained
        frame.dup = duplicated
        frame.fireAndObserve = fireAndObserve
        return frame
    }

}
