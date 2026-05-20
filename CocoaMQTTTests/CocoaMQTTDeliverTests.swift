//
//  CocoaMQTTDeliverTests.swift
//  CocoaMQTT-Tests
//
//  Created by JianBo on 2019/10/3.
//  Copyright © 2019 emqtt.io. All rights reserved.
//

import XCTest
@testable import CocoaMQTT

class CocoaMQTTDeliverTests: XCTestCase {

    override func setUp() {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testSerialDeliver() {
        let caller = Caller()
        let deliver = CocoaMQTTDeliver()

        let frames = [FramePublish(topic: "t/0", payload: [0x00], qos: .qos0),
                      FramePublish(topic: "t/1", payload: [0x01], qos: .qos1, msgid: 1),
                      FramePublish(topic: "t/2", payload: [0x02], qos: .qos2, msgid: 2)]

        deliver.delegate = caller
        for f in frames {
            _ = deliver.add(f)
        }
        ms_sleep(100)

        XCTAssertEqual(frames.count, caller.frames.count)
        for i in 0 ..< frames.count {
            assertEqual(frames[i], caller.frames[i])
        }

    }

    func testAckMessage() {
        let caller = Caller()
        let deliver = CocoaMQTTDeliver()

        let frames = [FramePublish(topic: "t/0", payload: [0x00], qos: .qos0),
                      FramePublish(topic: "t/1", payload: [0x01], qos: .qos1, msgid: 1),
                      FramePublish(topic: "t/2", payload: [0x02], qos: .qos2, msgid: 2)]

        deliver.delegate = caller
        for f in frames {
            _ = deliver.add(f)
        }

        ms_sleep(100)

        XCTAssertEqual(frames.count, caller.frames.count)
        for i in 0 ..< frames.count {
            assertEqual(frames[i], caller.frames[i])
        }

        var inflights = deliver.t_inflightFrames()
        XCTAssertEqual(inflights.count, 2)
        XCTAssertEqual(deliver.t_queuedFrames().count, 0)
        for i in 0 ..< inflights.count {
            assertEqual(inflights[i], frames[i+1])
        }

        deliver.ack(by: FramePubAck(msgid: 1))
        deliver.ack(by: FramePubRec(msgid: 2))
        ms_sleep(100)

        inflights = deliver.t_inflightFrames()
        XCTAssertEqual(inflights.count, 1)
        XCTAssertEqual(deliver.t_queuedFrames().count, 0)
        assertEqual(inflights[0], FramePubRel(msgid: 2))

        deliver.ack(by: FramePubComp(msgid: 2))
        ms_sleep(100)

        inflights = deliver.t_inflightFrames()
        XCTAssertEqual(inflights.count, 0)

        // Assert sent
        assertEqual(caller.frames[3], FramePubRel(msgid: 2))
    }

    func testQueueAndInflightReDeliver() {
        let caller = Caller()
        let deliver = CocoaMQTTDeliver()

        let frames = [FramePublish(topic: "t/0", payload: [0x00], qos: .qos0),
                      FramePublish(topic: "t/1", payload: [0x01], qos: .qos1, msgid: 1),
                      FramePublish(topic: "t/2", payload: [0x02], qos: .qos2, msgid: 2)]

        deliver.retryTimeInterval = 1000
        deliver.inflightWindowSize = 1
        deliver.mqueueSize = 1
        deliver.delegate = caller

        XCTAssertEqual(true, deliver.add(frames[1]))
        ms_sleep(100) // Wait the message transfer to inflight-window
        XCTAssertEqual(true, deliver.add(frames[2]))
        XCTAssertEqual(false, deliver.add(frames[0]))

        ms_sleep(1100) // Wait for re-delivering timeout
        XCTAssertEqual(caller.frames.count, 2)
        assertEqual(caller.frames[0], frames[1])
        assertEqual(caller.frames[1], frames[1])

        deliver.ack(by: FramePubAck(msgid: 1))
        ms_sleep(100)   // Waiting for the frame in the mqueue transfer to inflight window

        var inflights = deliver.t_inflightFrames()
        XCTAssertEqual(inflights.count, 1)
        assertEqual(inflights[0], frames[2])

        deliver.ack(by: FramePubRec(msgid: 2))
        ms_sleep(2000)  // Waiting for re-delivering timeout
        deliver.ack(by: FramePubComp(msgid: 2))
        ms_sleep(100)

        inflights = deliver.t_inflightFrames()
        XCTAssertEqual(inflights.count, 0)

        let sents: [Frame] = [frames[1], frames[1], frames[2], FramePubRel(msgid: 2), FramePubRel(msgid: 2)]
        XCTAssertEqual(caller.frames.count, sents.count)
        for i in 0 ..< sents.count {
            assertEqual(caller.frames[i], sents[i])
        }
    }

    func testRedeliverTimerDriftDoesNotSkipRetry() {
        let caller = Caller()
        let deliver = CocoaMQTTDeliver()
        let frame = FramePublish(topic: "t/drift", payload: [0x01], qos: .qos1, msgid: 42)

        deliver.retryTimeInterval = 1000
        deliver.delegate = caller
        XCTAssertTrue(deliver.add(frame))
        ms_sleep(100)

        caller.reset()
        let intervalNs = deliver.t_retryIntervalNanoseconds()
        XCTAssertTrue(deliver.t_setInflightNextRetryTime(intervalNs, forMsgid: frame.msgid))

        deliver.t_redeliver(atUptimeNanoseconds: intervalNs / 2)
        ms_sleep(50)
        XCTAssertEqual(caller.frames.count, 0)

        // Simulate strict timer ticks: first callback runs slightly late, second runs on the next deadline.
        deliver.t_redeliver(atUptimeNanoseconds: intervalNs + 1_000_000)
        deliver.t_redeliver(atUptimeNanoseconds: intervalNs * 2)
        ms_sleep(100)

        XCTAssertEqual(caller.frames.count, 2)
        for sent in caller.frames {
            guard let publish = sent as? FramePublish else {
                XCTFail("Expected FramePublish")
                continue
            }
            assertEqual(publish, frame)
            XCTAssertTrue(publish.dup)
        }
    }

    func testRetryIntervalNanosecondsClampsNonPositiveValues() {
        let deliver = CocoaMQTTDeliver()

        deliver.retryTimeInterval = 0
        XCTAssertEqual(deliver.t_retryIntervalNanoseconds(), 1)

        deliver.retryTimeInterval = -5
        XCTAssertEqual(deliver.t_retryIntervalNanoseconds(), 1)
    }

    func testRetryIntervalNanosecondsHandlesInvalidAndHugeValues() {
        let deliver = CocoaMQTTDeliver()

        deliver.retryTimeInterval = .infinity
        XCTAssertEqual(deliver.t_retryIntervalNanoseconds(), 1)

        deliver.retryTimeInterval = .nan
        XCTAssertEqual(deliver.t_retryIntervalNanoseconds(), 1)

        deliver.retryTimeInterval = (Double(UInt64.max) / 1_000_000.0) + 1
        XCTAssertEqual(deliver.t_retryIntervalNanoseconds(), UInt64.max)
    }

    func testRedeliverWithZeroRetryIntervalDoesNotCrash() {
        let caller = Caller()
        let deliver = CocoaMQTTDeliver()
        let frame = FramePublish(topic: "t/zero", payload: [0x01], qos: .qos1, msgid: 100)

        deliver.retryTimeInterval = 0
        deliver.delegate = caller
        XCTAssertTrue(deliver.add(frame))
        ms_sleep(100)

        caller.reset()
        XCTAssertTrue(deliver.t_setInflightNextRetryTime(0, forMsgid: frame.msgid))

        deliver.t_redeliver(atUptimeNanoseconds: 0)
        ms_sleep(100)

        XCTAssertEqual(caller.frames.count, 1)
        guard let publish = caller.frames.first as? FramePublish else {
            XCTFail("Expected FramePublish")
            return
        }
        assertEqual(publish, frame)
        XCTAssertTrue(publish.dup)
    }

    // MARK: - fireAndObserve

    /// Backward-compat: with fireAndObserve == false (the default), QoS 1
    /// behaviour is unchanged - the frame enters the inflight window and
    /// is retransmitted when the awaiting timer trips.
    func testQos1DefaultStillRetransmits() {
        let caller = Caller()
        let deliver = CocoaMQTTDeliver()
        let frame = FramePublish(topic: "t/keep", payload: [0x01], qos: .qos1, msgid: 10)
        XCTAssertFalse(frame.fireAndObserve, "default must be false for backward compatibility")

        deliver.retryTimeInterval = 1000
        deliver.delegate = caller
        XCTAssertTrue(deliver.add(frame))
        ms_sleep(100)

        XCTAssertEqual(caller.frames.count, 1)
        XCTAssertEqual(deliver.t_inflightFrames().count, 1)
        XCTAssertEqual(deliver.t_fireAndObserveMsgids().count, 0)

        // Pin the inflight deadline to a known value, then drive past it.
        let intervalNs = deliver.t_retryIntervalNanoseconds()
        XCTAssertTrue(deliver.t_setInflightNextRetryTime(intervalNs, forMsgid: frame.msgid))
        deliver.t_redeliver(atUptimeNanoseconds: intervalNs * 4)
        ms_sleep(50)

        XCTAssertEqual(caller.frames.count, 2, "default QoS 1 must retransmit when the deadline elapses")
        if let publish = caller.frames.last as? FramePublish {
            assertEqual(publish, frame)
            XCTAssertTrue(publish.dup, "retransmit should have DUP set")
        } else {
            XCTFail("Expected FramePublish on retransmit")
        }
    }

    /// With fireAndObserve == true, a QoS 1 PUBLISH is sent exactly once
    /// and never enters the inflight window or persistent storage, so no
    /// retransmit fires when the redeliver deadline is crossed.
    func testFireAndObserveQos1NoRetransmit() {
        let caller = Caller()
        let deliver = CocoaMQTTDeliver()
        var frame = FramePublish(topic: "t/fao", payload: [0x01], qos: .qos1, msgid: 21)
        frame.fireAndObserve = true

        deliver.retryTimeInterval = 1000
        deliver.delegate = caller
        XCTAssertTrue(deliver.add(frame))
        ms_sleep(100)

        // Sent exactly once on the wire.
        XCTAssertEqual(caller.frames.count, 1)
        if let publish = caller.frames.first as? FramePublish {
            assertEqual(publish, frame)
            XCTAssertTrue(publish.fireAndObserve, "flag should plumb through to the dispatched frame")
        } else {
            XCTFail("Expected FramePublish")
        }

        // No inflight bookkeeping; tracking set holds the msgid for the
        // PUBACK-suppression branch.
        XCTAssertEqual(deliver.t_inflightFrames().count, 0)
        XCTAssertTrue(deliver.t_fireAndObserveMsgids().contains(frame.msgid))

        // Walk the clock well past the retry deadline (2 * retry interval).
        let intervalNs = deliver.t_retryIntervalNanoseconds()
        deliver.t_redeliver(atUptimeNanoseconds: intervalNs * 2)
        deliver.t_redeliver(atUptimeNanoseconds: intervalNs * 4)
        ms_sleep(100)

        XCTAssertEqual(caller.frames.count, 1, "fire-and-observe QoS 1 must NOT retransmit")
    }

    /// PUBACK for a fire-and-observe publish still passes through deliver.ack()
    /// without warning, and the tracking entry is consumed.
    func testFireAndObservePubAckConsumesTrackingEntry() {
        let caller = Caller()
        let deliver = CocoaMQTTDeliver()
        var frame = FramePublish(topic: "t/fao-ack", payload: [0x02], qos: .qos1, msgid: 22)
        frame.fireAndObserve = true

        deliver.retryTimeInterval = 1000
        deliver.delegate = caller
        XCTAssertTrue(deliver.add(frame))
        ms_sleep(100)

        XCTAssertTrue(deliver.t_fireAndObserveMsgids().contains(frame.msgid))

        // Simulate broker PUBACK arrival via the same code path the reader
        // would use. The deliver layer must not throw, must not retransmit,
        // and must remove the tracking entry.
        deliver.ack(by: FramePubAck(msgid: frame.msgid))
        ms_sleep(100)

        XCTAssertEqual(caller.frames.count, 1)
        XCTAssertEqual(deliver.t_inflightFrames().count, 0)
        XCTAssertFalse(deliver.t_fireAndObserveMsgids().contains(frame.msgid))
    }

    /// fireAndObserve is intended for QoS 1. When the flag is set on a QoS 0
    /// or QoS 2 publish, the existing transport path is taken (no special
    /// behaviour change). This guards against accidental wiring regressions.
    func testFireAndObserveOnlyAffectsQos1() {
        let caller = Caller()
        let deliver = CocoaMQTTDeliver()

        var qos0 = FramePublish(topic: "t/fao-0", payload: [0x00], qos: .qos0, msgid: 0)
        qos0.fireAndObserve = true
        var qos2 = FramePublish(topic: "t/fao-2", payload: [0x02], qos: .qos2, msgid: 30)
        qos2.fireAndObserve = true

        deliver.retryTimeInterval = 1000
        deliver.delegate = caller
        XCTAssertTrue(deliver.add(qos0))
        XCTAssertTrue(deliver.add(qos2))
        ms_sleep(100)

        // QoS 2 still uses inflight + retransmit semantics.
        XCTAssertEqual(deliver.t_inflightFrames().count, 1)
        XCTAssertFalse(deliver.t_fireAndObserveMsgids().contains(qos2.msgid))
    }

    /// Plumbing check: setting fireAndObserve on CocoaMQTTMessage propagates
    /// to the FramePublish produced by the t_pub_frame helper that mirrors
    /// the publish() construction path.
    func testFireAndObservePlumbsThroughCocoaMQTTMessage() {
        let m = CocoaMQTTMessage(topic: "t/plumb-3", payload: [0x01], qos: .qos1)
        m.fireAndObserve = true
        let frame = m.t_pub_frame
        XCTAssertTrue(frame.fireAndObserve)
        XCTAssertEqual(frame.qos, .qos1)

        let m2 = CocoaMQTTMessage(topic: "t/plumb-3-default", payload: [0x01], qos: .qos1)
        XCTAssertFalse(m2.t_pub_frame.fireAndObserve)
    }

    /// Fire-and-observe publishes must not hit persistent storage (no point
    /// persisting something we will never retransmit / replay).
    func testFireAndObserveSkipsStorageWrite() {
        let clientID = "deliver-fao-\(UUID().uuidString)"
        defer { clearStorage(clientID) }

        guard let storage = CocoaMQTTStorage(by: clientID) else {
            XCTFail("Initial storage failed")
            return
        }

        let caller = Caller()
        let deliver = CocoaMQTTDeliver()
        deliver.delegate = caller
        deliver.recoverSessionBy(storage)

        var fao = FramePublish(topic: "t/fao-storage", payload: [0x01], qos: .qos1, msgid: 50)
        fao.fireAndObserve = true
        let normal = FramePublish(topic: "t/normal-storage", payload: [0x02], qos: .qos1, msgid: 51)

        XCTAssertTrue(deliver.add(fao))
        XCTAssertTrue(deliver.add(normal))
        ms_sleep(100)

        let saved = storage.readAll()
        XCTAssertEqual(saved.count, 1, "only the non-fire-and-observe publish should be persisted")
        if let only = saved.first as? FramePublish {
            XCTAssertEqual(only.msgid, normal.msgid)
        } else {
            XCTFail("Expected the normal QoS 1 frame to be in storage")
        }
    }

    /// Same plumbing check for the MQTT 5 message type.
    func testFireAndObservePlumbsThroughCocoaMQTT5Message() {
        let m = CocoaMQTT5Message(topic: "t/plumb-5", payload: [0x01], qos: .qos1)
        m.fireAndObserve = true
        let frame = m.t_pub_frame
        XCTAssertTrue(frame.fireAndObserve)
        XCTAssertEqual(frame.qos, .qos1)

        let m2 = CocoaMQTT5Message(topic: "t/plumb-5-default", payload: [0x01], qos: .qos1)
        XCTAssertFalse(m2.t_pub_frame.fireAndObserve)
    }

    func testStorage() {

        let clientID = "deliver-unit-testing"
        let caller = Caller()
        let deliver = CocoaMQTTDeliver()

        let frames = [FramePublish(topic: "t/0", payload: [0x00], qos: .qos0),
                      FramePublish(topic: "t/1", payload: [0x01], qos: .qos1, msgid: 1),
                      FramePublish(topic: "t/2", payload: [0x02], qos: .qos2, msgid: 2)]

        guard let storage = CocoaMQTTStorage(by: clientID) else {
            XCTAssert(false, "Initial storage failed")
            return
        }

        deliver.delegate = caller
        deliver.recoverSessionBy(storage)

        for f in frames {
            _ = deliver.add(f)
        }

        var saved = storage.readAll()
        XCTAssertEqual(saved.count, 2)

        deliver.ack(by: FramePubAck(msgid: 1))
        ms_sleep(100)
        saved = storage.readAll()
        XCTAssertEqual(saved.count, 1)

        deliver.ack(by: FramePubRec(msgid: 2))
        ms_sleep(100)
        saved = storage.readAll()
        XCTAssertEqual(saved.count, 1)
        assertEqual(saved[0], FramePubRel(msgid: 2))

        deliver.ack(by: FramePubComp(msgid: 2))
        ms_sleep(100)
        saved = storage.readAll()
        XCTAssertEqual(saved.count, 0)

        caller.reset()
        _ = storage.write(frames[1])
        deliver.recoverSessionBy(storage)
        ms_sleep(100)
        XCTAssertEqual(caller.frames.count, 1)
        assertEqual(caller.frames[0], frames[1])

        deliver.ack(by: FramePubAck(msgid: 1))
        ms_sleep(100)
        XCTAssertEqual(storage.readAll().count, 0)
    }

    func testRecoverSessionKeepStoredFramesUntilAck() {
        let clientID = "deliver-recover-\(UUID().uuidString)"
        defer {
            clearStorage(clientID)
        }

        let frame = FramePublish(topic: "t/recover", payload: [0x01], qos: .qos1, msgid: 42)
        guard let storage = CocoaMQTTStorage(by: clientID) else {
            XCTFail("Initial storage failed")
            return
        }
        XCTAssertTrue(storage.write(frame))

        let caller1 = Caller()
        var deliver1: CocoaMQTTDeliver? = CocoaMQTTDeliver()
        deliver1?.delegate = caller1
        deliver1?.recoverSessionBy(storage)
        ms_sleep(100)
        XCTAssertEqual(caller1.frames.count, 1)
        if let firstRecovered = caller1.frames.first {
            assertEqual(firstRecovered, frame)
        }
        XCTAssertEqual(storage.readAll().count, 1)

        // Simulate an app crash/restart before receiving PUBACK.
        deliver1 = nil

        guard let storageAfterRestart = CocoaMQTTStorage(by: clientID) else {
            XCTFail("Reload storage failed")
            return
        }
        let caller2 = Caller()
        let deliver2 = CocoaMQTTDeliver()
        deliver2.delegate = caller2
        deliver2.recoverSessionBy(storageAfterRestart)
        ms_sleep(100)

        XCTAssertEqual(caller2.frames.count, 1)
        if let secondRecovered = caller2.frames.first {
            assertEqual(secondRecovered, frame)
        }
        XCTAssertEqual(storageAfterRestart.readAll().count, 1)

        deliver2.ack(by: FramePubAck(msgid: frame.msgid))
        ms_sleep(100)
        XCTAssertEqual(storageAfterRestart.readAll().count, 0)
    }

    func testTODO() {
        // TODO: How to test large of messages combined qos0/qos1/qos2
    }

    private func clearStorage(_ clientId: String) {
        let suiteName = "cocomqtt-\(clientId)"
        guard let defaults = UserDefaults(suiteName: suiteName) else {
            return
        }
        for key in defaults.dictionaryRepresentation().keys {
            defaults.removeObject(forKey: key)
        }
        defaults.synchronize()
    }

    // Helper for assert equality for Frame
    private func assertEqual(_ f1: Frame, _ f2: Frame, _ lines: Int = #line) {
        if let pub1 = f1 as? FramePublish,
           let pub2 = f2 as? FramePublish {
            XCTAssertEqual(pub1.topic, pub2.topic)
            XCTAssertEqual(pub1.payload(), pub2.payload())
            XCTAssertEqual(pub1.msgid, pub2.msgid)
            XCTAssertEqual(pub1.qos, pub2.qos)
        } else if let rel1 = f1 as? FramePubRel,
                let rel2 = f2 as? FramePubRel {
            XCTAssertEqual(rel1.msgid, rel2.msgid)
        } else {
            XCTAssert(false, "Assert equal failed line: \(lines)")
        }
    }

    private func ms_sleep(_ ms: Int) {
        usleep(useconds_t(ms * 1000))
    }
}

private class Caller: CocoaMQTTDeliverProtocol {

    private let delegate_queue_key = DispatchSpecificKey<String>()
    private let delegate_queue_val = "_custom_delegate_queue_"

    var delegateQueue: DispatchQueue

    var frames = [Frame]()

    init() {
        delegateQueue = DispatchQueue(label: "caller.deliver.test")
        delegateQueue.setSpecific(key: delegate_queue_key, value: delegate_queue_val)
    }

    func reset() {
        frames = []
    }

    func deliver(_ deliver: CocoaMQTTDeliver, wantToSend frame: Frame) {
        assert_in_del_queue()

        frames.append(frame)
    }

    private func assert_in_del_queue() {
        XCTAssertEqual(delegate_queue_val, DispatchQueue.getSpecific(key: delegate_queue_key))
    }
}
