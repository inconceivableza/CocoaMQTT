# Test broker

The `CocoaMQTTTests` target's broker-dependent test classes (`CocoaMQTTTests`, the auto-reconnect / publish / SSL / WebSocket cases) connect to `localhost` and assume:

| Port | Transport | Auth |
|---|---|---|
| `1883` | plain MQTT | anonymous |
| `8083` | MQTT over WebSocket | anonymous |
| `8883` | MQTT over TLS, any cert | anonymous; broker does not validate client certs |

`test-broker.conf` is a ready-to-run mosquitto config that listens on all three. To use it locally:

```sh
# 1. install mosquitto on the host (one-off):
brew install mosquitto              # macOS
sudo apt-get install -y mosquitto   # Debian/Ubuntu

# 2. generate a self-signed server cert (one-off):
cd CocoaMQTTTests/Fixtures
openssl req -x509 -newkey rsa:2048 -days 3650 -nodes \
    -keyout test-broker.key -out test-broker.crt \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

# 3. start the broker (each time, in a separate terminal):
mosquitto -c CocoaMQTTTests/Fixtures/test-broker.conf -v
```

Then from the repo root:

```sh
swift test
```

The two SSL test cases (`testOnyWaySSL`, `testTwoWaySLL`) both set `mqtt.allowUntrustCACertificate = true`, so the self-signed cert is accepted client-side. `testTwoWaySLL` sends a client cert from the bundled `client-keycert.p12` but the broker does not validate it; the test only asserts that the connection succeeded.

The pure-unit test classes (`CocoaMQTTServerNameTests`, `FrameTests`, `CocoaMQTTDeliverTests`, etc.) do not need the broker and will pass either way.
