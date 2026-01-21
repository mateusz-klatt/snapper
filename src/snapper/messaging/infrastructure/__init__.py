"""ZeroMQ messaging infrastructure components.

This package provides the core ZMQ infrastructure for the Snapper messaging system,
including:

- **Broker**: XPUB/XSUB message broker for centralizing pub/sub traffic
- **Validated Sockets**: Topic-validated wrappers around raw ZMQ sockets
- **Logger**: Message auditing service for debugging and compliance

Architecture
------------
The messaging infrastructure implements a central broker pattern:

    Publishers (XSUB) ──> [ZMQ Broker] ──> Subscribers (XPUB)

- Publishers connect to XSUB endpoint and send messages
- Subscribers connect to XPUB endpoint and receive filtered messages
- The broker forwards messages bidirectionally with zero-copy

Components
----------
ZmqBrokerProcess
    Async XPUB/XSUB broker using RegisterableProcess pattern.
ZmqBrokerThread
    Synchronous threaded broker alternative for simpler deployments.
ValidatedPublisher
    Topic-validated PUB socket wrapper preventing invalid topic strings.
ValidatedSubscriber
    Pattern-validated SUB socket wrapper preventing invalid subscriptions.
ZmqMessageLogger
    Audit trail service subscribing to all messages for logging.

Example:
-------
Start the broker and connect validated sockets::

    broker = ZmqBrokerProcess()
    await broker.start()

    # Publisher side
    ctx = zmq.asyncio.Context()
    raw_pub = ctx.socket(zmq.PUB)
    raw_pub.connect("tcp://localhost:5555")
    publisher = ValidatedPublisher(raw_pub)
    await publisher.send_multipart("market.kraken.BTC-USD.ticks", payload)

    # Subscriber side
    raw_sub = ctx.socket(zmq.SUB)
    raw_sub.connect("tcp://localhost:5556")
    subscriber = ValidatedSubscriber(raw_sub)
    subscriber.subscribe("market.kraken.")  # Prefix pattern
    topic, payload = await subscriber.recv_multipart()
"""
