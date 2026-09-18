# Milestone 1: Data Ingestion System (Cloud Pub/Sub)

## GitHub Link
Scripts used in the SmartMeter part: in Root


Scripts used in the Design part: in `Design/`

---

## Discussion

### What is EDA? What are its advantages and disadvantages?
EDA (Event-Driven Architecture) is a design where components talk to each other by producing and consuming **events** through a broker, instead of calling each other directly. In my project, `producer.py` doesn't know who's listening — it just publishes to the `labels` topic, and `consumer.py` reacts whenever a message shows up.

**Advantages:**
- Producer and consumer are decoupled: I can run, stop, or scale either one without touching the other.
- Scales well: I could add more consumers to the same subscription to process messages faster.
- Resilient: if my consumer is down, Pub/Sub holds the messages until it comes back.

**Disadvantages:**
- Harder to debug: there's no direct call stack, so tracing a message from producer to consumer takes more effort.
- Eventual consistency: the consumer processes messages a moment after they're published, not instantly.
- More moving parts to manage: I need a topic, a subscription, and credentials configured correctly on both ends.

### Push vs Pull subscriptions
Cloud Pub/Sub supports two delivery models:

- **Pull**: the subscriber actively asks Pub/Sub for messages. This is what I used in `consumer.py` with `subscriber.subscribe(...)`. 
  - Strength: I control the pace, it is good for batch jobs or when my consumer needs to throttle load.
  - Weakness: needs a client running and polling; not truly instant.

- **Push**: Pub/Sub sends the message directly to an HTTPS endpoint I configure (like a Cloud Function) as soon as it arrives.
  - Strength: near real-time, no need to keep a client process running.
  - Weakness: my endpoint has to be publicly reachable and secured, and a burst of messages can overwhelm it if it's not built to scale.

Pull fits my Design part well since `consumer.py` is a long-running script I control. Push would fit better if I wanted a serverless function to react to every new record automatically.

### Ordering keys
An ordering key guarantees that all messages published with the **same key** arrive at the subscriber in the exact order they were published, while messages with different keys can still be delivered in parallel.

Example from my data: in `Labels.csv`, each record has a `profileName` (boston, denver, losang). If I published with `profileName` as the ordering key, all "boston" readings would always arrive in the order they were recorded, even if "denver" and "losang" messages are interleaved with them. The benefit is I get correctness per device without losing the throughput of publishing everything in parallel.

---

## Design

**Goal:** read `Labels.csv`, publish every row as a message to a `labels` topic, and have a consumer print each message back out.

### Producer (`Design/producer.py`)
I open the CSV with `csv.DictReader`, so each row comes back to me already as a dictionary, which is no manual parsing needed. Then I serialize it to JSON bytes and publish it, same pattern as `smartMeter.py`:

```python
with open("Labels.csv", newline='') as csv_file:
    reader = csv.DictReader(csv_file)   # each row is read directly as a dictionary
    for row in reader:
        record_value = json.dumps(row).encode('utf-8')   # serialize the dictionary
        future = publisher.publish(topic_path, record_value)
        future.result()
        print("The record {} has been published successfully".format(row))
```

### Consumer (`Design/consumer.py`)
The consumer subscribes to `labels-sub`, and for every message received, deserializes the bytes back into a dictionary and prints its values:

```python
def callback(message):
    record = json.loads(message.data.decode('utf-8'))   # deserialize back into a dictionary
    for value in record.values():
        print(value)
    message.ack()
```

### Running it
1. Create topic `labels` and subscription `labels-sub` in GCP.
2. Copy the service account JSON key into `Design/`.
3. Run `consumer.py` first (it listens).
4. Run `producer.py` from inside `Design/` so it finds `Labels.csv`.
5. Watch the consumer print every row of the CSV as it comes in.

---

## Videos
- Smart meter application demo (3 min): https://drive.google.com/file/d/1FtZnEVxrZbP_lcbDxKe2GuFwGxt5vJKG/view?usp=drive_link
- Design part demo (5 min): https://drive.google.com/file/d/1IDBR-_2ZgEcAf0nvSeSph8OJWlbJ3gfg/view?usp=drive_link
