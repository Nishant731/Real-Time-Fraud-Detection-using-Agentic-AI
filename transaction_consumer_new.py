from kafka import KafkaConsumer
import json
from mgclient import connect
import time
from collections import deque

print("Creating consumer...")
consumer = KafkaConsumer(
    "fraud-transactions",
    bootstrap_servers="127.0.0.1:9093",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="txn-consumer-fresh-start",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    max_poll_records=100  # Fetch more records per poll
)

print("Connecting to Memgraph...")
db = connect(host="localhost", port=7687)
cursor = db.cursor()
print("Connected to Memgraph.")

print("Consumer started... waiting for messages.")

# Metrics tracking
message_count = 0
start_time = time.time()
last_report_time = start_time
last_report_count = 0
recent_times = deque(maxlen=100)

# BATCHING CONFIGURATION
BATCH_SIZE = 100
batch = []

for message in consumer:
    batch_start_time = time.time()
    
    data = message.value
    txns = data if isinstance(data, list) else [data]
    batch.extend(txns)
    
    # Process when batch is full
    if len(batch) >= BATCH_SIZE:
        # Prepare batch data
        transactions = []
        for txn in batch:
            loc = txn.get("location", {})
            city = loc.get("city")
            country = loc.get("country")
            location_key = f"{city}_{country}" if city and country else "unknown"
            
            transactions.append({
                "transaction_id": txn.get("transaction_id"),
                "from_account": txn.get("from_account"),
                "to_account": txn.get("to_account"),
                "amount": txn.get("amount"),
                "timestamp": txn.get("timestamp"),
                "ip_address": txn.get("ip_address"),
                "device_id": txn.get("device_id"),
                "latitude": loc.get("latitude"),
                "longitude": loc.get("longitude"),
                "city": city,
                "country": country,
                "location_key": location_key
            })
        
        # OPTIMIZED BATCH CYPHER QUERY
        cypher = """
        UNWIND $transactions AS txn
        
        // Accounts (indexed MERGE is fast)
        MERGE (from:Account {account_id: txn.from_account})
        MERGE (to:Account {account_id: txn.to_account})
        
        // Transaction (CREATE is faster than MERGE for unique IDs)
        CREATE (t:Transaction {
            transaction_id: txn.transaction_id,
            amount: txn.amount,
            timestamp: txn.timestamp,
            ip_address: txn.ip_address,
            device_id: txn.device_id,
            latitude: txn.latitude,
            longitude: txn.longitude,
            city: txn.city,
            country: txn.country
        })
        
        // Basic relationships
        CREATE (from)-[:SENT]->(t)
        CREATE (t)-[:RECEIVED_BY]->(to)
        
        // Shared resources (indexed MERGE)
        MERGE (ip:IPAddress {address: txn.ip_address})
        MERGE (device:Device {device_id: txn.device_id})
        MERGE (loc:Location {location_key: txn.location_key})
        ON CREATE SET loc.city = txn.city, loc.country = txn.country
        
        // Relationships to shared resources
        CREATE (t)-[:USED_IP]->(ip)
        CREATE (t)-[:USED_DEVICE]->(device)
        CREATE (t)-[:ORIGINATED_FROM]->(loc)
        
        // Historical tracking (only if needed for fraud detection)
        MERGE (from)-[:ACCESSED_FROM]->(ip)
        MERGE (from)-[:ACCESSED_VIA]->(device)
        MERGE (from)-[:LOCATED_IN]->(loc)
        """
        
        # Execute batch
        cursor.execute(cypher, {"transactions": transactions})
        db.commit()
        
        batch_end_time = time.time()
        batch_processing_time = batch_end_time - batch_start_time
        recent_times.append(batch_processing_time)
        
        message_count += len(batch)
        
        # Clear batch
        batch = []
        
        # Report every 10 seconds
        current_time = time.time()
        if current_time - last_report_time >= 10:
            elapsed = current_time - start_time
            messages_in_window = message_count - last_report_count
            window_duration = current_time - last_report_time
            
            overall_throughput = message_count / elapsed
            window_throughput = messages_in_window / window_duration
            avg_batch_time = sum(recent_times) / len(recent_times) if recent_times else 0
            
            print(f"\n{'='*60}")
            print(f"📊 THROUGHPUT METRICS")
            print(f"{'='*60}")
            print(f"Total messages processed: {message_count:,}")
            print(f"Overall throughput: {overall_throughput:.2f} msg/sec")
            print(f"Current throughput: {window_throughput:.2f} msg/sec")
            print(f"Avg batch time: {avg_batch_time:.2f} sec ({BATCH_SIZE} msgs)")
            print(f"Avg per-message time: {(avg_batch_time/BATCH_SIZE)*1000:.2f} ms")
            print(f"Total elapsed time: {elapsed:.2f} seconds")
            print(f"{'='*60}\n")
            
            last_report_time = current_time
            last_report_count = message_count
