# real_databases1.py
"""
Integrated Database Connections for Distributed Fraud Detection

PC1 (Remote):
- MariaDB (Behavioral Data)
- Qdrant (Vector Similarity)

PC2 (Local - Windows + WSL + Docker):
- Memgraph (Graph + MAGE Algorithms)
"""

import mysql.connector
from neo4j import GraphDatabase
from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels
from typing import Dict, List, Optional
from datetime import datetime
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== MARIADB (PC1) ====================

class RealMariaDB:
    """MariaDB connection for user behavior and transaction data"""

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.connection_params = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'connect_timeout': 10
        }
        self.conn = None
        self.connect()

    def connect(self):
        try:
            self.conn = mysql.connector.connect(**self.connection_params)
            logger.info(f"✓ MariaDB connected: {self.connection_params['host']}")
        except Exception as e:
            logger.error(f"✗ MariaDB connection failed: {e}")
            raise

    def ensure_connection(self):
        try:
            if not self.conn or not self.conn.is_connected():
                self.connect()
        except:
            self.connect()

    def get_user_stats(self, user_id: str) -> Optional[Dict]:
        self.ensure_connection()
        cursor = self.conn.cursor(dictionary=True)
        try:
            cursor.execute(
                "SELECT user_id, created_at, status, risk_level FROM users WHERE user_id = %s",
                (user_id,)
            )
            user_info = cursor.fetchone()
            if not user_info:
                return None

            cursor.execute(
                "SELECT avg_daily_tx_count, avg_daily_amount, stddev_amount "
                "FROM user_transaction_profile WHERE user_id = %s",
                (user_id,)
            )
            tx_profile = cursor.fetchone()

            cursor.execute(
                "SELECT total_tx_count, total_tx_amount "
                "FROM daily_user_stats WHERE user_id = %s AND stat_date = CURDATE()",
                (user_id,)
            )
            daily_stats = cursor.fetchone()

            return {
                'user_id': user_info['user_id'],
                'risk_level': user_info['risk_level'],
                'avg_daily_amount': float(tx_profile['avg_daily_amount']) if tx_profile else 0.0,
                'today_tx_count': daily_stats['total_tx_count'] if daily_stats else 0,
                'today_tx_amount': float(daily_stats['total_tx_amount']) if daily_stats else 0.0
            }
        finally:
            cursor.close()

    def get_device_history(self, user_id: str, device_id: str) -> Dict:
        self.ensure_connection()
        cursor = self.conn.cursor(dictionary=True)
        try:
            cursor.execute(
                "SELECT usage_count FROM user_network_profile "
                "WHERE user_id = %s AND device_id = %s",
                (user_id, device_id)
            )
            res = cursor.fetchone()
            return {
                'is_known_device': res is not None,
                'usage_count': res['usage_count'] if res else 0
            }
        finally:
            cursor.close()

    def get_recent_anomalies(self, user_id: str, hours: int = 24) -> List[Dict]:
        self.ensure_connection()
        cursor = self.conn.cursor(dictionary=True)
        try:
            cursor.execute(
                "SELECT anomaly_type, severity FROM user_anomaly_flags "
                "WHERE user_id = %s AND detected_at > NOW() - INTERVAL %s HOUR",
                (user_id, hours)
            )
            return cursor.fetchall()
        finally:
            cursor.close()

    def close(self):
        if self.conn and self.conn.is_connected():
            self.conn.close()

# ==================== QDRANT (PC1) ====================

class RealQdrant:
    """Qdrant connection for 16-dimensional vector similarity search"""

    def __init__(self, host: str, port: int = 6333):
        self.client = QdrantClient(host=host, port=port)
        self.collection_name = "transaction_embeddings"
        self._ensure_collection()

    def _ensure_collection(self):
        if not self.client.collection_exists(self.collection_name):
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=qmodels.VectorParams(
                    size=16,
                    distance=qmodels.Distance.COSINE
                )
            )
            print(f"✓ Created collection: {self.collection_name}")
        else:
            print(f"✓ Collection {self.collection_name} already exists.")

    def search_similar(self, features: List[float], limit: int = 5) -> List[Dict]:
        results = self.client.search(
            collection_name=self.collection_name,
            query_vector=[float(f) for f in features],
            limit=limit
        )
        return [
            {"id": r.id, "score": r.score, "payload": r.payload}
            for r in results
        ]

# ==================== MEMGRAPH (PC2 - LOCAL) ====================

class RealMemgraph:
    """Local Memgraph connection (Docker on PC2 via WSL)"""

    def __init__(self, host: str = "127.0.0.1", port: int = 7687):
        self.uri = f"bolt://{host}:{port}"
        self.driver = GraphDatabase.driver(
            self.uri,
            auth=None,
            encrypted=False
        )
        try:
            self.driver.verify_connectivity()
            logger.info(f"✓ Memgraph connected locally on PC2 ({host})")
        except Exception as e:
            logger.error(f"✗ Memgraph connection failed: {e}")
            raise

    def close(self):
        self.driver.close()

    def _execute(self, query: str, params: Dict):
        with self.driver.session() as session:
            result = session.run(query, params)
            return result.single()

    def detect_mule_pattern_mage(self, user_id: str) -> Dict:
        query = (
            "CALL betweenness_centrality.get() "
            "YIELD node, score "
            "WHERE node.id = $user_id "
            "RETURN score"
        )
        record = self._execute(query, {"user_id": user_id})
        score = record["score"] if record else 0.0
        return {
            'is_mule': score > 0.8,
            'betweenness_centrality': float(score)
        }

    def detect_fraud_ring_mage(self, user_id: str) -> Dict:
        query = (
            "MATCH (u:User {id: $user_id})-[:TRANSFER*2..]->(u) "
            "RETURN count(*) > 0 AS detected"
        )
        record = self._execute(query, {"user_id": user_id})
        detected = record["detected"] if record else False
        return {
            'is_fraud_ring': detected,
            'cycle_found': detected
        }

    def detect_layering_mage(self, user_id: str) -> Dict:
        query = (
            "MATCH (u:User {id: $user_id})-[:TRANSFER*2..]->(u) "
            "RETURN count(*) > 0 AS detected"
        )
        record = self._execute(query, {"user_id": user_id})
        detected = record["detected"] if record else False
        return {
            'is_layering': detected,
            'cycle_detected': detected
        }

    def detect_structuring_mage(self, user_id: str, threshold: float = 10000.0) -> Dict:
        query = (
            "MATCH (u:User {id: $user_id})-[t:TRANSFER]->() "
            "WHERE t.amount > ($limit * 0.9) AND t.amount < $limit "
            "RETURN count(t) AS count"
        )
        record = self._execute(query, {"user_id": user_id, "limit": threshold})
        count = record["count"] if record else 0
        return {
            'is_structuring': count >= 3,
            'near_threshold_count': int(count)
        }

    def detect_scatter_gather_mage(self, user_id: str) -> Dict:
        query = (
            "CALL pagerank.get() "
            "YIELD node, rank "
            "WHERE node.id = $user_id "
            "RETURN rank"
        )
        record = self._execute(query, {"user_id": user_id})
        rank = record["rank"] if record else 0.0
        return {
            'is_scatter_gather': rank > 0.5,
            'pagerank_score': float(rank)
        }

    def detect_account_takeover_mage(self, user_id: str) -> Dict:
        query = (
            "MATCH (u:User {id: $user_id})-[r:USED_DEVICE]->(d:Device) "
            "WITH d, count(DISTINCT u) AS users_on_device "
            "RETURN max(users_on_device) AS max_sharing"
        )
        record = self._execute(query, {"user_id": user_id})
        sharing = record["max_sharing"] if record else 0
        return {
            'is_account_takeover': sharing > 5,
            'anomaly_score': float(sharing / 10)
        }

# ==================== MAIN TEST BLOCK ====================

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    print("\n" + "=" * 80)
    print("DISTRIBUTED DATABASE TEST SUITE")
    print("=" * 80)

    # 1. Test MariaDB (PC1)
    try:
        db = RealMariaDB(
            host=os.getenv("MARIADB_HOST", "192.168.8.180"),
            port=int(os.getenv("MARIADB_PORT", 3306)),
            user=os.getenv("MARIADB_USER", "fraud_user"),
            password=os.getenv("MARIADB_PASSWORD", "fraud_password"),
            database=os.getenv("MARIADB_DATABASE", "fraud_detection")
        )
        stats = db.get_user_stats("USER_001")
        print(
            f"✓ MariaDB (PC1): User Stats retrieved "
            f"(Risk: {stats['risk_level'] if stats else 'N/A'})"
        )
        db.close()
    except Exception as e:
        print(f"✗ MariaDB Error: {e}")

    # 2. Test Qdrant (PC1)
    try:
        qd = RealQdrant(
            host=os.getenv("QDRANT_HOST", "192.168.8.180"),
            port=int(os.getenv("QDRANT_PORT", 6333))
        )
        print("✓ Qdrant (PC1): Connection and collection verified")
    except Exception as e:
        print(f"✗ Qdrant Error: {e}")

    # 3. Test Memgraph (PC2 - Local)
    try:
        mg = RealMemgraph(
            host=os.getenv("MEMGRAPH_HOST", "127.0.0.1"),
            port=int(os.getenv("MEMGRAPH_PORT", 7687))
        )
        res = mg.detect_layering_mage("USER_001")
        print(
            f"✓ Memgraph (PC2): Algorithm test "
            f"(Layering Detected: {res['is_layering']})"
        )
        mg.close()
    except Exception as e:
        print(f"✗ Memgraph Error: {e}")

    print("=" * 80 + "\n")
