# fastmcp_database_servers.py
"""
FastMCP Servers for Memgraph, MariaDB, and Qdrant
UPDATED: Works with actual MariaDB schema
"""
from dotenv import load_dotenv
load_dotenv()

from fastmcp import FastMCP
from typing import Dict, List, Optional
import json
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ==================== MEMGRAPH MCP SERVER ====================

memgraph_mcp = FastMCP("Memgraph Fraud Patterns")

# Import memgraph connector
try:
    from real_databases1 import RealMemgraph
    
    MEMGRAPH_HOST = os.getenv('MEMGRAPH_HOST')
    MEMGRAPH_PORT = int(os.getenv('MEMGRAPH_PORT', 7687))
    
    memgraph = RealMemgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
    logger.info(f"✓ Memgraph connected: {MEMGRAPH_HOST}:{MEMGRAPH_PORT}")
except Exception as e:
    logger.error(f"✗ Memgraph connection failed: {e}")
    memgraph = None


@memgraph_mcp.tool()
def detect_mules(user_id: str, hours: int = 24) -> Dict:
    """
    Detect money mule patterns using MAGE betweenness centrality.
    
    Args:
        user_id: User identifier
        hours: Time window to analyze (default 24 hours)
    
    Returns:
        Dict with mule detection results and indicators
    """
    if memgraph is None:
        return {"error": "Memgraph not connected", "is_mule": False}
    
    try:
        result = memgraph.detect_mule_pattern_mage(user_id, hours)
        return result
    except Exception as e:
        logger.error(f"Mule detection error: {e}")
        return {"error": str(e), "is_mule": False}


@memgraph_mcp.tool()
def detect_fraud_ring(user_id: str) -> Dict:
    """
    Detect fraud rings using MAGE Louvain community detection.
    
    Args:
        user_id: User identifier
    
    Returns:
        Dict with fraud ring detection results
    """
    if memgraph is None:
        return {"error": "Memgraph not connected", "is_fraud_ring": False}
    
    try:
        result = memgraph.detect_fraud_ring_mage(user_id)
        return result
    except Exception as e:
        logger.error(f"Fraud ring detection error: {e}")
        return {"error": str(e), "is_fraud_ring": False}


@memgraph_mcp.tool()
def detect_scatter_gather(user_id: str, hours: int = 6) -> Dict:
    """
    Detect scatter-gather patterns using MAGE PageRank.
    
    Args:
        user_id: User identifier
        hours: Time window to analyze
    
    Returns:
        Dict with scatter-gather pattern detection
    """
    if memgraph is None:
        return {"error": "Memgraph not connected", "is_scatter_gather": False}
    
    try:
        result = memgraph.detect_scatter_gather_mage(user_id, hours)
        return result
    except Exception as e:
        logger.error(f"Scatter-gather detection error: {e}")
        return {"error": str(e), "is_scatter_gather": False}


@memgraph_mcp.tool()
def detect_layering(user_id: str) -> Dict:
    """
    Detect layering patterns using MAGE cycle detection.
    
    Args:
        user_id: User identifier
    
    Returns:
        Dict with layering detection results
    """
    if memgraph is None:
        return {"error": "Memgraph not connected", "is_layering": False}
    
    try:
        result = memgraph.detect_layering_mage(user_id)
        return result
    except Exception as e:
        logger.error(f"Layering detection error: {e}")
        return {"error": str(e), "is_layering": False}


@memgraph_mcp.tool()
def detect_structuring(user_id: str, threshold: float = 10000.0) -> Dict:
    """
    Detect structuring - transactions near reporting thresholds.
    
    Args:
        user_id: User identifier
        threshold: Reporting threshold (default $10,000)
    
    Returns:
        Dict with structuring detection results
    """
    if memgraph is None:
        return {"error": "Memgraph not connected", "is_structuring": False}
    
    try:
        result = memgraph.detect_structuring_mage(user_id, threshold)
        return result
    except Exception as e:
        logger.error(f"Structuring detection error: {e}")
        return {"error": str(e), "is_structuring": False}


@memgraph_mcp.tool()
def detect_account_takeover(user_id: str) -> Dict:
    """
    Detect account takeover - device/IP changes and anomalies.
    
    Args:
        user_id: User identifier
    
    Returns:
        Dict with account takeover detection results
    """
    if memgraph is None:
        return {"error": "Memgraph not connected", "is_account_takeover": False}
    
    try:
        result = memgraph.detect_account_takeover_mage(user_id)
        return result
    except Exception as e:
        logger.error(f"Account takeover detection error: {e}")
        return {"error": str(e), "is_account_takeover": False}


@memgraph_mcp.resource("graph://fraud-patterns")
def get_fraud_patterns_resource() -> str:
    """Expose available fraud patterns as a resource"""
    patterns = {
        "available_patterns": [
            "mules",
            "fraud_ring",
            "scatter_gather",
            "layering",
            "structuring",
            "account_takeover"
        ],
        "mage_algorithms": [
            "Betweenness Centrality",
            "Louvain Community Detection",
            "PageRank",
            "Cycle Detection"
        ],
        "status": "connected" if memgraph else "disconnected"
    }
    return json.dumps(patterns, indent=2)


# ==================== MARIADB MCP SERVER ====================

mariadb_mcp = FastMCP("MariaDB User Behavior")

try:
    from real_databases1 import RealMariaDB
    
    MARIADB_HOST = os.getenv('MARIADB_HOST')
    MARIADB_PORT = int(os.getenv('MARIADB_PORT', 3307))
    MARIADB_USER = os.getenv('MARIADB_USER')
    MARIADB_PASSWORD = os.getenv('MARIADB_PASSWORD')
    MARIADB_DATABASE = os.getenv('MARIADB_DATABASE')
    
    mariadb = RealMariaDB(
        host=MARIADB_HOST,
        port=MARIADB_PORT,
        user=MARIADB_USER,
        password=MARIADB_PASSWORD,
        database=MARIADB_DATABASE
    )
    logger.info(f"✓ MariaDB connected: {MARIADB_HOST}:{MARIADB_PORT}")
except Exception as e:
    logger.error(f"✗ MariaDB connection failed: {e}")
    mariadb = None


@mariadb_mcp.tool()
def get_user_behavior(user_id: str) -> Dict:
    """
    Get comprehensive user behavioral statistics.
    
    Fetches from:
    - users table (status, risk level)
    - user_transaction_profile (historical averages)
    - daily_user_stats (today's activity)
    - user_home_location (geographic profile)
    
    Args:
        user_id: User identifier
    
    Returns:
        Dict with user statistics or error
    """
    if mariadb is None:
        return {"error": "MariaDB not connected", "status": "error"}
    
    try:
        stats = mariadb.get_user_stats(user_id)
        if stats:
            return {
                "status": "success",
                "user_id": user_id,
                "data": stats
            }
        else:
            return {
                "status": "not_found",
                "user_id": user_id,
                "message": "User not found in database"
            }
    except Exception as e:
        logger.error(f"Get user behavior error: {e}")
        return {"error": str(e), "status": "error"}


@mariadb_mcp.tool()
def check_device_history(user_id: str, device_id: str) -> Dict:
    """
    Check if device has been used by this user before.
    
    Queries user_network_profile table.
    
    Args:
        user_id: User identifier
        device_id: Device identifier
    
    Returns:
        Dict with device usage history
    """
    if mariadb is None:
        return {"error": "MariaDB not connected"}
    
    try:
        device_info = mariadb.get_device_history(user_id, device_id)
        return {
            "status": "success",
            "user_id": user_id,
            "device_id": device_id,
            "device_info": device_info
        }
    except Exception as e:
        logger.error(f"Check device history error: {e}")
        return {"error": str(e)}


@mariadb_mcp.tool()
def check_ip_history(user_id: str, ip_address: str) -> Dict:
    """
    Check if IP address has been used by this user before.
    
    Queries user_network_profile table.
    
    Args:
        user_id: User identifier
        ip_address: IP address
    
    Returns:
        Dict with IP usage history
    """
    if mariadb is None:
        return {"error": "MariaDB not connected"}
    
    try:
        ip_info = mariadb.get_ip_history(user_id, ip_address)
        return {
            "status": "success",
            "user_id": user_id,
            "ip_address": ip_address,
            "ip_info": ip_info
        }
    except Exception as e:
        logger.error(f"Check IP history error: {e}")
        return {"error": str(e)}


@mariadb_mcp.tool()
def get_recent_anomalies(user_id: str, hours: int = 24) -> Dict:
    """
    Get recent anomaly flags for user.
    
    Queries user_anomaly_flags table.
    
    Args:
        user_id: User identifier
        hours: Look back this many hours (default 24)
    
    Returns:
        Dict with list of recent anomalies
    """
    if mariadb is None:
        return {"error": "MariaDB not connected", "anomalies": []}
    
    try:
        anomalies = mariadb.get_recent_anomalies(user_id, hours)
        
        # Categorize by severity
        critical = [a for a in anomalies if a['severity'] == 'CRITICAL']
        high = [a for a in anomalies if a['severity'] == 'HIGH']
        medium = [a for a in anomalies if a['severity'] == 'MEDIUM']
        low = [a for a in anomalies if a['severity'] == 'LOW']
        
        return {
            "status": "success",
            "user_id": user_id,
            "timeframe_hours": hours,
            "total_anomalies": len(anomalies),
            "by_severity": {
                "CRITICAL": len(critical),
                "HIGH": len(high),
                "MEDIUM": len(medium),
                "LOW": len(low)
            },
            "anomalies": anomalies,
            "has_critical_flags": len(critical) > 0
        }
    except Exception as e:
        logger.error(f"Get anomalies error: {e}")
        return {"error": str(e), "anomalies": []}


@mariadb_mcp.tool()
def calculate_transaction_velocity(user_id: str, hours: int = 1) -> Dict:
    """
    Calculate transaction velocity (transactions per time period).
    
    Args:
        user_id: User identifier
        hours: Time window (default 1 hour)
    
    Returns:
        Dict with velocity metrics
    """
    if mariadb is None:
        return {"error": "MariaDB not connected", "velocity": 0}
    
    try:
        velocity = mariadb.calculate_transaction_velocity(user_id, hours)
        
        # Assess risk based on velocity
        if velocity > 10:
            risk_level = "CRITICAL"
        elif velocity > 5:
            risk_level = "HIGH"
        elif velocity > 3:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"
        
        return {
            "status": "success",
            "user_id": user_id,
            "timeframe_hours": hours,
            "transaction_count": velocity,
            "velocity_risk": risk_level,
            "is_rapid_fire": velocity > 5
        }
    except Exception as e:
        logger.error(f"Calculate velocity error: {e}")
        return {"error": str(e), "velocity": 0}


@mariadb_mcp.tool()
def log_agent_decision(
    transaction_id: str,
    decision: str,
    reasoning: str,
    confidence: float
) -> Dict:
    """
    Log the AI agent's decision.
    
    Note: This requires an agent_decisions table in your schema.
    Currently logs to console only.
    
    Args:
        transaction_id: Transaction identifier
        decision: FRAUD or LEGITIMATE
        reasoning: Explanation of the decision
        confidence: Confidence score (0.0-1.0)
    
    Returns:
        Dict with operation status
    """
    if mariadb is None:
        return {"error": "MariaDB not connected"}
    
    try:
        mariadb.log_agent_decision(
            transaction_id,
            'cot_fraud_agent_mcp',
            decision,
            reasoning,
            confidence
        )
        return {
            "status": "logged",
            "transaction_id": transaction_id,
            "decision": decision,
            "confidence": confidence,
            "note": "Logged to console (agent_decisions table not in schema)"
        }
    except Exception as e:
        logger.error(f"Log decision error: {e}")
        return {"error": str(e)}


@mariadb_mcp.resource("database://schema-info")
def get_schema_info() -> str:
    """Expose database schema information as a resource"""
    schema = {
        "tables": [
            "users",
            "daily_user_stats",
            "user_transaction_profile",
            "user_network_profile",
            "user_home_location",
            "user_anomaly_flags"
        ],
        "available_metrics": {
            "user_behavior": [
                "avg_daily_tx_count",
                "avg_daily_amount",
                "stddev_amount",
                "max_historical_tx",
                "risk_level",
                "account_age"
            ],
            "network_profile": [
                "device_history",
                "ip_history",
                "first_seen",
                "last_seen",
                "usage_count"
            ],
            "anomaly_flags": [
                "VOLUME_SPIKE",
                "AMOUNT_SPIKE",
                "LOCATION_CHANGE",
                "IP_CHANGE",
                "DEVICE_CHANGE",
                "THROUGHPUT_SPIKE",
                "ACCOUNT_TAKEOVER"
            ]
        },
        "status": "connected" if mariadb else "disconnected"
    }
    return json.dumps(schema, indent=2)


# ==================== QDRANT MCP SERVER ====================

qdrant_mcp = FastMCP("Qdrant Vector Similarity")

try:
    from real_databases1 import RealQdrant
    
    QDRANT_HOST = os.getenv('QDRANT_HOST', 'localhost')
    QDRANT_PORT = int(os.getenv('QDRANT_PORT', 6333))
    
    qdrant = RealQdrant(host=QDRANT_HOST, port=QDRANT_PORT)
    logger.info(f"✓ Qdrant connected: {QDRANT_HOST}:{QDRANT_PORT}")
except Exception as e:
    logger.error(f"✗ Qdrant connection failed: {e}")
    qdrant = None


@qdrant_mcp.tool()
def find_similar_frauds(
    transaction_text: str,
    limit: int = 5
) -> Dict:
    """
    Find similar fraudulent transactions using vector similarity.
    
    Args:
        transaction_text: Text description of transaction
        limit: Number of similar transactions to return
    
    Returns:
        Dict with similar transactions
    """
    if qdrant is None:
        return {"error": "Qdrant not connected", "similar_transactions": []}
    
    try:
        # Create simple embedding (in production use sentence-transformers)
        vector = [0.1] * 768
        
        results = qdrant.search_similar(vector, limit=limit, fraud_only=True)
        
        return {
            "status": "success",
            "query": transaction_text,
            "similar_transactions": results,
            "count": len(results)
        }
    except Exception as e:
        logger.error(f"Vector search error: {e}")
        return {"error": str(e), "similar_transactions": []}


@qdrant_mcp.resource("vector://collection-info")
def get_vector_collection_info() -> str:
    """Expose vector collection information as a resource"""
    info = {
        "collection_name": "fraud_transactions",
        "vector_size": 384,
        "distance_metric": "cosine",
        "status": "connected" if qdrant else "disconnected",
        "note": "Placeholder - implement with qdrant-client"
    }
    return json.dumps(info, indent=2)


# ==================== RUN SERVERS ====================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python fastmcp_database_servers.py [memgraph|mariadb|qdrant]")
        print("\nRun separate servers for each database:")
        print("  python fastmcp_database_servers.py memgraph")
        print("  python fastmcp_database_servers.py mariadb")
        print("  python fastmcp_database_servers.py qdrant")
        sys.exit(1)
    
    server_type = sys.argv[1].lower()
    
    if server_type == "memgraph":
        print("="*80)
        print("FASTMCP MEMGRAPH SERVER - Fraud Pattern Detection")
        print("="*80)
        print(f"Status: {'✓ Connected' if memgraph else '✗ Disconnected'}")
        print("\nAvailable Tools:")
        print("  - detect_mules")
        print("  - detect_fraud_ring")
        print("  - detect_scatter_gather")
        print("  - detect_layering")
        print("  - detect_structuring")
        print("  - detect_account_takeover")
        print("="*80 + "\n")
        memgraph_mcp.run()
        
    elif server_type == "mariadb":
        print("="*80)
        print("FASTMCP MARIADB SERVER - User Behavior (UPDATED SCHEMA)")
        print("="*80)
        print(f"Status: {'✓ Connected' if mariadb else '✗ Disconnected'}")
        print("\nTables:")
        print("  - users")
        print("  - daily_user_stats")
        print("  - user_transaction_profile")
        print("  - user_network_profile")
        print("  - user_home_location")
        print("  - user_anomaly_flags")
        print("\nAvailable Tools:")
        print("  - get_user_behavior")
        print("  - check_device_history")
        print("  - check_ip_history")
        print("  - get_recent_anomalies")
        print("  - calculate_transaction_velocity")
        print("  - log_agent_decision")
        print("="*80 + "\n")
        mariadb_mcp.run()
        
    elif server_type == "qdrant":
        print("="*80)
        print("FASTMCP QDRANT SERVER - Vector Similarity")
        print("="*80)
        print(f"Status: {'✓ Connected' if qdrant else '✗ Disconnected'}")
        print("\nAvailable Tools:")
        print("  - find_similar_frauds")
        print("="*80 + "\n")
        qdrant_mcp.run()
        
    else:
        print(f"Unknown server type: {server_type}")
        print("Use: memgraph, mariadb, or qdrant")
        sys.exit(1)
