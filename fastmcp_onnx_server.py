# fastmcp_onnx_server.py
"""
FastMCP Server for ONNX XGBoost Fraud Detection Model
UPDATED: Automatically fetches features from MariaDB
"""
import math
from dotenv import load_dotenv
load_dotenv()

from fastmcp import FastMCP
import onnxruntime as ort
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastMCP server
mcp = FastMCP("Fraud Detection ONNX Model")

# Global ONNX session
# Global ONNX session
onnx_session = None

# Correct ONNX model path for Windows
MODEL_PATH = os.getenv(
    "ONNX_MODEL_PATH",
    r"C:\Users\NISHANT\OneDrive\Desktop\project_cdac\fraudDetection\models\fraud_model_xgboost_20260131_054113.onnx"
)


# MariaDB connection for feature engineering
mariadb = None


# ==================== LOAD ONNX MODEL ====================

def load_onnx_model():
    """Load ONNX model at startup"""
    global onnx_session
    
    try:
        if not os.path.exists(MODEL_PATH):
            logger.warning(f"⚠ ONNX model not found at {MODEL_PATH}")
            logger.warning("  Server will run in fallback mode")
            return False
        
        onnx_session = ort.InferenceSession(
            MODEL_PATH,
            providers=['CPUExecutionProvider']
        )
        
        input_name = onnx_session.get_inputs()[0].name
        input_shape = onnx_session.get_inputs()[0].shape
        
        logger.info(f"✓ ONNX model loaded successfully")
        logger.info(f"  Model: {MODEL_PATH}")
        logger.info(f"  Input: {input_name}, Shape: {input_shape}")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to load ONNX model: {e}")
        return False


def load_mariadb():
    """Load MariaDB connection for feature engineering"""
    global mariadb
    
    try:
        from real_databases1 import RealMariaDB
        
        MARIADB_HOST = os.getenv('MARIADB_HOST', 'localhost')
        MARIADB_PORT = int(os.getenv('MARIADB_PORT', 3306))
        MARIADB_USER = os.getenv('MARIADB_USER', 'fraud_user')
        MARIADB_PASSWORD = os.getenv('MARIADB_PASSWORD', 'fraud_password')
        MARIADB_DATABASE = os.getenv('MARIADB_DATABASE', 'fraud_detection')
        
        mariadb = RealMariaDB(
            host=MARIADB_HOST,
            port=MARIADB_PORT,
            user=MARIADB_USER,
            password=MARIADB_PASSWORD,
            database=MARIADB_DATABASE
        )
        
        logger.info(f"✓ MariaDB connected for feature engineering")
        return True
        
    except Exception as e:
        logger.error(f"✗ MariaDB connection failed: {e}")
        logger.warning("  Features will use default values")
        return False


# Initialize model and database on startup
load_onnx_model()
load_mariadb()


# ==================== HELPER FUNCTIONS ====================

def prepare_features(features: Dict) -> np.ndarray:
    """Convert feature dict to numpy array for ONNX"""
    
    # Feature order must match your XGBoost training
    feature_array = np.array([[
        features.get('amount', 0.0),
        features.get('hour_of_day', 12),
        features.get('day_of_week', 3),
        features.get('user_avg_amount', 0.0),
        features.get('user_total_transactions', 0)
    ]], dtype=np.float32)
    
    return feature_array


def engineer_features(
    user_id: str,
    amount: float,
    timestamp: str,
    device_id: str = "unknown",
    ip_address: str = "unknown",
    location: str = "unknown",
    merchant_id: str = "unknown"
) -> Dict:
    """
    Engineer all 16 features from basic transaction data + MariaDB
    
    This is the KEY function that bridges simple transaction data
    to the 16 features needed by the ONNX model
    """
    
    features = {}
    
    # ===== TIME-BASED FEATURES =====
    try:
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        features['hour_of_day'] = dt.hour
        features['day_of_week'] = dt.weekday()
    except:
        features['hour_of_day'] = 12
        features['day_of_week'] = 3
    
    features['amount'] = amount
    
    # ===== FETCH USER STATS FROM MARIADB =====
    if mariadb:
        try:
            user_stats = mariadb.get_user_stats(user_id)
            
            if user_stats:
                # User behavioral features
                features['user_total_transactions'] = user_stats['today_tx_count']
                features['user_avg_amount'] = user_stats['avg_daily_amount']
                features['user_transaction_frequency'] = user_stats['avg_daily_tx_count']
                
                # Risk score mapping: LOW=0.2, MEDIUM=0.5, HIGH=0.8
                risk_map = {'LOW': 0.2, 'MEDIUM': 0.5, 'HIGH': 0.8}
                features['user_risk_score'] = risk_map.get(user_stats['risk_level'], 0.5)
                
                # Amount deviation
                avg_amt = user_stats['avg_daily_amount']
                if avg_amt > 0:
                    features['amount_deviation_from_avg'] = (amount - avg_amt) / avg_amt
                else:
                    features['amount_deviation_from_avg'] = 0.0
                
            else:
                # New user - use defaults
                features['user_total_transactions'] = 0
                features['user_avg_amount'] = 0.0
                features['user_transaction_frequency'] = 0.0
                features['user_risk_score'] = 0.5
                features['amount_deviation_from_avg'] = 0.0
            
            # ===== DEVICE/IP CHANGE DETECTION =====
            device_info = mariadb.get_device_history(user_id, device_id)
            features['device_change'] = 0 if device_info['is_known_device'] else 1
            
            ip_info = mariadb.get_ip_history(user_id, ip_address)
            features['ip_change'] = 0 if ip_info['is_known_ip'] else 1
            
            # ===== VELOCITY FEATURES =====
            velocity = mariadb.calculate_transaction_velocity(user_id, hours=1)
            features['transaction_velocity'] = float(velocity)
            features['rapid_fire_flag'] = 1 if velocity > 5 else 0
            
            # ===== LOCATION RISK =====
            # Check if transaction is from home location
            if user_stats and location != 'unknown':
                home_city = user_stats.get('home_city', '')
                if home_city and home_city.lower() in location.lower():
                    features['location_risk_score'] = 0.1  # Low risk - home location
                else:
                    features['location_risk_score'] = 0.6  # Medium risk - different location
            else:
                features['location_risk_score'] = 0.3  # Unknown
            
        except Exception as e:
            logger.error(f"Error fetching from MariaDB: {e}")
            # Use default values on error
            features['user_total_transactions'] = 0
            features['user_avg_amount'] = 0.0
            features['user_transaction_frequency'] = 0.0
            features['user_risk_score'] = 0.5
            features['amount_deviation_from_avg'] = 0.0
            features['device_change'] = 0
            features['ip_change'] = 0
            features['transaction_velocity'] = 0.0
            features['rapid_fire_flag'] = 0
            features['location_risk_score'] = 0.3
    
    else:
        # MariaDB not available - use defaults
        features['user_total_transactions'] = 0
        features['user_avg_amount'] = 0.0
        features['user_transaction_frequency'] = 0.0
        features['user_risk_score'] = 0.5
        features['amount_deviation_from_avg'] = 0.0
        features['device_change'] = 0
        features['ip_change'] = 0
        features['transaction_velocity'] = 0.0
        features['rapid_fire_flag'] = 0
        features['location_risk_score'] = 0.3
    
    # ===== MERCHANT FRAUD RATE =====
    # TODO: Implement merchant fraud rate calculation when you have merchant stats table
    features['merchant_fraud_rate'] = 0.0
    
    # ===== PATTERN FLAGS =====
    # These require graph analysis (Memgraph) - set to 0 for now
    # The agent will check these separately using Memgraph MCP server
    features['circular_payment_flag'] = 0
    features['split_transaction_flag'] = 0
    
    return features


def fallback_prediction(features: Dict) -> Dict:
    """Simple rule-based fallback if ONNX model unavailable"""
    amount = features.get('amount', 0)
    
    # Simple heuristic
    fraud_prob = min(amount / 10000, 0.9)
    
    # Adjust for other signals
    if features.get('rapid_fire_flag', 0) == 1:
        fraud_prob += 0.1
    if features.get('device_change', 0) == 1:
        fraud_prob += 0.15
    if features.get('ip_change', 0) == 1:
        fraud_prob += 0.1
    if features.get('location_risk_score', 0) > 0.5:
        fraud_prob += 0.15
    
    fraud_prob = min(fraud_prob, 0.95)
    
    return {
        'fraud_probability': fraud_prob,
        'is_fraud': fraud_prob > 0.75,
        'confidence': 0.5,
        'model': 'fallback_rules'
    }


# ==================== MCP TOOLS ====================

@mcp.tool()
def predict_fraud_smart(
    transaction_id: str,
    user_id: str,
    amount: float,
    timestamp: str,
    merchant_id: str = "unknown",
    location: str = "unknown",
    device_id: str = "unknown",
    ip_address: str = "unknown"
) -> Dict:
    """
    🎯 SMART PREDICTION - Automatically engineers all 16 features!
    
    This is the MAIN tool agents should use. Just pass basic transaction data,
    and this function will:
    1. Fetch user stats from MariaDB
    2. Calculate all derived features
    3. Run ONNX inference
    
    Args:
        transaction_id: Transaction identifier
        user_id: User identifier
        amount: Transaction amount in USD
        timestamp: ISO format timestamp (e.g., "2026-02-01T14:30:00")
        merchant_id: Merchant identifier (optional)
        location: Transaction location (optional)
        device_id: Device identifier (optional)
        ip_address: IP address (optional)
    
    Returns:
        Dict with fraud_probability, is_fraud, confidence, and all features used
    """
    
    try:
        # 1. ENGINEER ALL FEATURES
        logger.info(f"Engineering features for transaction {transaction_id}")
        
        features = engineer_features(
            user_id=user_id,
            amount=amount,
            timestamp=timestamp,
            device_id=device_id,
            ip_address=ip_address,
            location=location,
            merchant_id=merchant_id
        )
        
        logger.info(f"Features engineered: {list(features.keys())}")
        
        # 2. RUN ONNX INFERENCE
        if onnx_session is not None:
            input_array = prepare_features(features)
            input_name = onnx_session.get_inputs()[0].name
            
            # Run inference
            import time
            start = time.time()
            outputs = onnx_session.run(None, {input_name: input_array})
            inference_time = (time.time() - start) * 1000  # ms
            
            raw_score = float(outputs[0][0][0])
            # Extract probabilities (XGBoost ONNX format)
            fraud_probability = min(max(raw_score / 100.0, 0.0), 1.0)  # Probability of class 1
            is_fraud = fraud_probability > 0.75
            confidence = abs(fraud_probability - 0.5) * 2
            
            logger.info(f"✓ ONNX Prediction: {fraud_probability:.4f} ({inference_time:.2f}ms)")
            
            return {
                'transaction_id': transaction_id,
                'fraud_probability': fraud_probability,
                'is_fraud': is_fraud,
                'confidence': confidence,
                'model': 'onnx_xgboost',
                'inference_time_ms': inference_time,
                'timestamp': datetime.now().isoformat(),
                'features_used': features  # Return features for transparency
            }
        else:
            # Fallback
            logger.warning("Using fallback prediction (ONNX not loaded)")
            result = fallback_prediction(features)
            result['transaction_id'] = transaction_id
            result['timestamp'] = datetime.now().isoformat()
            result['features_used'] = features
            return result
            
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        # Return safe fallback
        fallback = fallback_prediction({'amount': amount})
        fallback['transaction_id'] = transaction_id
        fallback['error'] = str(e)
        return fallback


@mcp.tool()
def predict_fraud_manual(
    amount: float,
    hour_of_day: int = 12,
    day_of_week: int = 3,
    user_total_transactions: int = 0,
    user_avg_amount: float = 0.0,
    user_transaction_frequency: float = 0.0,
    user_risk_score: float = 0.0,
    amount_deviation_from_avg: float = 0.0,
    transaction_velocity: float = 0.0,
    merchant_fraud_rate: float = 0.0,
    location_risk_score: float = 0.0,
    device_change: int = 0,
    ip_change: int = 0,
    rapid_fire_flag: int = 0,
    circular_payment_flag: int = 0,
    split_transaction_flag: int = 0
) -> Dict:
    """
    MANUAL PREDICTION - For when you already have all 16 features calculated.
    
    Most agents should use predict_fraud_smart() instead.
    
    Args:
        All 16 features as separate parameters
    
    Returns:
        Dict with fraud prediction
    """
    
    try:
        features = {
            'amount': amount,
            'hour_of_day': hour_of_day,
            'day_of_week': day_of_week,
            'user_total_transactions': user_total_transactions,
            'user_avg_amount': user_avg_amount,
            'user_transaction_frequency': user_transaction_frequency,
            'user_risk_score': user_risk_score,
            'amount_deviation_from_avg': amount_deviation_from_avg,
            'transaction_velocity': transaction_velocity,
            'merchant_fraud_rate': merchant_fraud_rate,
            'location_risk_score': location_risk_score,
            'device_change': device_change,
            'ip_change': ip_change,
            'rapid_fire_flag': rapid_fire_flag,
            'circular_payment_flag': circular_payment_flag,
            'split_transaction_flag': split_transaction_flag
        }
        
        # Use ONNX model if available
        if onnx_session is not None:
            input_array = prepare_features(features)
            input_name = onnx_session.get_inputs()[0].name
            
            outputs = onnx_session.run(None, {input_name: input_array})
            raw_score = float(outputs[0][0][0])  # ✓ Get the one value that exists
            fraud_probability = 1 / (1 + math.exp(-raw_score))  # ✓ Convert to probability
            is_fraud = fraud_probability > 0.75
            confidence = abs(fraud_probability - 0.5) * 2
            
            return {
                'fraud_probability': fraud_probability,
                'is_fraud': is_fraud,
                'confidence': confidence,
                'model': 'onnx_xgboost',
                'timestamp': datetime.now().isoformat()
            }
        else:
            return fallback_prediction(features)
            
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        return fallback_prediction(features)


@mcp.tool()
def get_model_info() -> Dict:
    """Get information about the loaded ONNX model and MariaDB connection"""
    
    if onnx_session is None:
        return {
            'status': 'not_loaded',
            'model_path': MODEL_PATH,
            'mode': 'fallback'
        }
    
    try:
        inputs = onnx_session.get_inputs()
        outputs = onnx_session.get_outputs()
        
        return {
            'status': 'loaded',
            'model_path': MODEL_PATH,
            'mode': 'onnx_inference',
            'input_name': inputs[0].name,
            'input_shape': inputs[0].shape,
            'input_type': str(inputs[0].type),
            'output_name': outputs[0].name,
            'output_shape': outputs[0].shape,
            'providers': onnx_session.get_providers(),
            'num_features': 16,
            'mariadb_connected': mariadb is not None,
            'feature_engineering': 'automatic' if mariadb else 'manual_only'
        }
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e)
        }


@mcp.resource("model://fraud-detector")
def get_model_resource() -> str:
    """Expose model as a resource"""
    import json
    info = get_model_info()
    return json.dumps(info, indent=2)


# ==================== MAIN ====================

if __name__ == "__main__":
    import sys
    
    print("="*80)
    print("FASTMCP ONNX FRAUD DETECTION SERVER (SMART FEATURE ENGINEERING)")
    print("="*80)
    print(f"\nModel Path: {MODEL_PATH}")
    print(f"ONNX Status: {'✓ Loaded' if onnx_session else '✗ Fallback Mode'}")
    print(f"MariaDB Status: {'✓ Connected' if mariadb else '✗ Disconnected'}")
    print("\nAvailable MCP Tools:")
    print("   predict_fraud_smart - Auto feature engineering (RECOMMENDED)")
    print("  - predict_fraud_manual - Manual features (advanced)")
    print("  - get_model_info - Model metadata")
    print("\nAvailable MCP Resources:")
    print("  - model://fraud-detector")
    print("\n" + "="*80)
    
    # Run FastMCP server
    mcp.run()
