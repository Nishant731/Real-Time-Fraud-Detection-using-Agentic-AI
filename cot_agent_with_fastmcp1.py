# cot_agent_with_fastmcp.py
"""
Chain-of-Thought Fraud Detection Agent with Kafka Real-Time Processing
Consumes transactions from Kafka and analyzes them using FastMCP servers
"""

from typing import Dict, List, Optional
from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, END
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
import asyncio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
import json
from datetime import datetime
import time
import logging
import os
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import signal
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ==================== STATE MODEL ====================

class ChainOfThoughtState(BaseModel):
    """State for CoT agent"""
    transaction: Dict = Field(default_factory=dict)
    transaction_id: str = ""
    observation: str = ""
    reasoning_chain: List[str] = Field(default_factory=list)
    evidence_plan: List[str] = Field(default_factory=list)
    evidence_gathered: Dict = Field(default_factory=dict)
    pattern_scores: Dict = Field(default_factory=dict)
    final_reasoning: str = ""
    is_fraud: bool = False
    fraud_type: Optional[str] = None
    fraud_score: float = 0.0
    confidence: float = 0.0
    current_phase: str = "observation"
    
    class Config:
        arbitrary_types_allowed = True


# ==================== MCP CLIENT MANAGER ====================

class MCPClientManager:
    """Manages connections to multiple FastMCP servers"""
    
    def __init__(self):
        self.servers = {
            'onnx': StdioServerParameters(
                command="python3",
                args=["fastmcp_onnx_server.py"]
            ),
            'memgraph': StdioServerParameters(
                command="python3",
                args=["fastmcp_database_servers.py", "memgraph"]
            ),
            'mariadb': StdioServerParameters(
                command="python3",
                args=["fastmcp_database_servers.py", "mariadb"]
            ),
            'qdrant': StdioServerParameters(
                command="python3",
                args=["fastmcp_database_servers.py", "qdrant"]
            )
        }
        
        logger.info("MCP Client Manager initialized with 4 servers")
    
    async def call_tool(self, server_name: str, tool_name: str, arguments: Dict) -> Dict:
        """Call a tool on a specific MCP server"""
        try:
            server_params = self.servers[server_name]
            
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    # Initialize session
                    await session.initialize()
                    
                    # Call the tool
                    result = await session.call_tool(tool_name, arguments)
                    
                    # Parse result
                    if hasattr(result, 'content'):
                        if isinstance(result.content, list) and len(result.content) > 0:
                            content = result.content[0]
                            if hasattr(content, 'text'):
                                return json.loads(content.text)
                    
                    return {"error": "Unable to parse MCP response"}
                    
        except Exception as e:
            logger.error(f"MCP call error ({server_name}.{tool_name}): {e}")
            return {"error": str(e)}


# ==================== COT AGENT WITH FASTMCP ====================

class CoTFraudAgentMCP:
    """Chain-of-Thought agent using FastMCP servers"""
    
    def __init__(self, api_key: str):
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash-lite",
            google_api_key=api_key,
            temperature=0.1
        )
        
        self.mcp = MCPClientManager()
        self.app = self._build_graph()
        
        # Statistics tracking
        self.stats = {
            'total_processed': 0,
            'fraud_detected': 0,
            'legitimate': 0,
            'errors': 0
        }
        
        logger.info("✓ CoT Fraud Agent with FastMCP initialized")
    
    def _build_graph(self):
        """Build CoT workflow"""
        workflow = StateGraph(ChainOfThoughtState)
        
        workflow.add_node("observation", self.observation_phase)
        workflow.add_node("reasoning", self.reasoning_phase)
        workflow.add_node("planning", self.planning_phase)
        workflow.add_node("evidence_gathering", self.evidence_gathering_phase)
        workflow.add_node("analysis", self.analysis_phase)
        workflow.add_node("decision", self.decision_phase)
        
        workflow.set_entry_point("observation")
        workflow.add_edge("observation", "reasoning")
        workflow.add_edge("reasoning", "planning")
        workflow.add_edge("planning", "evidence_gathering")
        workflow.add_edge("evidence_gathering", "analysis")
        workflow.add_edge("analysis", "decision")
        workflow.add_edge("decision", END)
        
        return workflow.compile()
    
    def observation_phase(self, state: ChainOfThoughtState) -> Dict:
        """Phase 1: Observe transaction"""
        txn = state.transaction
        
        prompt = f"""Observe this transaction carefully:

Transaction Details:
- Amount: ${txn.get('amount', 0):,.2f}
- User: {txn.get('user_id', txn.get('account_id', 'unknown'))}
- Merchant: {txn.get('merchant_id', 'unknown')}
- Location: {txn.get('location', 'unknown')}
- Device: {txn.get('device_id', 'unknown')}
- Time: {txn.get('timestamp', 'unknown')}

What stands out about this transaction? (2-3 sentences)"""
        
        response = self.llm.invoke([HumanMessage(content=prompt)])
        
        return {
            "observation": response.content,
            "current_phase": "observation_complete"
        }
    
    def reasoning_phase(self, state: ChainOfThoughtState) -> Dict:
        """Phase 2: Reason about fraud patterns"""
        txn = state.transaction
        
        prompt = f"""Transaction: ${txn.get('amount', 0):,.2f} from {txn.get('user_id', txn.get('account_id', 'unknown'))}

Consider these fraud patterns:
1. **mules** - Money transfer intermediaries (high betweenness)
2. **fraud_ring** - Organized groups working together
3. **scatter_gather** - Money fans out then comes back together
4. **layering** - Complex transaction chains to hide origin
5. **account_takeover** - Unauthorized account access (new device/IP)
6. **structuring** - Splitting amounts to avoid $10K threshold

Given this transaction, list 4-6 reasoning steps about which patterns might apply.
Focus on the amount, user behavior, and transaction characteristics."""
        
        response = self.llm.invoke([HumanMessage(content=prompt)])
        steps = [s.strip() for s in response.content.split('\n') if s.strip() and not s.strip().startswith('#')]
        
        # Clean up numbered lists
        steps = [s.split('.', 1)[-1].strip() if s[0].isdigit() else s for s in steps]
        
        return {
            "reasoning_chain": steps[:6],
            "current_phase": "reasoning_complete"
        }
    
    def planning_phase(self, state: ChainOfThoughtState) -> Dict:
        """Phase 3: Plan evidence gathering"""
        
        prompt = f"""Based on the reasoning, which tools should we use to gather evidence?

Available Tools:
1. **predict_fraud_smart** - ML model prediction (auto features from MariaDB)
2. **get_user_behavior** - User stats from MariaDB
3. **check_device_history** - Has user used this device before?
4. **check_ip_history** - Has user used this IP before?
5. **get_recent_anomalies** - Recent anomaly flags
6. **calculate_transaction_velocity** - Transaction velocity
7. **detect_mules** - Memgraph mule pattern detection
8. **detect_fraud_ring** - Memgraph fraud ring detection
9. **detect_scatter_gather** - Memgraph scatter-gather detection
10. **detect_layering** - Memgraph layering detection
11. **detect_account_takeover** - Memgraph account takeover
12. **find_similar_frauds** - Qdrant vector similarity

Select 4-6 most relevant tools, one per line (just the tool name)."""
        
        response = self.llm.invoke([HumanMessage(content=prompt)])
        
        # Extract tool names
        tools = []
        for line in response.content.split('\n'):
            line = line.strip()
            # Remove numbers, bullets, and asterisks
            line = line.lstrip('0123456789.-*• ')
            if line and any(tool in line for tool in [
                'predict_fraud_smart', 'get_user_behavior', 'check_device_history',
                'check_ip_history', 'get_recent_anomalies', 'calculate_transaction_velocity',
                'detect_mules', 'detect_fraud_ring', 'detect_scatter_gather',
                'detect_layering', 'detect_account_takeover', 'find_similar_frauds'
            ]):
                # Extract just the tool name
                for tool in ['predict_fraud_smart', 'get_user_behavior', 'check_device_history',
                             'check_ip_history', 'get_recent_anomalies', 'calculate_transaction_velocity',
                             'detect_mules', 'detect_fraud_ring', 'detect_scatter_gather',
                             'detect_layering', 'detect_account_takeover', 'find_similar_frauds']:
                    if tool in line:
                        tools.append(tool)
                        break
        
        return {
            "evidence_plan": tools[:6],
            "current_phase": "planning_complete"
        }
    
    async def _gather_evidence_async(self, state: ChainOfThoughtState) -> Dict:
        """Async evidence gathering using MCP"""
        evidence = {}
        txn = state.transaction
        user_id = txn.get('user_id', txn.get('account_id', 'UNKNOWN'))
        device_id = txn.get('device_id', 'unknown')
        ip_address = txn.get('ip_address', 'unknown')
        
        # Map tool names to MCP calls
        tool_map = {
            'predict_fraud_smart': ('onnx', 'predict_fraud_smart', {
                'transaction_id': txn.get('transaction_id', 'UNKNOWN'),
                'user_id': user_id,
                'amount': float(txn.get('amount', 0)),
                'timestamp': txn.get('timestamp', datetime.now().isoformat()),
                'merchant_id': txn.get('merchant_id', 'unknown'),
                'location': txn.get('location', 'unknown'),
                'device_id': device_id,
                'ip_address': ip_address
            }),
            'get_user_behavior': ('mariadb', 'get_user_behavior', {
                'user_id': user_id
            }),
            'check_device_history': ('mariadb', 'check_device_history', {
                'user_id': user_id,
                'device_id': device_id
            }),
            'check_ip_history': ('mariadb', 'check_ip_history', {
                'user_id': user_id,
                'ip_address': ip_address
            }),
            'get_recent_anomalies': ('mariadb', 'get_recent_anomalies', {
                'user_id': user_id,
                'hours': 24
            }),
            'calculate_transaction_velocity': ('mariadb', 'calculate_transaction_velocity', {
                'user_id': user_id,
                'hours': 1
            }),
            'detect_mules': ('memgraph', 'detect_mules', {
                'user_id': user_id
            }),
            'detect_fraud_ring': ('memgraph', 'detect_fraud_ring', {
                'user_id': user_id
            }),
            'detect_scatter_gather': ('memgraph', 'detect_scatter_gather', {
                'user_id': user_id
            }),
            'detect_layering': ('memgraph', 'detect_layering', {
                'user_id': user_id
            }),
            'detect_account_takeover': ('memgraph', 'detect_account_takeover', {
                'user_id': user_id
            }),
            'find_similar_frauds': ('qdrant', 'find_similar_frauds', {
                'transaction_text': f"{txn.get('amount')} {txn.get('transaction_type', 'payment')}"
            })
        }
        
        # Execute tools in plan
        for tool_name in state.evidence_plan:
            if tool_name in tool_map:
                server, mcp_tool, args = tool_map[tool_name]
                logger.info(f"📞 Calling {server}.{mcp_tool}...")
                
                result = await self.mcp.call_tool(server, mcp_tool, args)
                evidence[tool_name] = result
                
                logger.info(f"✓ Received response from {tool_name}")
        
        return evidence
    
    def evidence_gathering_phase(self, state: ChainOfThoughtState) -> Dict:
        """Phase 4: Gather evidence via MCP"""
        logger.info("\n" + "="*80)
        logger.info("EVIDENCE GATHERING PHASE")
        logger.info("="*80)
        
        # Run async evidence gathering
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            evidence = loop.run_until_complete(self._gather_evidence_async(state))
        finally:
            loop.close()
        
        logger.info(f"✓ Gathered evidence from {len(evidence)} tools")
        
        return {
            "evidence_gathered": evidence,
            "current_phase": "evidence_complete"
        }
    
    def analysis_phase(self, state: ChainOfThoughtState) -> Dict:
        """Phase 5: Analyze evidence"""
        evidence = state.evidence_gathered
        
        # Create concise summary of evidence
        evidence_summary = {}
        for tool, result in evidence.items():
            if isinstance(result, dict):
                # Extract key findings
                if 'fraud_probability' in result:
                    evidence_summary[tool] = f"Fraud prob: {result['fraud_probability']:.2%}"
                elif 'is_mule' in result:
                    evidence_summary[tool] = f"Mule: {result['is_mule']}"
                elif 'is_fraud_ring' in result:
                    evidence_summary[tool] = f"Ring: {result['is_fraud_ring']}"
                elif 'is_scatter_gather' in result:
                    evidence_summary[tool] = f"Scatter: {result['is_scatter_gather']}"
                elif 'is_layering' in result:
                    evidence_summary[tool] = f"Layering: {result['is_layering']}"
                elif 'is_account_takeover' in result:
                    evidence_summary[tool] = f"Takeover: {result['is_account_takeover']}"
                elif 'total_anomalies' in result:
                    evidence_summary[tool] = f"Anomalies: {result['total_anomalies']}"
                elif 'is_known_device' in result.get('device_info', {}):
                    evidence_summary[tool] = f"Known device: {result['device_info']['is_known_device']}"
                elif 'data' in result:
                    evidence_summary[tool] = f"Risk: {result['data'].get('risk_level', 'unknown')}"
                else:
                    evidence_summary[tool] = "Retrieved"
        
        prompt = f"""Analyze this fraud investigation evidence:

{json.dumps(evidence_summary, indent=2)}

Score each fraud pattern (0.0 to 1.0):
- mules
- fraud_ring  
- scatter_gather
- layering
- account_takeover
- structuring

Provide your analysis and pattern scores in JSON format:
{{
  "pattern_scores": {{"mules": 0.0-1.0, "fraud_ring": 0.0-1.0, ...}},
  "final_reasoning": "2-3 sentence explanation of your analysis"
}}

Return ONLY valid JSON."""
        
        response = self.llm.invoke([HumanMessage(content=prompt)])
        
        try:
            content = response.content
            # Extract JSON from markdown if present
            if '```json' in content:
                start = content.find('```json') + 7
                end = content.find('```', start)
                content = content[start:end].strip()
            elif '```' in content:
                start = content.find('```') + 3
                end = content.find('```', start)
                content = content[start:end].strip()
            
            analysis = json.loads(content)
            
            return {
                "pattern_scores": analysis.get('pattern_scores', {}),
                "final_reasoning": analysis.get('final_reasoning', 'Analysis completed'),
                "current_phase": "analysis_complete"
            }
        except Exception as e:
            logger.error(f"Analysis parse error: {e}")
            logger.error(f"Response was: {response.content}")
            
            # Fallback - extract from ML model if available
            if 'predict_fraud_smart' in evidence:
                ml_result = evidence['predict_fraud_smart']
                fraud_prob = ml_result.get('fraud_probability', 0.5)
                
                return {
                    "pattern_scores": {
                        "mules": fraud_prob * 0.8,
                        "fraud_ring": fraud_prob * 0.6,
                        "scatter_gather": fraud_prob * 0.5,
                        "layering": fraud_prob * 0.7,
                        "account_takeover": fraud_prob * 0.9,
                        "structuring": fraud_prob * 0.85
                    },
                    "final_reasoning": f"Based on ML model prediction: {fraud_prob:.2%} fraud probability",
                    "current_phase": "analysis_complete"
                }
            else:
                return {
                    "pattern_scores": {},
                    "final_reasoning": "Analysis failed - insufficient evidence",
                    "current_phase": "analysis_error"
                }
    
    async def _log_decision_async(self, transaction_id: str, decision: str, 
                                   reasoning: str, confidence: float):
        """Log decision via MariaDB MCP"""
        try:
            await self.mcp.call_tool('mariadb', 'log_agent_decision', {
                'transaction_id': transaction_id,
                'decision': decision,
                'reasoning': reasoning,
                'confidence': confidence
            })
        except Exception as e:
            logger.error(f"Failed to log decision: {e}")
    
    def decision_phase(self, state: ChainOfThoughtState) -> Dict:
        """Phase 6: Make final decision"""
        scores = state.pattern_scores
        
        if not scores:
            return {
                "is_fraud": False,
                "fraud_score": 0.0,
                "confidence": 0.5,
                "fraud_type": None,
                "current_phase": "complete"
            }
        
        # Find highest pattern
        max_pattern = max(scores.items(), key=lambda x: x[1])
        fraud_type = max_pattern[0]
        max_score = max_pattern[1]
        
        # Overall score (weighted average of top 3)
        sorted_scores = sorted(scores.values(), reverse=True)
        if len(sorted_scores) >= 3:
            overall_score = (sorted_scores[0] * 0.5 + sorted_scores[1] * 0.3 + sorted_scores[2] * 0.2)
        elif len(sorted_scores) == 2:
            overall_score = (sorted_scores[0] * 0.6 + sorted_scores[1] * 0.4)
        else:
            overall_score = sorted_scores[0] if sorted_scores else 0.5
        
        is_fraud = overall_score > 0.6
        confidence = abs(overall_score - 0.5) * 2  # 0.5 -> 0%, 1.0 -> 100%
        
        # Update statistics
        self.stats['total_processed'] += 1
        if is_fraud:
            self.stats['fraud_detected'] += 1
        else:
            self.stats['legitimate'] += 1
        
        # Log decision via MCP
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._log_decision_async(
                state.transaction_id,
                'FRAUD' if is_fraud else 'LEGITIMATE',
                state.final_reasoning,
                confidence
            ))
        finally:
            loop.close()
        
        return {
            "is_fraud": is_fraud,
            "fraud_type": fraud_type if is_fraud else None,
            "fraud_score": overall_score,
            "confidence": confidence,
            "current_phase": "complete"
        }
    
    def analyze_transaction(self, transaction: Dict) -> ChainOfThoughtState:
        """Fast analysis with single LLM call"""
    
        initial_state = ChainOfThoughtState(
        transaction=transaction,
        transaction_id=transaction.get('transaction_id', 'UNKNOWN')
    )
    
    # Gather evidence first (no LLM)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
        try:
        # Plan: use ALL tools (skip LLM planning)
            initial_state.evidence_plan = [
                'predict_fraud_smart',
                'get_user_behavior', 
                'check_device_history'
        ]
        
        # Gather evidence
            evidence = loop.run_until_complete(self._gather_evidence_async(initial_state))
            initial_state.evidence_gathered = evidence
        finally:
            loop.close()
    
    # Single LLM call for full analysis
        prompt = f'''Analyze this fraud transaction:

    Transaction: ${transaction.get('amount', 0):,.2f}
    User: {transaction.get('user_id', 'unknown')}
    Type: {transaction.get('transaction_type', 'unknown')}

    Evidence:
    {json.dumps(evidence, indent=2)}

    Provide complete analysis in JSON:
    {{
        "observation": "2-3 sentences",
        "reasoning_steps": ["step1", "step2", "step3", "step4"],
        "pattern_scores": {{
        "mules": 0.0-1.0,
        "fraud_ring": 0.0-1.0,
        "scatter_gather": 0.0-1.0,
        "layering": 0.0-1.0,
        "account_takeover": 0.0-1.0,
        "structuring": 0.0-1.0
      }},
      "final_reasoning": "conclusion"
    }}'''
        time.sleep(7)
        response = self.llm.invoke([HumanMessage(content=prompt)])
    
        try:
            content = response.content
            if '```json' in content:
                start = content.find('```json') + 7
                end = content.find('```', start)
                content = content[start:end].strip()
        
            analysis = json.loads(content)
        
            # Complete state
            initial_state.observation = analysis.get('observation', '')
            initial_state.reasoning_chain = analysis.get('reasoning_steps', [])
            initial_state.pattern_scores = analysis.get('pattern_scores', {})
            initial_state.final_reasoning = analysis.get('final_reasoning', '')
        
        # Make decision
            if initial_state.pattern_scores:
                max_pattern = max(initial_state.pattern_scores.items(), key=lambda x: x[1])
                initial_state.fraud_type = max_pattern[0]
            
                sorted_scores = sorted(initial_state.pattern_scores.values(), reverse=True)
                overall_score = (sorted_scores[0] * 0.5 + sorted_scores[1] * 0.3 + sorted_scores[2] * 0.2)
            
                initial_state.is_fraud = overall_score > 0.6
                initial_state.fraud_score = overall_score
                initial_state.confidence = abs(overall_score - 0.5) * 2
        
            return initial_state
        
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            return initial_state
    
    def print_result(self, result: ChainOfThoughtState):
        """Print analysis result in a nice format"""
        print("\n" + "="*80)
        print("FRAUD ANALYSIS RESULT")
        print("="*80)
        print(f"\n📋 Transaction ID: {result.transaction_id}")
        print(f"💰 Amount: ${result.transaction.get('amount', 0):,.2f}")
        print(f"\n📝 Observation:\n{result.observation}\n")
        print(f"🧠 Reasoning Steps:")
        for i, step in enumerate(result.reasoning_chain, 1):
            print(f"  {i}. {step}")
        print(f"\n📊 Evidence Gathered: {', '.join(result.evidence_plan)}\n")
        print(f"🎯 Pattern Scores:")
        for pattern, score in sorted(result.pattern_scores.items(), key=lambda x: x[1], reverse=True):
            print(f"  {pattern}: {score:.2%}")
        print(f"\n⚖️  Decision: {'🚨 FRAUD' if result.is_fraud else '✅ LEGITIMATE'}")
        if result.is_fraud:
            print(f"  Type: {result.fraud_type}")
        print(f"  Score: {result.fraud_score:.2%}")
        print(f"  Confidence: {result.confidence:.2%}")
        print(f"\n💭 Final Reasoning:\n{result.final_reasoning}")
        print("="*80)
        
        # Print statistics
        print(f"\n📈 Statistics:")
        print(f"  Total Processed: {self.stats['total_processed']}")
        print(f"  Fraud Detected: {self.stats['fraud_detected']}")
        print(f"  Legitimate: {self.stats['legitimate']}")
        print(f"  Errors: {self.stats['errors']}")
        print("="*80 + "\n")


# ==================== KAFKA CONSUMER ====================

class KafkaTransactionConsumer:
    """Kafka consumer for real-time transaction processing"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str = 'fraud_detection_cot_agent'
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self.running = False
        
        logger.info(f"Kafka Consumer initialized for topic: {topic}")
    
    def connect(self):
        """Connect to Kafka"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='latest',  # Start from latest (real-time)
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=1000  # 1 second timeout for graceful shutdown
            )
            logger.info(f"✓ Connected to Kafka: {self.bootstrap_servers}")
            logger.info(f"✓ Subscribed to topic: {self.topic}")
            return True
        except KafkaError as e:
            logger.error(f"✗ Kafka connection failed: {e}")
            return False
    
    def consume_transactions(self, agent: CoTFraudAgentMCP):
        """Consume and process transactions in real-time"""
        self.running = True
        
        logger.info("\n" + "="*80)
        logger.info("🚀 STARTING REAL-TIME FRAUD DETECTION")
        logger.info("="*80)
        logger.info(f"Listening on topic: {self.topic}")
        logger.info("Press Ctrl+C to stop\n")
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                try:
                    transaction = message.value
                    
                    # Log received transaction
                    logger.info(f"\n{'─'*80}")
                    logger.info(f"📨 Received transaction from Kafka")
                    logger.info(f"   Transaction ID: {transaction.get('transaction_id', 'UNKNOWN')}")
                    logger.info(f"   Amount: ${transaction.get('amount', 0):,.2f}")
                    logger.info(f"   User: {transaction.get('user_id', transaction.get('account_id', 'UNKNOWN'))}")
                    logger.info(f"{'─'*80}")
                    
                    # Analyze transaction
                    result = agent.analyze_transaction(transaction)
                    
                    # Print result
                    agent.print_result(result)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON: {e}")
                    agent.stats['errors'] += 1
                except Exception as e:
                    logger.error(f"Error processing transaction: {e}")
                    agent.stats['errors'] += 1
                    
        except KeyboardInterrupt:
            logger.info("\n\n⚠️  Interrupted by user")
        finally:
            self.stop()
    
    def stop(self):
        """Stop consuming"""
        self.running = False
        if self.consumer:
            logger.info("Closing Kafka consumer...")
            self.consumer.close()
            logger.info("✓ Kafka consumer closed")


# ==================== MAIN ====================

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully"""
    logger.info("\n\n⚠️  Received shutdown signal")
    sys.exit(0)


if __name__ == "__main__":
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    print("="*80)
    print("COT FRAUD AGENT - TESTING WITH FRAUD_DATA.JSON")
    print("="*80 + "\n")
    
    # Get API key
    GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
    
    if not GOOGLE_API_KEY:
        logger.error("❌ GOOGLE_API_KEY not found in environment")
        logger.info("Please set GOOGLE_API_KEY in your .env file")
        sys.exit(1)
    
    # Initialize agent
    logger.info("Initializing CoT Agent...")
    agent = CoTFraudAgentMCP(api_key=GOOGLE_API_KEY)
    
    # Load fraud_data.json
    fraud_data_file = "fraud_data.json"
    
    if not os.path.exists(fraud_data_file):
        logger.error(f"❌ File not found: {fraud_data_file}")
        logger.info("Please ensure fraud_data.json is in the current directory")
        sys.exit(1)
    
    logger.info(f"Loading transactions from {fraud_data_file}...")
    
    try:
        with open(fraud_data_file, 'r') as f:
            transactions = json.load(f)
        
        logger.info(f"✓ Loaded {len(transactions):,} transactions")
    except json.JSONDecodeError as e:
        logger.error(f"❌ Error parsing JSON file: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Error loading file: {e}")
        sys.exit(1)
    
    # Ask user how many transactions to test
    print(f"\n📊 Found {len(transactions):,} transactions in fraud_data.json")
    print("\nOptions:")
    print("  1. Test first 10 transactions (quick test)")
    print("  2. Test first 100 transactions (thorough test)")
    print("  3. Test first 1000 transactions")
    print("  4. Test all transactions (this will take VERY long!)")
    print("  5. Test specific range")
    print("  6. Exit")
    
    choice = input("\nEnter your choice (1-6): ").strip()
    
    if choice == "1":
        test_transactions = transactions[:10]
        logger.info("Testing first 10 transactions...")
    elif choice == "2":
        test_transactions = transactions[:100]
        logger.info("Testing first 100 transactions...")
    elif choice == "3":
        test_transactions = transactions[:1000]
        logger.info("Testing first 1000 transactions...")
    elif choice == "4":
        confirm = input(f"\n⚠️  This will analyze ALL {len(transactions):,} transactions. This may take hours! Continue? (yes/no): ")
        if confirm.lower() == 'yes':
            test_transactions = transactions
            logger.info(f"Testing all {len(transactions):,} transactions...")
        else:
            logger.info("Cancelled")
            sys.exit(0)
    elif choice == "5":
        start = int(input("Start index (0-based): "))
        end = int(input("End index: "))
        test_transactions = transactions[start:end]
        logger.info(f"Testing transactions {start} to {end} ({len(test_transactions)} total)...")
    else:
        logger.info("Exiting")
        sys.exit(0)
    
    # Process transactions
    print("\n" + "="*80)
    print(f"STARTING ANALYSIS OF {len(test_transactions):,} TRANSACTIONS")
    print("="*80)
    
    results = []
    
    for i, txn in enumerate(test_transactions, 1):
        try:
            # Convert fraud_data.json format to agent format
            # Your format:
            # {
            #   "transaction_id": "f2da1203-...",
            #   "from_account": "ACCT_00089",
            #   "to_account": "ACCT_00109",
            #   "amount": 3342.77,
            #   "timestamp": "2025-11-25T15:38:34.285476",
            #   "type": "RING_ACTIVITY",
            #   "ip_address": "212.125.189.38",
            #   "device_id": "9350312d-...",
            #   "location": {
            #     "latitude": 74.674523,
            #     "longitude": 36.491223,
            #     "city": "South Mollyton",
            #     "country": "Canada"
            #   }
            # }
            
            location_obj = txn.get('location', {})
            location_str = f"{location_obj.get('city', 'Unknown')}, {location_obj.get('country', 'Unknown')}"
            
            normalized_txn = {
                'transaction_id': txn.get('transaction_id'),
                'user_id': txn.get('from_account'),  # Use from_account as user_id
                'account_id': txn.get('from_account'),
                'merchant_id': txn.get('to_account'),  # Use to_account as merchant
                'amount': float(txn.get('amount', 0)),
                'timestamp': txn.get('timestamp'),
                'transaction_type': txn.get('type'),
                'ip_address': txn.get('ip_address'),
                'device_id': txn.get('device_id'),
                'location': location_str
            }
            
            # Log progress every 10 transactions
            if i % 10 == 0 or i == 1:
                logger.info(f"\n{'─'*80}")
                logger.info(f"📊 Progress: {i}/{len(test_transactions)} ({i/len(test_transactions)*100:.1f}%)")
                logger.info(f"   Transaction: {normalized_txn['transaction_id']}")
                logger.info(f"   Amount: ${normalized_txn['amount']:,.2f}")
                logger.info(f"   Type: {normalized_txn['transaction_type']}")
                logger.info(f"{'─'*80}")
            
            # Analyze transaction
            result = agent.analyze_transaction(normalized_txn)
            
            # Store result
            results.append({
                'transaction_id': result.transaction_id,
                'is_fraud': result.is_fraud,
                'fraud_type': result.fraud_type,
                'fraud_score': result.fraud_score,
                'confidence': result.confidence,
                'original_type': normalized_txn['transaction_type'],
                'amount': normalized_txn['amount']
            })
            
            # Print quick summary
            fraud_status = "🚨 FRAUD" if result.is_fraud else "✅ LEGIT"
            print(f"{i:4d}. {fraud_status} | {result.transaction_id[:20]}... | ${normalized_txn['amount']:>10,.2f} | Score: {result.fraud_score:>5.1%} | Type: {normalized_txn['transaction_type']}")
            
            # Print detailed stats every 50 transactions
            if i % 50 == 0:
                print(f"\n{'─'*80}")
                print(f"📈 Interim Statistics:")
                print(f"   Analyzed: {agent.stats['total_processed']}")
                print(f"   Fraud: {agent.stats['fraud_detected']} ({agent.stats['fraud_detected']/agent.stats['total_processed']*100:.1f}%)")
                print(f"   Legitimate: {agent.stats['legitimate']}")
                print(f"   Errors: {agent.stats['errors']}")
                print(f"{'─'*80}\n")
            
        except KeyboardInterrupt:
            logger.info("\n\n⚠️  Analysis interrupted by user")
            break
        except Exception as e:
            logger.error(f"❌ Error analyzing transaction {i}: {e}")
            agent.stats['errors'] += 1
            continue
    
    # Final Statistics
    print("\n" + "="*80)
    print("FINAL ANALYSIS RESULTS")
    print("="*80)
    print(f"\n📊 Overall Statistics:")
    print(f"  Total Analyzed: {agent.stats['total_processed']:,}")
    print(f"  Fraud Detected: {agent.stats['fraud_detected']:,}")
    print(f"  Legitimate: {agent.stats['legitimate']:,}")
    print(f"  Errors: {agent.stats['errors']:,}")
    
    if agent.stats['total_processed'] > 0:
        fraud_rate = (agent.stats['fraud_detected'] / agent.stats['total_processed']) * 100
        print(f"  Fraud Rate: {fraud_rate:.2f}%")
    
    # Breakdown by detected fraud type
    print(f"\n🎯 Detected Fraud Types:")
    fraud_type_counts = {}
    for result in results:
        if result['is_fraud'] and result['fraud_type']:
            fraud_type_counts[result['fraud_type']] = fraud_type_counts.get(result['fraud_type'], 0) + 1
    
    if fraud_type_counts:
        for fraud_type, count in sorted(fraud_type_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {fraud_type.ljust(20)}: {count:4d} ({count/agent.stats['fraud_detected']*100:5.1f}%)")
    else:
        print("  No fraud detected")
    
    # Breakdown by original type
    print(f"\n🏷️  Original Transaction Types:")
    original_type_counts = {}
    for result in results:
        orig_type = result['original_type']
        original_type_counts[orig_type] = original_type_counts.get(orig_type, 0) + 1
    
    for orig_type, count in sorted(original_type_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {orig_type.ljust(20)}: {count:4d}")
    
    # Comparison with original labels
    print(f"\n🔍 Original Labels vs Agent Detection:")
    original_fraud_count = sum(1 for txn in test_transactions if txn.get('type') != 'LEGITIMATE')
    detected_fraud_count = agent.stats['fraud_detected']
    
    print(f"  Original Fraud Labels: {original_fraud_count:,}")
    print(f"  Agent Detected Fraud: {detected_fraud_count:,}")
    print(f"  Difference: {abs(original_fraud_count - detected_fraud_count):,}")
    
    if original_fraud_count > 0:
        match_rate = (min(original_fraud_count, detected_fraud_count) / original_fraud_count) * 100
        print(f"  Match Rate: {match_rate:.1f}%")
    
    # Save results to file
    results_file = f"analysis_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    print(f"\n💾 Saving results to: {results_file}")
    
    with open(results_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'total_transactions': len(test_transactions),
            'statistics': agent.stats,
            'fraud_type_breakdown': fraud_type_counts,
            'original_type_breakdown': original_type_counts,
            'original_fraud_count': original_fraud_count,
            'detected_fraud_count': detected_fraud_count,
            'results': results
        }, f, indent=2)
    
    logger.info(f"✓ Results saved to: {results_file}")
    
    print("="*80)
    print("\n✅ Analysis complete!")
    print(f"\nResults saved to: {results_file}")
    print("\nYou can now:")
    print("  1. Review results in the JSON file")
    print("  2. Compare detected vs original fraud labels")
    print("  3. Analyze fraud type distributions")
    print("  4. Adjust detection thresholds if needed")
    print("="*80)
