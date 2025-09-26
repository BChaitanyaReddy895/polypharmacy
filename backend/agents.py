import sqlite3
import numpy as np
from scipy.optimize import minimize
from scipy.stats import norm
import pandas as pd

class BaseAgent:
    def __init__(self, name, db_path='db/polypharm.db'):
        self.name = name
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def query_db(self, sql, params=()):
        return self.cursor.execute(sql, params).fetchall()

# Feature 1: Agent Debate Protocol
class DebateAgent(BaseAgent):
    def process(self, drugs):
        """Simulate agent debate to resolve drug interaction uncertainties using game theory."""
        try:
            # Query interactions for input drugs (e.g., ['DB00316', 'DB00635'])
            placeholders = ','.join(['?'] * len(drugs))
            sql = f"SELECT effect FROM drug_interactions WHERE drugbank-id IN ({placeholders}) OR interacting_drugbank-id IN ({placeholders})"
            interactions = self.query_db(sql, drugs + drugs)
            uncertainties = [effect for effect, in interactions]  # E.g., ['Increased risk of bleeding', ...]
            
            if not uncertainties:
                return {"consensus": "No interactions found", "details": []}
            
            # Mock game theory: Minimize conflict score (simplified for prototype)
            def utility(x): return sum(len(u) for u in uncertainties) - sum(x)
            consensus = minimize(utility, [0] * len(uncertainties), bounds=[(0, 1)] * len(uncertainties))
            return {
                "consensus": f"Resolved {len(uncertainties)} interactions with confidence {consensus.fun:.2f}",
                "details": uncertainties[:3]  # Limit for demo
            }
        except Exception as e:
            return {"error": f"DebateAgent failed: {str(e)}"}

# Feature 2: Probabilistic Outcome Simulator
class SimulatorAgent(BaseAgent):
    def process(self, patient_vars, drugs):
        """Run Monte Carlo simulations to estimate polypharmacy risk."""
        try:
            # Count interactions for risk estimation
            placeholders = ','.join(['?'] * len(drugs))
            sql = f"SELECT COUNT(*) FROM drug_interactions WHERE drugbank-id IN ({placeholders})"
            interaction_count = self.query_db(sql, drugs)[0][0]
            
            # Monte Carlo: Risk scales with interaction count
            trials = [norm.rvs(loc=0.1 * interaction_count, scale=0.05) for _ in range(1000)]
            risk_prob = np.clip(np.mean(trials), 0, 1)
            return {"risk_prob": risk_prob, "interaction_count": interaction_count}
        except Exception as e:
            return {"error": f"SimulatorAgent failed: {str(e)}"}

# Feature 5: Adverse Event Predictor with Explainability
class AdverseEventAgent(BaseAgent):
    def process(self, drugs):
        """Predict and explain adverse events based on interactions."""
        try:
            placeholders = ','.join(['?'] * len(drugs))
            sql = f"SELECT effect, drugbank-id, interacting_drugbank-id FROM drug_interactions WHERE drugbank-id IN ({placeholders})"
            interactions = self.query_db(sql, drugs)
            
            explanations = []
            for effect, drug_id, interact_id in interactions:
                # Get drug names for readability