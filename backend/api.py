from flask import Flask, request, jsonify
from data_loader import load_drugbank_data
from agents import run_agent_swarm

app = Flask(__name__)
load_drugbank_data()  # Ensure DB is populated (run once)

@app.route('/optimize', methods=['POST'])
def optimize():
    try:
        data = request.json or {}  # {'drugs': ['DB00316', 'DB00635'], 'symptoms': 'fatigue'}
        if not data.get('drugs'):
            return jsonify({"error": "No drugs provided"}), 400
        results = run_agent_swarm(data)
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": f"API error: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)