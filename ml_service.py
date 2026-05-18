from flask import Flask, request, jsonify
from flask_cors import CORS
import joblib
import pandas as pd

app = Flask(__name__)
CORS(app)


model = joblib.load('best_model.pkl')


EXPECTED_COLUMNS = [
    'protocol', 'action', 'log_type', 'bytes_transferred', 
    'user_agent', 'request_path', 'hour', 'day', 'month'
]

@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json()
    
    # 1. Renommer les variables reçues de React
    if 'bytes' in data:
        data['bytes_transferred'] = data.pop('bytes')
    if 'path' in data:
        data['request_path'] = data.pop('path')
        
    # 2. Créer le DataFrame
    df = pd.DataFrame([data])
    
    # 3. Sécurité : Ajouter les colonnes manquantes si jamais React en oublie
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = 0
            
    # 4. RÉORGANISER les colonnes dans l'ordre strict
    df = df[EXPECTED_COLUMNS]
    
    # 5. Faire la prédiction
    prediction = model.predict(df)[0]
    probability = model.predict_proba(df).max()
    
    return jsonify({
        'prediction': int(prediction),
        'confidence': round(float(probability) * 100, 2)
    })

if __name__ == '__main__':
    app.run(port=5001)