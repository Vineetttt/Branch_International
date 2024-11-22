import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import pickle
from flask import Flask, request, jsonify
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
load_dotenv()

with open('Pickle Files/rf.pkl', 'rb') as file:
    model = pickle.load(file)

app = Flask(__name__)

db_params = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT', 5432),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

connection_string = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
engine = create_engine(connection_string)

def prepare_data_inference(loan_outcomes, gps_fixes, user_attributes):
    loan_outcomes['application_at'] = pd.to_datetime(loan_outcomes['application_at'])
    loan_outcomes['application_day'] = loan_outcomes['application_at'].dt.day
    loan_outcomes['application_month'] = loan_outcomes['application_at'].dt.month
    loan_outcomes['application_year'] = loan_outcomes['application_at'].dt.year
    loan_outcomes['is_weekend'] = loan_outcomes['application_at'].dt.dayofweek.isin([5, 6]).astype(int)
    
    gps_fixes['upload_delay'] = (gps_fixes['server_upload_at'] - gps_fixes['gps_fix_at']).dt.total_seconds()
    
    gps_agg = gps_fixes.groupby('user_id').agg({
        'accuracy': ['mean', 'max'],
        'upload_delay': ['mean', 'max'],
        'longitude': 'first',
        'latitude': 'first'
    }).reset_index()

    gps_agg.columns = ['user_id', 'avg_accuracy', 'max_accuracy', 'avg_upload_delay', 'max_upload_delay', 'longitude', 'latitude']
    
    merged_data = loan_outcomes.merge(user_attributes, on='user_id', how='left')
    merged_data = merged_data.merge(gps_agg, on='user_id', how='left')

    merged_data['cash_incoming_category'] = pd.qcut(merged_data['cash_incoming_30days'], q=4, labels=['Low', 'Medium-Low', 'Medium-High', 'High'], duplicates='drop')
    merged_data['age_group'] = pd.cut(merged_data['age'], bins=[0, 25, 35, 45, 60, 100], labels=['18-25', '26-35', '36-45', '46-60', '60+'])
    merged_data['application_hour'] = pd.to_datetime(merged_data['application_at']).dt.hour

    merged_data[['avg_accuracy', 'max_accuracy', 'avg_upload_delay', 'max_upload_delay']] = \
        merged_data[['avg_accuracy', 'max_accuracy', 'avg_upload_delay', 'max_upload_delay']].fillna(0)
    
    merged_data['latitude'] = merged_data['latitude'].fillna(merged_data['latitude'].median())
    merged_data['longitude'] = merged_data['longitude'].fillna(merged_data['longitude'].median())

    X = merged_data.drop(['user_id', 'application_at'], axis=1)
    
    return X


@app.route('/predict', methods=['POST'])
def predict():
    try:
        user_data = request.json
        user_id = user_data['user_id']
        
        query = f"SELECT * FROM loan_outcomes WHERE user_id = {user_id};"
        loan_outcome_data = pd.read_sql_query(query, engine)
        
        query = f"SELECT * FROM gps_fixes WHERE user_id = {user_id};"
        gps_fixes_data = pd.read_sql_query(query, engine)
        
        query = f"SELECT * FROM user_attributes WHERE user_id = {user_id};"
        user_attributes_data = pd.read_sql_query(query, engine)
        X = prepare_data_inference(loan_outcome_data, gps_fixes_data, user_attributes_data)
        prediction = model.predict(X)
        result = 'repaid' if prediction[0] == 1 else 'defaulted'
        
        return jsonify({'prediction': result})
    
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True)
