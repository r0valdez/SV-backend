from dotenv import load_dotenv

import sys
import logging
import io
import os

from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient, UpdateOne
import pandas as pd

load_dotenv()

app = Flask(__name__)
CORS(app)

# logging config
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.info('API is starting up')

# MongoDB connection
MONGO_URI = os.getenv('MONGO_URI')
DATABASE_NAME = os.getenv('DATABASE_NAME')
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

@app.route('/')
def hello_world():
  return 'Hello, World!'

@app.route('/product/download', methods=['POST'])
def lookup_product():
  file = request.files['file']
  try:
    data = pd.read_csv(io.StringIO(file.stream.read().decode("UTF8")), delimiter = ',', dtype = {'Zip': str})
    zip_codes = data['Zip'].tolist()
    collection = db.Products
    documents = collection.find({'Zip': {'$in': zip_codes}}, {'_id': 0})
    doc_map = {doc['Zip']: doc for doc in documents}
      
    result = []
    for zip_code in zip_codes:
      if zip_code in doc_map:
        result.append(doc_map[zip_code])
      else:
        result.append({'Zip': zip_code, 'Product': 'N/A', 'Recorded': 'N/A', 'ORG User': 'N/A', 'Modified User': 'N/A'})

    logger.debug(result)

    return jsonify(result), 200
  except Exception as e:
    return jsonify({'error': str(e)}), 500


@app.route('/product/upload', methods=['POST'])
def update_product():
  file = request.files['file']
  try:
    data = pd.read_csv(io.StringIO(file.stream.read().decode("UTF8")), delimiter=',', dtype=str)
    logger.debug(data)

    operations = []
    for _, row in data.iterrows():
      zip_code = row['Zip']
      product = row['Product']
      recorded = row['Recorded']
      if 'ORG User' in row:
        org_user = row['ORG User']
      if 'ORG_User' in row:
        org_user = row['ORG_User']
      if 'Modified User' in row:
        modified_user = row['Modified User']
      if 'Modified_User' in row:
        modified_user = row['Modified_User']

      # Skip rows where Modified User is not specified
      if pd.isna(modified_user) or modified_user.strip() == '':
        continue
      
      # Check for 'N/A' values in the row
      if row.isnull().any():
        return jsonify({'error': f"Row with Zip {zip_code} contains 'N/A' value"}), 400

      # Prepare the data to be inserted/updated
      update_data = {
        'Zip': zip_code,
        'Product': product,
        'Recorded': recorded,
        'ORG User': org_user,
        'Modified User': modified_user
      }

      # Add the update operation to the list
      operations.append(
        UpdateOne({'Zip': zip_code}, {'$set': update_data}, upsert=True)
      )
    
    # Execute bulk operations
    if operations:
      db.Products.bulk_write(operations)

    return jsonify({'message': 'Database updated successfully'}), 200
  except Exception as e:
    return jsonify({'error': str(e)}), 500


@app.route('/population/download', methods=['POST'])
def lookup_population():
  file = request.files['file']
  try:
    data = pd.read_csv(io.StringIO(file.stream.read().decode("UTF8")), delimiter = ',', dtype = {'Zip': str})
    zip_codes = data['Zip'].tolist()
    collection = db.Populations
    documents = collection.find({'Zip': {'$in': zip_codes}}, {'_id': 0})
    doc_map = {doc['Zip']: doc for doc in documents}
      
    result = []
    for zip_code in zip_codes:
      if zip_code in doc_map:
        result.append(doc_map[zip_code])
      else:
        result.append({'Zip': zip_code, '5 Mile Population': 'N/A', 'Recorded': 'N/A', 'ORG User': 'N/A', 'Modified User': 'N/A'})

    logger.debug(result)

    return jsonify(result), 200
  except Exception as e:
    return jsonify({'error': str(e)}), 500


@app.route('/population/upload', methods=['POST'])
def update_population():
  file = request.files['file']
  try:
    data = pd.read_csv(io.StringIO(file.stream.read().decode("UTF8")), delimiter=',', dtype=str)
    logger.debug(data)

    operations = []
    for _, row in data.iterrows():
      zip_code = row['Zip']
      recorded = row['Recorded']
      if '5 Mile Population' in row:
        five_mile_population = row['5 Mile Population']
      if '5_Mile_Population' in row:
        five_mile_population = row['5_Mile_Population']
      if 'ORG User' in row:
        org_user = row['ORG User']
      if 'ORG_User' in row:
        org_user = row['ORG_User']
      if 'Modified User' in row:
        modified_user = row['Modified User']
      if 'Modified_User' in row:
        modified_user = row['Modified_User']

      # Skip rows where Modified User is not specified
      if pd.isna(modified_user) or modified_user.strip() == '':
        continue
      
      # Check for 'N/A' values in the row
      if row.isnull().any():
        return jsonify({'error': f"Row with Zip {zip_code} contains 'N/A' value"}), 400

      # Prepare the data to be inserted/updated
      update_data = {
        'Zip': zip_code,
        '5 Mile Population': five_mile_population,
        'Recorded': recorded,
        'ORG User': org_user,
        'Modified User': modified_user
      }

      # Add the update operation to the list
      operations.append(
        UpdateOne({'Zip': zip_code}, {'$set': update_data}, upsert=True)
      )
    
    # Execute bulk operations
    if operations:
      db.Populations.bulk_write(operations)

    return jsonify({'message': 'Database updated successfully'}), 200
  except Exception as e:
    return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
  app.run(debug=True)
