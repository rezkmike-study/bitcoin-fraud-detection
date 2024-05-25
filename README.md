# ML Project - Bitcoin Fraud Detection (WQD7007)

This project aims to build a robust Bitcoin fraud detection system using various machine learning models. The models are trained to classify transactions as fraudulent or non-fraudulent based on a set of features derived from transaction data. The best model is selected and deployed as a Streamlit application for easy interaction.

## Team Members (Contributors):

1. Vinod
2. Ronjon
3. Aloysius
4. Kamil
5. Atikah

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Data Preparation](#data-preparation)
- [Model Training](#model-training)
- [Model Deployment](#model-deployment)
- [Usage](#usage)
- [Requirements](#requirements)
- [Installation](#installation)
- [Running the Project](#running-the-project)
- [Contributing](#contributing)
- [License](#license)

## Introduction

Bitcoin fraud detection is crucial for maintaining the integrity and security of Bitcoin transactions. This project uses machine learning techniques to identify potentially fraudulent transactions based on historical data.

## Features

- Data preprocessing and feature selection using Lasso regression.
- Training multiple machine learning models:
  - K-Nearest Neighbors (KNN)
  - Decision Tree
  - Random Forest
  - XGBoost
  - Deep Learning (Neural Network)
- Comparing model performance and selecting the best model.
- Deploying the best model using Streamlit for real-time fraud detection.

## Data Preparation

1. **Loading Data**: The transaction data is loaded from a CSV file.
2. **Feature Selection**: Lasso regression is used to select the most significant features for model training.
3. **Scaling**: Features are scaled to ensure that all models perform optimally.
4. **Splitting**: Data is split into training and testing sets.

## Model Training

1. **Training**: Various machine learning models are trained on the prepared dataset.
2. **Evaluation**: Models are evaluated based on accuracy.
3. **Selection**: The model with the highest accuracy is selected for deployment.

## Model Deployment

A Streamlit application is developed to allow users to interact with the trained model. Users can upload transaction datasets and get predictions on whether transactions are fraudulent or not.

## Usage

### Streamlit Application

The Streamlit app allows users to:

- Upload a CSV file containing transaction data.
- Get real-time predictions on whether transactions are fraudulent.
- Download the dataset with predictions.

## Requirements

- Python 3.8 or higher
- Apache Airflow
- pandas
- scikit-learn
- xgboost
- tensorflow
- scikeras
- joblib
- streamlit

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/bitcoin-fraud-detection.git
cd bitcoin-fraud-detection
```

2. Create a virtual environment and activate it:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```

3. Install the required packages:
```bash
pip install -r requirements.txt
```

4. Set up Apache Airflow:
```bash
airflow db init
```

5. Add your DAG file to the Airflow dags directory:
```bash
cp bitcoin_fraud.py $AIRFLOW_HOME/dags/
```

## Running the Project

1. Start the Airflow web server and scheduler:
```bash
airflow webserver --port 8080
airflow scheduler
```

2. Access the Airflow web interface at http://localhost:8080 to trigger the DAG and train the models.

3. streamlit run app.py
```bash
streamlit run app.py
```

## Contributing

Contributions are welcome! Please submit a pull request or open an issue to discuss any changes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.