import streamlit as st
import pandas as pd
import joblib
import tensorflow as tf
from tensorflow.keras.models import load_model
import matplotlib.pyplot as plt
import os

# Function to load the best model
def load_best_model():
    if "best_model.h5" in os.listdir('saved_models'):
        model = load_model('saved_models/best_model.h5')
    else:
        model = joblib.load('saved_models/best_model.pkl')
    return model

# Function to make predictions
def make_prediction(model, input_data, model_type):
    if model_type == "Deep Learning":
        input_df = pd.DataFrame(input_data)
        predictions = model.predict(input_df)
        predictions = (predictions > 0.5).astype(int) + 1  # Adjusting the predicted class values
    else:
        predictions = model.predict(input_data)
    return predictions

# Load the best model
model = load_best_model()

# Streamlit app
st.title('Bitcoin Fraud Detection App')
st.write('Upload a dataset to check if the transactions are fraudulent (class 3).')

# File uploader
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    # Read the uploaded CSV file
    data = pd.read_csv(uploaded_file)
    st.write("Dataset Preview:")
    st.write(data.head())

    # Filter data where class is 3
    data = data[data['class'] == 3]

    # Ensure the dataset contains the necessary features
    required_columns = model.input_names if hasattr(model, 'input_names') else model.feature_names_in_
    if all(column in data.columns for column in required_columns):
        # Extract the relevant features
        input_data = data[required_columns]

        # Make predictions
        model_type = "Deep Learning" if isinstance(model, tf.keras.Model) else "Other"
        predictions = make_prediction(model, input_data, model_type)

        # Add the predictions to the dataset
        data['fraud_prediction'] = predictions

        # Display the dataset with predictions
        st.write("Predictions:")
        st.write(data)

        # Provide an option to download the dataset with predictions
        @st.cache_data
        def convert_df(df):
            # IMPORTANT: Cache the conversion to prevent computation on every rerun
            return df.to_csv(index=False).encode('utf-8')

        csv = convert_df(data)
        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name='predictions.csv',
            mime='text/csv',
        )

        # Plot the results
        fig, ax = plt.subplots()
        prediction_counts = data['fraud_prediction'].value_counts().sort_index()
        prediction_counts.index = ['Licit' if x == 1 else 'Illicit' for x in prediction_counts.index]
        prediction_counts.plot(kind='bar', ax=ax, color=['green', 'red'])
        ax.set_title('Fraud Prediction Results')
        ax.set_xlabel('Prediction')
        ax.set_ylabel('Count')
        st.pyplot(fig)

    else:
        st.error(f"The uploaded dataset does not contain the required columns: {required_columns}")
