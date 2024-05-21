import streamlit as st
import pandas as pd
import joblib
import tensorflow as tf
from tensorflow.keras.models import load_model

# Function to load the best model
def load_best_model():
    # Load your model based on the saved format
    # For example, if the best model was a RandomForest model saved using joblib:
    model = joblib.load('./saved_models/best_model.pkl')
    return model

# Function to make predictions
def make_prediction(model, input_data):
    # Ensure the input data is in the correct format for prediction
    prediction = model.predict(input_data)
    return prediction

# Load the best model
model = load_best_model()

# Streamlit app
st.title('Fraud Detection App')
st.write('Upload a dataset to check if the transactions are fraudulent.')

# File uploader
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    # Read the uploaded CSV file
    data = pd.read_csv(uploaded_file)
    st.write("Dataset Preview:")
    st.write(data.head())

    # Ensure the dataset contains the necessary features
    # Adjust these columns based on your significant features
    required_columns = ["feature1", "feature2", "feature3", "feature4"]  # Update with your actual feature names
    if all(column in data.columns for column in required_columns):
        # Extract the relevant features
        input_data = data[required_columns]

        # Make predictions
        predictions = make_prediction(model, input_data)

        # Add the predictions to the dataset
        data['fraud_prediction'] = predictions

        # Display the dataset with predictions
        st.write("Predictions:")
        st.write(data)

        # Provide an option to download the dataset with predictions
        @st.cache
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
    else:
        st.error(f"The uploaded dataset does not contain the required columns: {required_columns}")
