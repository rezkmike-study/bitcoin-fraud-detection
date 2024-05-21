import streamlit as st
import pandas as pd
import joblib
import tensorflow as tf
from tensorflow.keras.models import load_model
import matplotlib.pyplot as plt

# Function to load the best model
def load_best_model():
    # Load your model based on the saved format
    # For example, if the best model was a RandomForest model saved using joblib:
    model = joblib.load('./saved_model/best_model.pkl')
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
    # required_columns = ["feature1", "feature2", "feature3", "feature4"]  # Update with your actual feature names
    required_columns = [
        "Time step", 
        "Local_feature_1", 
        "Local_feature_2", 
        "Local_feature_3", 
        "Local_feature_6", 
        "Local_feature_7", 
        "Local_feature_10", 
        "Local_feature_12", 
        "Local_feature_13", 
        "Local_feature_14", 
        "Local_feature_15", 
        "Local_feature_16", 
        "Local_feature_19", 
        "Local_feature_20", 
        "Local_feature_21", 
        "Local_feature_22", 
        "Local_feature_23", 
        "Local_feature_25", 
        "Local_feature_28", 
        "Local_feature_29", 
        "Local_feature_31", 
        "Local_feature_32", 
        "Local_feature_34", 
        "Local_feature_35", 
        "Local_feature_37", 
        "Local_feature_39", 
        "Local_feature_42", 
        "Local_feature_44", 
        "Local_feature_45", 
        "Local_feature_46", 
        "Local_feature_47", 
        "Local_feature_48", 
        "Local_feature_50", 
        "Local_feature_51", 
        "Local_feature_53", 
        "Local_feature_54", 
        "Local_feature_55", 
        "Local_feature_56", 
        "Local_feature_57", 
        "Local_feature_59", 
        "Local_feature_60", 
        "Local_feature_61", 
        "Local_feature_63", 
        "Local_feature_64", 
        "Local_feature_66", 
        "Local_feature_67", 
        "Local_feature_68", 
        "Local_feature_70", 
        "Local_feature_75", 
        "Local_feature_76", 
        "Local_feature_77", 
        "Local_feature_81", 
        "Local_feature_82", 
        "Local_feature_83", 
        "Local_feature_85", 
        "Local_feature_87", 
        "Local_feature_89", 
        "Local_feature_90", 
        "Local_feature_91", 
        "Local_feature_92", 
        "Local_feature_93", 
        "Aggregate_feature_1", 
        "Aggregate_feature_3", 
        "Aggregate_feature_5", 
        "Aggregate_feature_6", 
        "Aggregate_feature_8", 
        "Aggregate_feature_9", 
        "Aggregate_feature_10", 
        "Aggregate_feature_11", 
        "Aggregate_feature_12", 
        "Aggregate_feature_13", 
        "Aggregate_feature_14", 
        "Aggregate_feature_15", 
        "Aggregate_feature_18", 
        "Aggregate_feature_19", 
        "Aggregate_feature_20", 
        "Aggregate_feature_21", 
        "Aggregate_feature_22", 
        "Aggregate_feature_24", 
        "Aggregate_feature_25", 
        "Aggregate_feature_26", 
        "Aggregate_feature_27", 
        "Aggregate_feature_28", 
        "Aggregate_feature_30", 
        "Aggregate_feature_31", 
        "Aggregate_feature_32", 
        "Aggregate_feature_33", 
        "Aggregate_feature_34", 
        "Aggregate_feature_35", 
        "Aggregate_feature_37", 
        "Aggregate_feature_38", 
        "Aggregate_feature_39", 
        "Aggregate_feature_41", 
        "Aggregate_feature_44", 
        "Aggregate_feature_45", 
        "Aggregate_feature_46", 
        "Aggregate_feature_47", 
        "Aggregate_feature_48", 
        "Aggregate_feature_49", 
        "Aggregate_feature_50", 
        "Aggregate_feature_52", 
        "Aggregate_feature_55", 
        "Aggregate_feature_56", 
        "Aggregate_feature_57", 
        "Aggregate_feature_60", 
        "Aggregate_feature_61", 
        "Aggregate_feature_62", 
        "Aggregate_feature_63", 
        "Aggregate_feature_64", 
        "Aggregate_feature_66", 
        "Aggregate_feature_67", 
        "Aggregate_feature_68", 
        "Aggregate_feature_69", 
        "Aggregate_feature_70", 
        "Aggregate_feature_71"
    ]
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
