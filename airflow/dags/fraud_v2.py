# import logging
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# import pandas as pd
# from sklearn.linear_model import Lasso, LassoCV
# from sklearn.preprocessing import StandardScaler
# from sklearn.model_selection import train_test_split
# from sklearn.neighbors import KNeighborsClassifier
# from sklearn.tree import DecisionTreeClassifier
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.metrics import accuracy_score
# import xgboost as xgb
# import tensorflow as tf
# from tensorflow.keras.models import Sequential
# from tensorflow.keras.layers import Dense
# from scikeras.wrappers import KerasClassifier
# import joblib
# import pickle
# import base64
# import os

# # Initialize logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
# }

# dag = DAG(
#     'fraud_bitcoin',
#     default_args=default_args,
#     description='ML Project',
#     schedule_interval='@daily',
#     start_date=days_ago(1),
#     tags=['fraud_detection'],
# )

# def load_and_prepare_data(**kwargs):
#     file_path = kwargs['file_path']
#     data = pd.read_csv(file_path)

#     logger.info("Data loaded successfully")

#     # Data Preparation
#     X = data.drop(columns=['Unnamed: 0', 'txId', 'class'])
#     y = data['class']

#     # Standardizing the features
#     scaler = StandardScaler()
#     X_scaled = scaler.fit_transform(X)

#     # Splitting the data into training and test sets
#     X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.3, random_state=42)

#     # LassoCV for dynamic alpha value selection
#     lasso_cv = LassoCV(cv=5, random_state=42).fit(X_train, y_train)

#     # Best alpha value
#     best_alpha = lasso_cv.alpha_
#     logger.info(f"Best alpha value: {best_alpha}")

#     # Applying Lasso with the best alpha value
#     lasso = Lasso(alpha=best_alpha)
#     lasso.fit(X_train, y_train)

#     # Identifying non-zero coefficients
#     lasso_coefficients = lasso.coef_
#     significant_features = X.columns[lasso_coefficients != 0]

#     # Displaying the significant features
#     logger.info(significant_features.tolist())

#     # Showing the number of selected features from the total features
#     total_features = X.shape[1]
#     selected_features = len(significant_features)

#     logger.info(f"Number of features selected: {selected_features} out of {total_features}")

#     kwargs['ti'].xcom_push(key='significant_features', value=significant_features.tolist())

# def train_knn(**kwargs):
#     ti = kwargs['ti']
#     significant_features = ti.xcom_pull(key='significant_features', task_ids='load_and_prepare_data')

#     file_path = kwargs['file_path']
#     data = pd.read_csv(file_path)

#     # Filter the data for class 1 and 2
#     data = data[data['class'].isin([1, 2])]

#     # Data Preparation
#     X = data.drop(columns=['Unnamed: 0', 'txId', 'class'])
#     y = data['class']

#     # Standardizing the features
#     scaler = StandardScaler()
#     X_scaled = scaler.fit_transform(X)

#     # Splitting the data into training and test sets
#     X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.3, random_state=42)

#     if not significant_features or not significant_features:
#         logger.error("No significant features available for training.")
#         return

#     # Select only the significant features
#     X_significant_train = pd.DataFrame(X_train, columns=X.columns)[significant_features]
#     X_significant_test = pd.DataFrame(X_test, columns=X.columns)[significant_features]

#     # Verify that the significant features match the columns in the DataFrame
#     logger.info(f"Columns in X_significant_train: {X_significant_train.columns.tolist()}")
#     logger.info(f"Significant features: {significant_features}")

#     # Train a KNN model using the significant features
#     knn_model = KNeighborsClassifier()
#     knn_model.fit(X_significant_train, y_train)

#     # Make predictions
#     y_pred = knn_model.predict(X_significant_test)

#     # Evaluate the model
#     knn_accuracy = accuracy_score(y_test, y_pred)

#     # Serialize the model
#     knn_model = pickle.dumps(knn_model) 
#     encoded_knn = base64.b64encode(knn_model).decode('utf-8')

#     ti.xcom_push(key='knn_accuracy', value=knn_accuracy)
#     ti.xcom_push(key='knn_model', value=encoded_knn)

#     logger.info(f"KNN model trained with accuracy: {knn_accuracy}")

# def train_decision_tree(**kwargs):
#     ti = kwargs['ti']
#     significant_features = ti.xcom_pull(key='significant_features', task_ids='load_and_prepare_data')

#     file_path = kwargs['file_path']
#     data = pd.read_csv(file_path)

#     # Filter the data for class 1 and 2
#     data = data[data['class'].isin([1, 2])]

#     # Data Preparation
#     X = data.drop(columns=['Unnamed: 0', 'txId', 'class'])
#     y = data['class']

#     # Standardizing the features
#     scaler = StandardScaler()
#     X_scaled = scaler.fit_transform(X)

#     # Splitting the data into training and test sets
#     X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.3, random_state=42)

#     # Select only the significant features
#     X_significant_train = pd.DataFrame(X_train, columns=X.columns)[significant_features]
#     X_significant_test = pd.DataFrame(X_test, columns=X.columns)[significant_features]

#     # Verify that the significant features match the columns in the DataFrame
#     logger.info(f"Columns in X_significant_train: {X_significant_train.columns.tolist()}")
#     logger.info(f"Significant features: {significant_features}")

#     # Train a Decision Tree model using the significant features
#     dt_model = DecisionTreeClassifier(random_state=42)
#     dt_model.fit(X_significant_train, y_train)

#     # Make predictions
#     y_pred = dt_model.predict(X_significant_test)

#     # Evaluate the model
#     dt_accuracy = accuracy_score(y_test, y_pred)

#     # Serialize the model
#     dt_model = pickle.dumps(dt_model) 
#     encoded_dt = base64.b64encode(dt_model).decode('utf-8')

#     ti.xcom_push(key='dt_accuracy', value=dt_accuracy)
#     ti.xcom_push(key='dt_model', value=encoded_dt)

#     logger.info(f"Decision Tree model trained with accuracy: {dt_accuracy}")

# def train_random_forest(**kwargs):
#     ti = kwargs['ti']
#     significant_features = ti.xcom_pull(key='significant_features', task_ids='load_and_prepare_data')

#     file_path = kwargs['file_path']
#     data = pd.read_csv(file_path)

#     # Filter the data for class 1 and 2
#     data = data[data['class'].isin([1, 2])]

#     # Data Preparation
#     X = data.drop(columns=['Unnamed: 0', 'txId', 'class'])
#     y = data['class']

#     # Standardizing the features
#     scaler = StandardScaler()
#     X_scaled = scaler.fit_transform(X)

#     # Splitting the data into training and test sets
#     X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.3, random_state=42)

#     # Select only the selected features
#     X_selected_train = pd.DataFrame(X_train, columns=X.columns)[significant_features]
#     X_selected_test = pd.DataFrame(X_test, columns=X.columns)[significant_features]

#     # Train a Random Forest model using the selected features
#     rf_model = RandomForestClassifier(random_state=42)
#     rf_model.fit(X_selected_train, y_train)

#     # Make predictions
#     y_pred = rf_model.predict(X_selected_test)

#     # Evaluate the model
#     rf_accuracy = accuracy_score(y_test, y_pred)

#     # Serialize the model
#     rf_model = pickle.dumps(rf_model) 
#     encoded_rf = base64.b64encode(rf_model).decode('utf-8')

#     ti.xcom_push(key='rf_accuracy', value=rf_accuracy)
#     ti.xcom_push(key='rf_model', value=encoded_rf)

#     logger.info(f"Random Forest model trained with accuracy: {rf_accuracy}")

# def train_xgboost(**kwargs):
#     ti = kwargs['ti']
#     significant_features = ti.xcom_pull(key='significant_features', task_ids='load_and_prepare_data')

#     file_path = kwargs['file_path']
#     data = pd.read_csv(file_path)

#     # Filter the data for class 1 and 2
#     data = data[data['class'].isin([1, 2])]

#     # Adjust class labels to be 0 and 1 instead of 1 and 2
#     data['class'] = data['class'] - 1

#     # Data Preparation
#     X = data.drop(columns=['Unnamed: 0', 'txId', 'class'])
#     y = data['class']

#     # Standardizing the features
#     scaler = StandardScaler()
#     X_scaled = scaler.fit_transform(X)

#     # Splitting the data into training and test sets
#     X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.8, random_state=50000)

#     # Ensure this variable is defined from the previous step
#     # significant_features = <list_of_significant_features_from_previous_step>

#     # Select only the significant features
#     X_significant_train = pd.DataFrame(X_train, columns=X.columns)[significant_features]
#     X_significant_test = pd.DataFrame(X_test, columns=X.columns)[significant_features]

#     # Verify that the significant features match the columns in the DataFrame
#     print(f"Columns in X_significant_train: {X_significant_train.columns.tolist()}")
#     print(f"Significant features: {significant_features}")

#     # Train an XGBoost model using the significant features
#     xgb_model = xgb.XGBClassifier(random_state=42)
#     xgb_model.fit(X_significant_train, y_train)

#     # Make predictions
#     y_pred = xgb_model.predict(X_significant_test)

#     # Evaluate the model
#     xgboost_accuracy = accuracy_score(y_test, y_pred)

#     # Serialize the model
#     xgb_model = pickle.dumps(xgb_model) 
#     encoded_xgb = base64.b64encode(xgb_model).decode('utf-8')

#     ti.xcom_push(key='xgboost_accuracy', value=xgboost_accuracy)
#     ti.xcom_push(key='xgb_model', value=encoded_xgb)

#     logger.info(f"XGBoost model trained with accuracy: {xgboost_accuracy}")

# def create_dl_model(input_dim):
#     model = Sequential()
#     model.add(Dense(64, input_dim=input_dim, activation='relu'))
#     model.add(Dense(32, activation='relu'))
#     model.add(Dense(2, activation='softmax'))
#     model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
#     return model

# def train_deep_learning(**kwargs):
#     ti = kwargs['ti']
#     significant_features = ti.xcom_pull(key='significant_features', task_ids='load_and_prepare_data')

#     file_path = kwargs['file_path']
#     data = pd.read_csv(file_path)

#     # Filter the data for class 1 and 2
#     data = data[data['class'].isin([1, 2])]

#     # Data Preparation
#     X = data.drop(columns=['Unnamed: 0', 'txId', 'class'])
#     y = data['class']

#     # Standardizing the features
#     scaler = StandardScaler()
#     X_scaled = scaler.fit_transform(X)

#     # Splitting the data into training and test sets
#     X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.3, random_state=42)

#     # Ensure this variable is defined from the previous step
#     # significant_features = <list_of_significant_features_from_previous_step>

#     # Select only the significant features
#     X_significant_train = pd.DataFrame(X_train, columns=X.columns)[significant_features]
#     X_significant_test = pd.DataFrame(X_test, columns=X.columns)[significant_features]

#     # Verify that the significant features match the columns in the DataFrame
#     logger.info(f"Columns in X_significant_train: {X_significant_train.columns.tolist()}")
#     logger.info(f"Significant features: {significant_features}")

#     # Convert target variable to categorical (one-hot encoding)
#     y_train_categorical = tf.keras.utils.to_categorical(y_train - 1, num_classes=2)
#     y_test_categorical = tf.keras.utils.to_categorical(y_test - 1, num_classes=2)

#     dl_model = KerasClassifier(model=create_dl_model, input_dim=len(significant_features), epochs=50, batch_size=10, verbose=0)
#     dl_model.fit(X_significant_train, y_train_categorical)

#     y_pred_prob = dl_model.predict(X_significant_test)
#     y_pred = (y_pred_prob > 0.5).astype(int) + 1  # Adjusting the predicted class values
#     accuracy = accuracy_score(y_test, y_pred)

#     ti.xcom_push(key='dl_accuracy', value=accuracy)
#     # dl_model.model.save('saved_models/dl_model.h5')

#     # Serialize the model
#     dl_model = pickle.dumps(dl_model) 
#     encoded_dl = base64.b64encode(dl_model).decode('utf-8')

#     ti.xcom_push(key='xgboost_accuracy', value=xgboost_accuracy)
#     ti.xcom_push(key='xgb_model', value=encoded_dl)

#     logger.info(f"Deep Learning model trained with accuracy: {accuracy}")

# def compare_and_save_best_model(**kwargs):
#     ti = kwargs['ti']
#     accuracies = {
#         "KNN": ti.xcom_pull(key='knn_accuracy', task_ids='train_knn'),
#         "Decision Tree": ti.xcom_pull(key='dt_accuracy', task_ids='train_decision_tree'),
#         "Random Forest": ti.xcom_pull(key='rf_accuracy', task_ids='train_random_forest'),
#         "XGBoost": ti.xcom_pull(key='xgboost_accuracy', task_ids='train_xgboost'),
#         "Deep Learning": ti.xcom_pull(key='dl_accuracy', task_ids='train_deep_learning')
#     }

#     best_model_name = max(accuracies, key=accuracies.get)
#     best_accuracy = accuracies[best_model_name]

#     logger.info(f"The best model is: {best_model_name} with an accuracy of {best_accuracy}")

#     os.makedirs('saved_models', exist_ok=True)

#     if best_model_name == "KNN":
#         knn_model = ti.xcom_pull(key='knn_model', task_ids='train_knn')
#         joblib.dump(knn_model, 'saved_models/best_model.pkl')
#     elif best_model_name == "Decision Tree":
#         dt_model = ti.xcom_pull(key='dt_model', task_ids='train_decision_tree')
#         joblib.dump(dt_model, 'saved_models/best_model.pkl')
#     elif best_model_name == "Random Forest":
#         rf_model = ti.xcom_pull(key='rf_model', task_ids='train_random_forest')
#         joblib.dump(rf_model, 'saved_models/best_model.pkl')
#     elif best_model_name == "XGBoost":
#         xgb_model = ti.xcom_pull(key='xgb_model', task_ids='train_xgboost')
#         joblib.dump(xgb_model, 'saved_models/best_model.pkl')
#     elif best_model_name == "Deep Learning":
#         pass  # Model already saved in train_deep_learning

#     logger.info("Best model saved")

# with dag:
#     load_data_task = PythonOperator(
#         task_id='load_and_prepare_data',
#         python_callable=load_and_prepare_data,
#         # op_kwargs={'file_path': '/opt/airflow/dags/datasets/usable_datasets/out.csv'}
#         op_kwargs={'file_path': '/opt/airflow/dags/datasets/usable_datasets/training_data_5000.csv'}
#     )

#     train_knn_task = PythonOperator(
#         task_id='train_knn',
#         python_callable=train_knn,
#         provide_context=True,
#         op_kwargs={'file_path': '/opt/airflow/dags/datasets/usable_datasets/training_data_5000.csv'}
#     )

#     train_decision_tree_task = PythonOperator(
#         task_id='train_decision_tree',
#         python_callable=train_decision_tree,
#         provide_context=True,
#         op_kwargs={'file_path': '/opt/airflow/dags/datasets/usable_datasets/training_data_5000.csv'}
#     )

#     train_random_forest_task = PythonOperator(
#         task_id='train_random_forest',
#         python_callable=train_random_forest,
#         provide_context=True,
#         op_kwargs={'file_path': '/opt/airflow/dags/datasets/usable_datasets/training_data_5000.csv'}
#     )

#     train_xgboost_task = PythonOperator(
#         task_id='train_xgboost',
#         python_callable=train_xgboost,
#         provide_context=True,
#         op_kwargs={'file_path': '/opt/airflow/dags/datasets/usable_datasets/training_data_5000.csv'}
#     )

#     train_deep_learning_task = PythonOperator(
#         task_id='train_deep_learning',
#         python_callable=train_deep_learning,
#         provide_context=True,
#         op_kwargs={'file_path': '/opt/airflow/dags/datasets/usable_datasets/training_data_5000.csv'}
#     )

#     compare_and_save_task = PythonOperator(
#         task_id='compare_and_save_best_model',
#         python_callable=compare_and_save_best_model,
#         provide_context=True
#     )

#     load_data_task >> [train_knn_task, train_decision_tree_task, train_random_forest_task, train_xgboost_task, train_deep_learning_task] >> compare_and_save_task
