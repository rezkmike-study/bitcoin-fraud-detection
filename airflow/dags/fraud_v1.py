# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# import requests
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
# import os

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

# def download_file_from_google_drive(file_id, destination):
#     URL = "https://drive.google.com/uc?export=download"

#     session = requests.Session()

#     response = session.get(URL, params={'id': file_id}, stream=True)
#     token = get_confirm_token(response)

#     if token:
#         params = {'id': file_id, 'confirm': token}
#         response = session.get(URL, params=params, stream=True)

#     save_response_content(response, destination)

# def get_confirm_token(response):
#     for key, value in response.cookies.items():
#         if key.startswith('download_warning'):
#             return value
#     return None

# def save_response_content(response, destination):
#     CHUNK_SIZE = 32768

#     with open(destination, "wb") as f:
#         for chunk in response.iter_content(CHUNK_SIZE):
#             if chunk:  # filter out keep-alive new chunks
#                 f.write(chunk)

# def load_and_prepare_data(**kwargs):
#     file_path = kwargs['file_path']
#     data = pd.read_csv(file_path)

#     data = data[data['class'].isin([1, 2])]
#     X = data.drop(columns=['Unnamed: 0', 'txId', 'class'])
#     y = data['class']

#     scaler = StandardScaler()
#     X_scaled = scaler.fit_transform(X)

#     X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.3, random_state=42)

#     lasso_cv = LassoCV(cv=5, random_state=42).fit(X_train, y_train)
#     best_alpha = lasso_cv.alpha_

#     lasso = Lasso(alpha=best_alpha)
#     lasso.fit(X_train, y_train)

#     lasso_coefficients = lasso.coef_
#     significant_features = X.columns[lasso_coefficients != 0].tolist()  # Convert to list

#     X_significant_train = pd.DataFrame(X_train, columns=X.columns)[significant_features].values.tolist()  # Convert to list of lists
#     X_significant_test = pd.DataFrame(X_test, columns=X.columns)[significant_features].values.tolist()  # Convert to list of lists

#     y_train = y_train.tolist()  # Convert to list
#     y_test = y_test.tolist()  # Convert to list

#     kwargs['ti'].xcom_push(key='X_significant_train', value=X_significant_train)
#     kwargs['ti'].xcom_push(key='X_significant_test', value=X_significant_test)
#     kwargs['ti'].xcom_push(key='y_train', value=y_train)
#     kwargs['ti'].xcom_push(key='y_test', value=y_test)
#     kwargs['ti'].xcom_push(key='significant_features', value=significant_features)

# def train_knn(**kwargs):
#     ti = kwargs['ti']
#     X_significant_train = ti.xcom_pull(key='X_significant_train', task_ids='load_and_prepare_data')
#     X_significant_test = ti.xcom_pull(key='X_significant_test', task_ids='load_and_prepare_data')
#     y_train = ti.xcom_pull(key='y_train', task_ids='load_and_prepare_data')
#     y_test = ti.xcom_pull(key='y_test', task_ids='load_and_prepare_data')

#     knn_model = KNeighborsClassifier()
#     knn_model.fit(X_significant_train, y_train)
#     y_pred = knn_model.predict(X_significant_test)
#     accuracy = accuracy_score(y_test, y_pred)

#     ti.xcom_push(key='knn_accuracy', value=accuracy)
#     ti.xcom_push(key='knn_model', value=knn_model)

# def train_decision_tree(**kwargs):
#     ti = kwargs['ti']
#     X_significant_train = ti.xcom_pull(key='X_significant_train', task_ids='load_and_prepare_data')
#     X_significant_test = ti.xcom_pull(key='X_significant_test', task_ids='load_and_prepare_data')
#     y_train = ti.xcom_pull(key='y_train', task_ids='load_and_prepare_data')
#     y_test = ti.xcom_pull(key='y_test', task_ids='load_and_prepare_data')

#     dt_model = DecisionTreeClassifier(random_state=42)
#     dt_model.fit(X_significant_train, y_train)
#     y_pred = dt_model.predict(X_significant_test)
#     accuracy = accuracy_score(y_test, y_pred)

#     ti.xcom_push(key='dt_accuracy', value=accuracy)
#     ti.xcom_push(key='dt_model', value=dt_model)

# def train_random_forest(**kwargs):
#     ti = kwargs['ti']
#     X_significant_train = ti.xcom_pull(key='X_significant_train', task_ids='load_and_prepare_data')
#     X_significant_test = ti.xcom_pull(key='X_significant_test', task_ids='load_and_prepare_data')
#     y_train = ti.xcom_pull(key='y_train', task_ids='load_and_prepare_data')
#     y_test = ti.xcom_pull(key='y_test', task_ids='load_and_prepare_data')

#     rf_model = RandomForestClassifier(random_state=42)
#     rf_model.fit(X_significant_train, y_train)
#     y_pred = rf_model.predict(X_significant_test)
#     accuracy = accuracy_score(y_test, y_pred)

#     ti.xcom_push(key='rf_accuracy', value=accuracy)
#     ti.xcom_push(key='rf_model', value=rf_model)

# def train_xgboost(**kwargs):
#     ti = kwargs['ti']
#     X_significant_train = ti.xcom_pull(key='X_significant_train', task_ids='load_and_prepare_data')
#     X_significant_test = ti.xcom_pull(key='X_significant_test', task_ids='load_and_prepare_data')
#     y_train = ti.xcom_pull(key='y_train', task_ids='load_and_prepare_data')
#     y_test = ti.xcom_pull(key='y_test', task_ids='load_and_prepare_data')

#     xgb_model = xgb.XGBClassifier(random_state=42)
#     xgb_model.fit(X_significant_train, y_train)
#     y_pred = xgb_model.predict(X_significant_test)
#     accuracy = accuracy_score(y_test, y_pred)

#     ti.xcom_push(key='xgboost_accuracy', value=accuracy)
#     ti.xcom_push(key='xgb_model', value=xgb_model)

# def create_dl_model(input_dim):
#     model = Sequential()
#     model.add(Dense(64, input_dim=input_dim, activation='relu'))
#     model.add(Dense(32, activation='relu'))
#     model.add(Dense(2, activation='softmax'))
#     model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
#     return model

# def train_deep_learning(**kwargs):
#     ti = kwargs['ti']
#     X_significant_train = ti.xcom_pull(key='X_significant_train', task_ids='load_and_prepare_data')
#     X_significant_test = ti.xcom_pull(key='X_significant_test', task_ids='load_and_prepare_data')
#     y_train = ti.xcom_pull(key='y_train', task_ids='load_and_prepare_data')
#     y_test = ti.xcom_pull(key='y_test', task_ids='load_and_prepare_data')
#     significant_features = ti.xcom_pull(key='significant_features', task_ids='load_and_prepare_data')

#     y_train_categorical = tf.keras.utils.to_categorical([label - 1 for label in y_train], num_classes=2)
#     y_test_categorical = tf.keras.utils.to_categorical([label - 1 for label in y_test], num_classes=2)

#     dl_model = KerasClassifier(model=create_dl_model, input_dim=len(significant_features), epochs=50, batch_size=10, verbose=0)
#     dl_model.fit(X_significant_train, y_train_categorical)

#     y_pred_prob = dl_model.predict(X_significant_test)
#     y_pred = (y_pred_prob > 0.5).astype(int) + 1  # Adjusting the predicted class values
#     accuracy = accuracy_score(y_test, y_pred)

#     ti.xcom_push(key='dl_accuracy', value=accuracy)
#     dl_model.model.save('saved_models/dl_model.h5')

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

#     print(f"The best model is: {best_model_name} with an accuracy of {best_accuracy}")

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

# with dag:
#     download_data_task = PythonOperator(
#         task_id='download_dataset',
#         python_callable=download_file_from_google_drive,
#         op_kwargs={
#             'file_id': 'YOUR_FILE_ID',
#             'destination': '/opt/airflow/dags/datasets/usable_datasets/out.csv'
#         }
#     )

#     load_data_task = PythonOperator(
#         task_id='load_and_prepare_data',
#         python_callable=load_and_prepare_data,
#         op_kwargs={'file_path': '/opt/airflow/dags/datasets/usable_datasets/out.csv'}
#     )

#     train_knn_task = PythonOperator(
#         task_id='train_knn',
#         python_callable=train_knn,
#         provide_context=True
#     )

#     train_decision_tree_task = PythonOperator(
#         task_id='train_decision_tree',
#         python_callable=train_decision_tree,
#         provide_context=True
#     )

#     train_random_forest_task = PythonOperator(
#         task_id='train_random_forest',
#         python_callable=train_random_forest,
#         provide_context=True
#     )

#     train_xgboost_task = PythonOperator(
#         task_id='train_xgboost',
#         python_callable=train_xgboost,
#         provide_context=True
#     )

#     train_deep_learning_task = PythonOperator(
#         task_id='train_deep_learning',
#         python_callable=train_deep_learning,
#         provide_context=True
#     )

#     compare_and_save_task = PythonOperator(
#         task_id='compare_and_save_best_model',
#         python_callable=compare_and_save_best_model,
#         provide_context=True
#     )

#     download_data_task >> load_data_task >> [train_knn_task, train_decision_tree_task, train_random_forest_task, train_xgboost_task, train_deep_learning_task] >> compare_and_save_task
