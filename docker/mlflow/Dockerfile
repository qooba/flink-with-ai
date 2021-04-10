FROM continuumio/miniconda3
RUN pip install mlflow scikit-learn watchdog[watchmedo]

CMD mlflow server -h 0.0.0.0 --backend-store-uri sqlite:///mlflow/mlflow.db --default-artifact-root /mlflow/mlruns


