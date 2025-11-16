from google.cloud import aiplatform
import argparse
import logging

logging.basicConfig(level=logging.INFO)

labels = {
    "model_type": "xgboost",
    "task": "fraud_detection",
    "framework": "sklearn"
}

location="us-central1"
serving_container_image="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest"
description="XGBoost fraud detection model"
display_name="fraud-detection-xgboost"
project_id = 'int3319-477808'

artifact_uri = "1"

def upload_model_registry(artifact_uri):
    # Initialize Vertex AI
    logging.info(f"Initializing Vertex AI with project: {project_id}, location: {location}")
    aiplatform.init(project=project_id, location=location)

    # Check if model already exists
    existing_models = aiplatform.Model.list(
        filter=f'display_name="{display_name}"',
        order_by="create_time desc"
    )

    if existing_models:
        parent_model = existing_models[0]
        logging.info(f"Current version: v{parent_model.version_id}")
        logging.info(f"Uploading new version from: {artifact_uri}")
        
        model = aiplatform.Model.upload(
            display_name=display_name,
            description=f"{description} - Version {int(parent_model.version_id) + 1}",
            artifact_uri=artifact_uri,
            serving_container_image_uri=serving_container_image,
            labels=labels,
            parent_model=parent_model.resource_name
        )
        
        logging.info(f"✓ New model version registered successfully!")
    else:
        logging.info(f"Uploading model from: {artifact_uri}")
        logging.info(f"Display name: {display_name}")

        model = aiplatform.Model.upload(
            display_name=display_name,
            description=description,
            artifact_uri=artifact_uri,
            serving_container_image_uri=serving_container_image,
            labels=labels
        )

        logging.info(f"✓ Model registered successfully!")
