import json
import boto3
import base64

def lambda_handler(event, context):
    """Image classification using SageMaker endpoint"""
    
    # Get the SageMaker runtime client
    runtime = boto3.client('sagemaker-runtime')
    
    # Endpoint name
    ENDPOINT = "image-classification-2025-08-21-23-57-05-598"
    
    # Decode the image data from the event
    if isinstance(event['body']['image_data'], str):
        # If it's a string, decode from base64
        image = base64.b64decode(event['body']['image_data'])
    else:
        # If it's already bytes, decode first
        image_b64 = event['body']['image_data'].decode('utf-8')
        image = base64.b64decode(image_b64)
    
    # Make a prediction using SageMaker runtime
    response = runtime.invoke_endpoint(
        EndpointName=ENDPOINT,
        ContentType='image/png',
        Body=image
    )
    
    # Get the inference results
    result = response['Body'].read()
    inferences = result.decode('utf-8')
    
    # Update the event with inferences
    event["inferences"] = inferences
    
    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }