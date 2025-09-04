# Lambda Functions for Scones Unlimited Image Classification Workflow

# =============================================================================
# Lambda Function 1: Serialize Image Data
# =============================================================================
"""
Function Name: SerializeImageData
Runtime: Python 3.8
Description: Downloads image from S3 and base64 encodes it
Handler: lambda_function.lambda_handler
"""

import json
import boto3
import base64

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """A function to serialize target data from S3"""
    
    # Get the s3 address from the Step Function event input
    key = event['s3_key']
    bucket = event['s3_bucket']
    
    # Download the data from s3 to /tmp/image.png
    s3.download_file(bucket, key, '/tmp/image.png')
    
    # We read the data from a file
    with open("/tmp/image.png", "rb") as f:
        image_data = base64.b64encode(f.read())

    # Pass the data back to the Step Function
    print("Event:", event.keys())
    return {
        'statusCode': 200,
        'body': {
            "image_data": image_data,
            "s3_bucket": bucket,
            "s3_key": key,
            "inferences": []
        }
    }


# =============================================================================
# Lambda Function 2: Image Classification
# =============================================================================
"""
Function Name: ImageClassification
Runtime: Python 3.8
Description: Uses SageMaker endpoint to classify the image
Handler: lambda_function.lambda_handler
Note: Uses boto3 SageMaker runtime (more reliable than SageMaker SDK in Lambda)
"""

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


# =============================================================================
# Lambda Function 3: Filter Low Confidence Inferences
# =============================================================================
"""
Function Name: FilterLowConfidenceInferences
Runtime: Python 3.8
Description: Filters out low confidence predictions based on threshold
Handler: lambda_filter.lambda_handler (deployed version)
"""

import json

THRESHOLD = 0.93

def lambda_handler(event, context):
    """Filter low confidence inferences"""
    
    # Grab the inferences from the event
    inferences = json.loads(event['body'])['inferences']
    inferences = json.loads(inferences)
    
    # Check if any values in our inferences are above THRESHOLD
    meets_threshold = max(inferences) >= THRESHOLD
    
    # If our threshold is met, pass our data back out of the
    # Step Function, else, end the Step Function with an error
    if meets_threshold:
        pass
    else:
        raise Exception("THRESHOLD_CONFIDENCE_NOT_MET")

    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }


# =============================================================================
# Test Data Generator
# =============================================================================


import random
import boto3
import json

def generate_test_case(bucket_name):
    """Generate a test case for Step Function execution"""
    
    # Setup s3 in boto3
    s3 = boto3.resource('s3')
    
    # Randomly pick from test folder in our bucket
    objects = s3.Bucket(bucket_name).objects.filter(Prefix="test/")
    
    # Grab any random object key from that folder!
    obj = random.choice([x.key for x in objects])
    
    return {
        "image_data": "",
        "s3_bucket": bucket_name,
        "s3_key": obj
    }

# Example usage:
# test_input = generate_test_case("sagemaker-us-east-1-135808922609")
# print(json.dumps(test_input, indent=2))
