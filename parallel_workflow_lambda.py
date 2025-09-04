# Enhanced Lambda Functions for Parallel Processing (Fan-Out Pattern)
# Scones Unlimited Image Classification Workflow

# =============================================================================
# Lambda Function: Batch Image Serializer (Fan-Out)
# =============================================================================
"""
Function Name: BatchSerializeImageData
Runtime: Python 3.8
Description: Downloads multiple images from S3 and prepares them for parallel processing
Handler: lambda_function.lambda_handler
"""

import json
import boto3
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed

def lambda_handler(event, context):
    """Serialize multiple images for parallel processing"""
    
    s3 = boto3.client('s3')
    
    # Input can be either single image or batch of images
    if isinstance(event.get('s3_keys'), list):
        # Batch processing mode
        s3_keys = event['s3_keys']
        bucket = event['s3_bucket']
    else:
        # Single image mode (backward compatibility)
        s3_keys = [event['s3_key']]
        bucket = event['s3_bucket']
    
    def serialize_single_image(s3_key):
        """Serialize a single image"""
        try:
            # Download the data from s3 to /tmp/
            local_file = f"/tmp/{s3_key.split('/')[-1]}"
            s3.download_file(bucket, s3_key, local_file)
            
            # Read and encode the data
            with open(local_file, "rb") as f:
                image_data = base64.b64encode(f.read())
            
            return {
                "success": True,
                "s3_key": s3_key,
                "image_data": image_data.decode('utf-8'),
                "s3_bucket": bucket
            }
        except Exception as e:
            return {
                "success": False,
                "s3_key": s3_key,
                "error": str(e),
                "s3_bucket": bucket
            }
    
    # Process images in parallel
    serialized_images = []
    max_workers = min(len(s3_keys), 10)  # Limit concurrent downloads
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {executor.submit(serialize_single_image, key): key for key in s3_keys}
        
        for future in as_completed(future_to_key):
            result = future.result()
            serialized_images.append(result)
    
    # Separate successful and failed serializations
    successful_images = [img for img in serialized_images if img['success']]
    failed_images = [img for img in serialized_images if not img['success']]
    
    return {
        'statusCode': 200,
        'body': {
            "serialized_images": successful_images,
            "failed_images": failed_images,
            "total_requested": len(s3_keys),
            "successful_count": len(successful_images),
            "failed_count": len(failed_images)
        }
    }


# =============================================================================
# Lambda Function: Parallel Image Classification
# =============================================================================
"""
Function Name: ParallelImageClassification
Runtime: Python 3.8
Description: Processes multiple images in parallel using SageMaker endpoint
Handler: lambda_function.lambda_handler
"""

import json
import boto3
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed

def lambda_handler(event, context):
    """Classify multiple images in parallel"""
    
    runtime = boto3.client('sagemaker-runtime')
    ENDPOINT = "image-classification-2025-08-21-23-57-05-598"
    
    # Extract serialized images from previous step
    serialized_images = event['body']['serialized_images']
    
    def classify_single_image(image_data):
        """Classify a single image"""
        try:
            # Decode the image data
            image_bytes = base64.b64decode(image_data['image_data'])
            
            # Make prediction using SageMaker runtime
            response = runtime.invoke_endpoint(
                EndpointName=ENDPOINT,
                ContentType='image/png',
                Body=image_bytes
            )
            
            # Get the inference results
            result = response['Body'].read()
            inferences = result.decode('utf-8')
            
            return {
                "success": True,
                "s3_key": image_data['s3_key'],
                "s3_bucket": image_data['s3_bucket'],
                "image_data": image_data['image_data'],
                "inferences": inferences
            }
            
        except Exception as e:
            return {
                "success": False,
                "s3_key": image_data['s3_key'],
                "s3_bucket": image_data['s3_bucket'],
                "error": str(e)
            }
    
    # Process classifications in parallel
    classification_results = []
    max_workers = min(len(serialized_images), 5)  # Limit concurrent SageMaker calls
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_image = {executor.submit(classify_single_image, img): img for img in serialized_images}
        
        for future in as_completed(future_to_image):
            result = future.result()
            classification_results.append(result)
    
    # Separate successful and failed classifications
    successful_classifications = [result for result in classification_results if result['success']]
    failed_classifications = [result for result in classification_results if not result['success']]
    
    return {
        'statusCode': 200,
        'body': {
            "classifications": successful_classifications,
            "failed_classifications": failed_classifications,
            "total_processed": len(classification_results),
            "successful_count": len(successful_classifications),
            "failed_count": len(failed_classifications)
        }
    }


# =============================================================================
# Lambda Function: Batch Filter Low Confidence
# =============================================================================
"""
Function Name: BatchFilterLowConfidence
Runtime: Python 3.8
Description: Filters multiple predictions based on confidence threshold
Handler: lambda_function.lambda_handler
"""

import json

THRESHOLD = 0.93

def lambda_handler(event, context):
    """Filter multiple inferences based on confidence threshold"""
    
    # Extract classifications from previous step
    classifications = event['body']['classifications']
    failed_classifications = event['body'].get('failed_classifications', [])
    
    high_confidence_results = []
    low_confidence_results = []
    processing_errors = []
    
    for classification in classifications:
        try:
            # Parse the inferences
            inferences = json.loads(classification['inferences'])
            max_confidence = max(inferences)
            
            # Determine predicted class
            predicted_class = 0 if inferences[0] > inferences[1] else 1
            class_names = ['bicycle', 'motorcycle']
            
            result = {
                "s3_key": classification['s3_key'],
                "s3_bucket": classification['s3_bucket'],
                "inferences": inferences,
                "max_confidence": max_confidence,
                "predicted_class": predicted_class,
                "predicted_label": class_names[predicted_class],
                "meets_threshold": max_confidence >= THRESHOLD
            }
            
            if max_confidence >= THRESHOLD:
                high_confidence_results.append(result)
            else:
                low_confidence_results.append(result)
                
        except Exception as e:
            processing_errors.append({
                "s3_key": classification['s3_key'],
                "error": str(e),
                "raw_inferences": classification.get('inferences', 'N/A')
            })
    
    # Summary statistics
    total_processed = len(classifications)
    high_confidence_count = len(high_confidence_results)
    low_confidence_count = len(low_confidence_results)
    error_count = len(processing_errors) + len(failed_classifications)
    
    # Determine overall status
    if high_confidence_count == 0 and total_processed > 0:
        # All predictions were low confidence - this should trigger an alert
        overall_status = "ALL_LOW_CONFIDENCE"
    elif error_count > total_processed * 0.5:
        # More than 50% errors
        overall_status = "HIGH_ERROR_RATE"
    elif high_confidence_count > 0:
        overall_status = "SUCCESS"
    else:
        overall_status = "NO_VALID_PREDICTIONS"
    
    response = {
        'statusCode': 200,
        'body': {
            "overall_status": overall_status,
            "high_confidence_results": high_confidence_results,
            "low_confidence_results": low_confidence_results,
            "processing_errors": processing_errors,
            "failed_classifications": failed_classifications,
            "summary": {
                "total_processed": total_processed,
                "high_confidence_count": high_confidence_count,
                "low_confidence_count": low_confidence_count,
                "error_count": error_count,
                "success_rate": (high_confidence_count / total_processed * 100) if total_processed > 0 else 0,
                "threshold_used": THRESHOLD
            }
        }
    }
    
    # Raise error if all predictions are low confidence (for Step Functions error handling)
    if overall_status == "ALL_LOW_CONFIDENCE":
        raise Exception(f"ALL_PREDICTIONS_BELOW_THRESHOLD: {low_confidence_count} predictions below {THRESHOLD} confidence")
    elif overall_status == "HIGH_ERROR_RATE":
        raise Exception(f"HIGH_ERROR_RATE: {error_count} errors out of {total_processed} total")
    elif overall_status == "NO_VALID_PREDICTIONS":
        raise Exception("NO_VALID_PREDICTIONS: No successful predictions generated")
    
    return response


# =============================================================================
# Test Data Generator for Parallel Workflow
# =============================================================================

def generate_batch_test_case(bucket_name, num_images=5):
    """Generate test case for parallel workflow"""
    import boto3
    import random
    
    s3 = boto3.client('s3')
    
    # Get available test images
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix="test/")
    available_images = [obj['Key'] for obj in response.get('Contents', []) 
                       if obj['Key'].endswith('.png')]
    
    # Select random images for batch processing
    selected_images = random.sample(available_images, min(num_images, len(available_images)))
    
    return {
        "s3_bucket": bucket_name,
        "s3_keys": selected_images
    }


# Example Step Function Definition for Parallel Processing
PARALLEL_STEP_FUNCTION_DEFINITION = {
    "Comment": "Parallel Image Classification Workflow for Scones Unlimited",
    "StartAt": "BatchSerializeImages", 
    "States": {
        "BatchSerializeImages": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
                "FunctionName": "BatchSerializeImageData:$LATEST",
                "Payload.$": "$"
            },
            "Next": "CheckSerializationSuccess"
        },
        "CheckSerializationSuccess": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.Payload.body.successful_count",
                    "NumericGreaterThan": 0,
                    "Next": "ParallelClassification"
                }
            ],
            "Default": "SerializationFailed"
        },
        "ParallelClassification": {
            "Type": "Task", 
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
                "FunctionName": "ParallelImageClassification:$LATEST",
                "Payload.$": "$.Payload"
            },
            "Next": "BatchFilterConfidence"
        },
        "BatchFilterConfidence": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke", 
            "Parameters": {
                "FunctionName": "BatchFilterLowConfidence:$LATEST",
                "Payload.$": "$.Payload"
            },
            "End": True
        },
        "SerializationFailed": {
            "Type": "Fail",
            "Error": "SerializationError",
            "Cause": "Failed to serialize any images for processing"
        }
    }
}


if __name__ == "__main__":
    # Example usage and testing
    print("Parallel Workflow Lambda Functions for Scones Unlimited")
    print("=" * 60)
    
    # Example test case
    test_case = generate_batch_test_case("sagemaker-us-east-1-135808922609", 3)
    print("Sample batch test case:")
    print(json.dumps(test_case, indent=2))