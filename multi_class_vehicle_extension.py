#!/usr/bin/env python3
"""
Multi-Class Vehicle Extension for Scones Unlimited ML Workflow
Extends the binary bicycle/motorcycle classifier to support additional vehicle types
"""

import json
import boto3
import numpy as np
from typing import Dict, List, Any

# =============================================================================
# Extended Vehicle Classes from CIFAR-100
# =============================================================================

VEHICLE_CLASSES = {
    0: "bicycle",
    1: "motorcycle", 
    2: "automobile",
    3: "truck",
    4: "bus",
    5: "pickup_truck",
    6: "streetcar",
    7: "tank",
    8: "tractor",
    9: "lawn_mower"
}

# Enhanced routing rules for Scones Unlimited delivery optimization
ROUTING_RULES = {
    "bicycle": {
        "max_distance": 5,  # km
        "max_weight": 10,   # kg
        "terrain": ["urban", "bike_lanes"],
        "speed": "slow",
        "eco_friendly": True,
        "weather_dependent": True
    },
    "motorcycle": {
        "max_distance": 25,
        "max_weight": 25,
        "terrain": ["urban", "suburban", "highways"],
        "speed": "fast",
        "eco_friendly": False,
        "weather_dependent": True
    },
    "automobile": {
        "max_distance": 50,
        "max_weight": 50,
        "terrain": ["urban", "suburban", "highways"],
        "speed": "medium",
        "eco_friendly": False,
        "weather_dependent": False
    },
    "truck": {
        "max_distance": 100,
        "max_weight": 500,
        "terrain": ["highways", "industrial"],
        "speed": "medium",
        "eco_friendly": False,
        "weather_dependent": False
    },
    "bus": {
        "max_distance": 30,
        "max_weight": 0,  # Passenger only
        "terrain": ["urban", "suburban"],
        "speed": "slow",
        "eco_friendly": False,
        "weather_dependent": False,
        "passenger_capacity": 50
    },
    "pickup_truck": {
        "max_distance": 75,
        "max_weight": 200,
        "terrain": ["urban", "suburban", "rural"],
        "speed": "medium",
        "eco_friendly": False,
        "weather_dependent": False
    }
}

# =============================================================================
# Enhanced Lambda Function: Multi-Class Image Classification
# =============================================================================
"""
Function Name: MultiClassImageClassification
Runtime: Python 3.8
Description: Classifies images into multiple vehicle categories
Handler: lambda_function.lambda_handler
"""

def multi_class_lambda_handler(event, context):
    """Enhanced image classification with multiple vehicle types"""
    
    runtime = boto3.client('sagemaker-runtime')
    
    # Multi-class endpoint (would need to be trained separately)
    MULTICLASS_ENDPOINT = "multi-vehicle-classification-endpoint"
    
    # Get image data from previous step
    image_data = event.get("image_data", "")
    s3_bucket = event.get("s3_bucket", "")
    s3_key = event.get("s3_key", "")
    
    if not image_data:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'No image data provided',
                's3_bucket': s3_bucket,
                's3_key': s3_key
            })
        }
    
    try:
        import base64
        
        # Decode the image
        image = base64.b64decode(image_data)
        
        # Make prediction using multi-class SageMaker endpoint
        response = runtime.invoke_endpoint(
            EndpointName=MULTICLASS_ENDPOINT,
            ContentType='image/png',
            Body=image
        )
        
        # Get the inference results  
        result = response['Body'].read()
        inferences = json.loads(result.decode('utf-8'))
        
        # Process multi-class predictions
        predictions = []
        for i, confidence in enumerate(inferences):
            if i < len(VEHICLE_CLASSES):
                predictions.append({
                    "class_id": i,
                    "class_name": VEHICLE_CLASSES[i],
                    "confidence": float(confidence)
                })
        
        # Find top prediction
        top_prediction = max(predictions, key=lambda x: x['confidence'])
        
        # Get routing information
        routing_info = ROUTING_RULES.get(top_prediction['class_name'], {})
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'image_data': image_data,
                's3_bucket': s3_bucket,
                's3_key': s3_key,
                'predictions': predictions,
                'top_prediction': top_prediction,
                'routing_info': routing_info,
                'inference_metadata': {
                    'model_version': 'multi-class-v1.0',
                    'classes_supported': len(VEHICLE_CLASSES),
                    'prediction_timestamp': context.aws_request_id
                }
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'image_data': image_data,
                's3_bucket': s3_bucket,
                's3_key': s3_key
            })
        }


# =============================================================================
# Enhanced Lambda Function: Multi-Class Confidence Filtering
# =============================================================================
"""
Function Name: MultiClassFilterConfidence
Runtime: Python 3.8
Description: Filters multi-class predictions and provides routing decisions
Handler: lambda_function.lambda_handler
"""

# Confidence thresholds per vehicle type
CONFIDENCE_THRESHOLDS = {
    "bicycle": 0.85,      # Lower threshold for common delivery vehicle
    "motorcycle": 0.85,   # Lower threshold for common delivery vehicle  
    "automobile": 0.90,   # Higher threshold for larger vehicles
    "truck": 0.95,        # Highest threshold for commercial vehicles
    "bus": 0.95,          # Highest threshold for passenger vehicles
    "pickup_truck": 0.90, # Higher threshold for larger vehicles
    "streetcar": 0.98,    # Very high - uncommon for delivery
    "tank": 0.99,         # Extremely high - not for delivery
    "tractor": 0.95,      # High threshold for specialized vehicles
    "lawn_mower": 0.90    # High threshold - edge case
}

def multi_class_filter_lambda_handler(event, context):
    """Filter multi-class predictions and provide routing decisions"""
    
    try:
        # Extract data from previous step
        body = json.loads(event.get('body', '{}'))
        predictions = body.get('predictions', [])
        top_prediction = body.get('top_prediction', {})
        routing_info = body.get('routing_info', {})
        
        if not predictions:
            raise Exception("NO_PREDICTIONS_RECEIVED")
        
        # Apply vehicle-specific confidence threshold
        vehicle_type = top_prediction.get('class_name', 'unknown')
        confidence = top_prediction.get('confidence', 0.0)
        threshold = CONFIDENCE_THRESHOLDS.get(vehicle_type, 0.95)
        
        # Determine if prediction meets threshold
        meets_threshold = confidence >= threshold
        
        # Generate routing decision
        routing_decision = generate_routing_decision(
            vehicle_type, confidence, routing_info, meets_threshold
        )
        
        # Filter all predictions by their respective thresholds
        filtered_predictions = []
        for pred in predictions:
            pred_threshold = CONFIDENCE_THRESHOLDS.get(pred['class_name'], 0.95)
            pred['meets_threshold'] = pred['confidence'] >= pred_threshold
            pred['threshold_used'] = pred_threshold
            if pred['meets_threshold']:
                filtered_predictions.append(pred)
        
        response_body = {
            'vehicle_classification': {
                'primary_vehicle': vehicle_type,
                'confidence': confidence,
                'threshold_used': threshold,
                'meets_threshold': meets_threshold
            },
            'all_predictions': predictions,
            'high_confidence_predictions': filtered_predictions,
            'routing_decision': routing_decision,
            'business_rules': {
                'can_deliver': meets_threshold and vehicle_type in ['bicycle', 'motorcycle', 'automobile', 'pickup_truck'],
                'requires_manual_review': not meets_threshold or vehicle_type in ['tank', 'streetcar', 'tractor'],
                'eco_friendly_option': routing_info.get('eco_friendly', False)
            }
        }
        
        # Raise exception if prediction doesn't meet business requirements
        if not meets_threshold:
            raise Exception(f"CONFIDENCE_BELOW_THRESHOLD: {vehicle_type} confidence {confidence:.3f} below required {threshold}")
        
        if vehicle_type not in ['bicycle', 'motorcycle', 'automobile', 'pickup_truck', 'truck']:
            raise Exception(f"UNSUPPORTED_VEHICLE_TYPE: {vehicle_type} not suitable for delivery operations")
        
        return {
            'statusCode': 200,
            'body': json.dumps(response_body)
        }
        
    except Exception as e:
        error_body = {
            'error': str(e),
            'vehicle_classification': body.get('top_prediction', {}),
            'routing_decision': {
                'action': 'MANUAL_REVIEW',
                'reason': 'Classification failed quality checks',
                'fallback_vehicle': 'bicycle'  # Safe default
            }
        }
        
        return {
            'statusCode': 200,
            'body': json.dumps(error_body)
        }


def generate_routing_decision(vehicle_type: str, confidence: float, routing_info: Dict, meets_threshold: bool) -> Dict[str, Any]:
    """Generate intelligent routing decision based on vehicle classification"""
    
    if not meets_threshold:
        return {
            'action': 'REJECT',
            'reason': f'Low confidence ({confidence:.3f}) for {vehicle_type}',
            'alternative': 'Request new image or manual classification'
        }
    
    # Business logic for vehicle assignment
    if vehicle_type in ['bicycle', 'motorcycle']:
        return {
            'action': 'ASSIGN_ROUTE',
            'route_type': 'SHORT_DISTANCE',
            'max_distance_km': routing_info.get('max_distance', 5),
            'max_weight_kg': routing_info.get('max_weight', 10),
            'priority': 'HIGH',  # Fast, eco-friendly options
            'special_instructions': 'Weather-dependent scheduling'
        }
    
    elif vehicle_type in ['automobile', 'pickup_truck']:
        return {
            'action': 'ASSIGN_ROUTE', 
            'route_type': 'MEDIUM_DISTANCE',
            'max_distance_km': routing_info.get('max_distance', 50),
            'max_weight_kg': routing_info.get('max_weight', 50),
            'priority': 'MEDIUM',
            'special_instructions': 'All-weather capable'
        }
    
    elif vehicle_type == 'truck':
        return {
            'action': 'ASSIGN_ROUTE',
            'route_type': 'LONG_DISTANCE_BULK',
            'max_distance_km': routing_info.get('max_distance', 100),
            'max_weight_kg': routing_info.get('max_weight', 500),
            'priority': 'LOW',  # For bulk deliveries
            'special_instructions': 'Commercial delivery routes only'
        }
    
    else:
        return {
            'action': 'MANUAL_REVIEW',
            'reason': f'Unusual vehicle type: {vehicle_type}',
            'alternative': 'Human dispatcher review required'
        }


# =============================================================================
# Enhanced Step Function Definition for Multi-Class Workflow
# =============================================================================

MULTICLASS_STEP_FUNCTION_DEFINITION = {
    "Comment": "Multi-Class Vehicle Classification Workflow for Scones Unlimited",
    "StartAt": "SerializeImageData",
    "States": {
        "SerializeImageData": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
                "FunctionName": "SerializeImageData:$LATEST",
                "Payload.$": "$"
            },
            "Next": "MultiClassClassification"
        },
        "MultiClassClassification": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke", 
            "Parameters": {
                "FunctionName": "MultiClassImageClassification:$LATEST",
                "Payload.$": "$.Payload"
            },
            "Next": "MultiClassFilterConfidence"
        },
        "MultiClassFilterConfidence": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
                "FunctionName": "MultiClassFilterConfidence:$LATEST",
                "Payload.$": "$.Payload"
            },
            "Next": "RoutingDecision"
        },
        "RoutingDecision": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.Payload.body.routing_decision.action",
                    "StringEquals": "ASSIGN_ROUTE",
                    "Next": "RouteAssignmentSuccess"
                },
                {
                    "Variable": "$.Payload.body.routing_decision.action", 
                    "StringEquals": "MANUAL_REVIEW",
                    "Next": "ManualReviewRequired"
                }
            ],
            "Default": "ClassificationRejected"
        },
        "RouteAssignmentSuccess": {
            "Type": "Succeed",
            "Comment": "Vehicle classified and route assigned successfully"
        },
        "ManualReviewRequired": {
            "Type": "Pass",
            "Comment": "Vehicle requires manual dispatcher review",
            "End": True
        },
        "ClassificationRejected": {
            "Type": "Fail",
            "Error": "ClassificationQualityError",
            "Cause": "Vehicle classification did not meet confidence requirements"
        }
    }
}


# =============================================================================
# Training Data Preparation for Multi-Class Model
# =============================================================================

def prepare_multiclass_training_data():
    """
    Script to prepare CIFAR-100 data for multi-class vehicle training
    
    This would extract the vehicle classes from CIFAR-100:
    - bicycle (original)
    - motorcycle (original) 
    - automobile, bus, pickup_truck, streetcar, tank, tractor, lawn_mower
    
    The resulting dataset would have balanced samples across all vehicle types
    """
    
    vehicle_cifar_mapping = {
        "bicycle": 8,       # CIFAR-100 class ID
        "motorcycle": 48,   # CIFAR-100 class ID
        "automobile": 1,    # CIFAR-100 class ID  
        "bus": 13,          # CIFAR-100 class ID
        "pickup_truck": 58, # CIFAR-100 class ID
        "streetcar": 90,    # CIFAR-100 class ID
        "tank": 85,         # CIFAR-100 class ID
        "tractor": 84,      # CIFAR-100 class ID
        "lawn_mower": 38    # CIFAR-100 class ID
    }
    
    training_config = {
        "algorithm": "image-classification",
        "hyperparameters": {
            "num_classes": len(vehicle_cifar_mapping),
            "image_shape": "3,32,32",
            "num_training_samples": 4500,  # 500 per class
            "epochs": 30,
            "learning_rate": 0.001,
            "batch_size": 32
        },
        "training_data": {
            "source": "CIFAR-100",
            "classes": vehicle_cifar_mapping,
            "augmentation": True,
            "validation_split": 0.2
        }
    }
    
    return training_config


# =============================================================================
# Business Analytics for Multi-Class System
# =============================================================================

def analyze_fleet_optimization(classification_results: List[Dict]) -> Dict:
    """Analyze classification results for fleet optimization insights"""
    
    vehicle_counts = {}
    total_capacity = {"distance": 0, "weight": 0}
    eco_friendly_count = 0
    
    for result in classification_results:
        vehicle_type = result.get('vehicle_classification', {}).get('primary_vehicle', 'unknown')
        
        # Count vehicles by type
        vehicle_counts[vehicle_type] = vehicle_counts.get(vehicle_type, 0) + 1
        
        # Calculate total fleet capacity
        routing_info = result.get('routing_decision', {})
        total_capacity["distance"] += routing_info.get('max_distance_km', 0)
        total_capacity["weight"] += routing_info.get('max_weight_kg', 0)
        
        # Count eco-friendly options
        if result.get('business_rules', {}).get('eco_friendly_option', False):
            eco_friendly_count += 1
    
    # Generate insights
    total_vehicles = len(classification_results)
    eco_percentage = (eco_friendly_count / total_vehicles * 100) if total_vehicles > 0 else 0
    
    return {
        "fleet_composition": vehicle_counts,
        "total_capacity": total_capacity,
        "sustainability_metrics": {
            "eco_friendly_vehicles": eco_friendly_count,
            "eco_friendly_percentage": eco_percentage
        },
        "optimization_recommendations": generate_fleet_recommendations(vehicle_counts)
    }


def generate_fleet_recommendations(vehicle_counts: Dict[str, int]) -> List[str]:
    """Generate fleet optimization recommendations"""
    
    recommendations = []
    total_vehicles = sum(vehicle_counts.values())
    
    if total_vehicles == 0:
        return ["No vehicle data available for analysis"]
    
    # Analyze fleet balance
    bicycle_ratio = vehicle_counts.get('bicycle', 0) / total_vehicles
    motorcycle_ratio = vehicle_counts.get('motorcycle', 0) / total_vehicles
    
    if bicycle_ratio < 0.3:
        recommendations.append("Consider increasing bicycle fleet for eco-friendly short-distance deliveries")
    
    if motorcycle_ratio < 0.2:
        recommendations.append("Add more motorcycles for medium-distance urban deliveries")
    
    if vehicle_counts.get('truck', 0) == 0:
        recommendations.append("Consider adding trucks for bulk delivery capability")
    
    # Efficiency recommendations
    if bicycle_ratio > 0.6:
        recommendations.append("High bicycle ratio detected - excellent for sustainability goals")
    
    if vehicle_counts.get('automobile', 0) > vehicle_counts.get('bicycle', 0):
        recommendations.append("Balance automobile usage with more eco-friendly alternatives")
    
    return recommendations


if __name__ == "__main__":
    print("🚗 Multi-Class Vehicle Extension for Scones Unlimited")
    print("=" * 60)
    
    # Display supported vehicle types
    print("Supported Vehicle Classes:")
    for class_id, vehicle_name in VEHICLE_CLASSES.items():
        routing = ROUTING_RULES.get(vehicle_name, {})
        print(f"  {class_id}: {vehicle_name.title()} - Max Distance: {routing.get('max_distance', 'N/A')}km")
    
    print(f"\nTraining Configuration:")
    config = prepare_multiclass_training_data()
    print(f"  Classes: {config['hyperparameters']['num_classes']}")
    print(f"  Training Samples: {config['hyperparameters']['num_training_samples']}")
    print(f"  Image Shape: {config['hyperparameters']['image_shape']}")
    
    print(f"\nStep Function Definition available in MULTICLASS_STEP_FUNCTION_DEFINITION")
    print(f"Enhanced routing rules support business logic for delivery optimization")