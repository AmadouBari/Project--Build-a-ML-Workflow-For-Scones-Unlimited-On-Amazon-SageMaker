#!/usr/bin/env python3
"""
SNS Error Notification System for Scones Unlimited ML Workflow
Implements error handling and alerting for Step Function failures
"""

import json
import boto3
from datetime import datetime
import uuid

# =============================================================================
# Lambda Function: Error Notification Handler
# =============================================================================
"""
Function Name: StepFunctionErrorHandler
Runtime: Python 3.8
Description: Sends formatted notifications when Step Functions fail
Handler: lambda_function.lambda_handler
Trigger: EventBridge rule for Step Function state changes
"""

def lambda_handler(event, context):
    """Handle Step Function error notifications"""
    
    sns = boto3.client('sns')
    
    # SNS Topic ARN for error notifications
    SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:135808922609:scones-unlimited-ml-errors"
    
    # Extract Step Function execution details from EventBridge event
    detail = event.get('detail', {})
    
    execution_arn = detail.get('executionArn', 'Unknown')
    state_machine_arn = detail.get('stateMachineArn', 'Unknown')
    status = detail.get('status', 'Unknown')
    
    # Only process failed executions
    if status not in ['FAILED', 'TIMED_OUT', 'ABORTED']:
        return {
            'statusCode': 200,
            'body': json.dumps('Not an error status, skipping notification')
        }
    
    # Get execution name from ARN
    execution_name = execution_arn.split(':')[-1] if execution_arn != 'Unknown' else 'Unknown'
    state_machine_name = state_machine_arn.split(':')[-1] if state_machine_arn != 'Unknown' else 'Unknown'
    
    # Get additional details if available
    start_date = detail.get('startDate', 'Unknown')
    stop_date = detail.get('stopDate', 'Unknown')
    error = detail.get('error', 'No error details available')
    cause = detail.get('cause', 'No cause details available')
    
    # Format the error notification
    subject = f"🚨 Scones Unlimited ML Workflow Error - {state_machine_name}"
    
    message = f"""
🛵 SCONES UNLIMITED - ML WORKFLOW ALERT 🛵

❌ Step Function Execution Failed

📋 EXECUTION DETAILS:
   • State Machine: {state_machine_name}
   • Execution: {execution_name}
   • Status: {status}
   • Start Time: {start_date}
   • Stop Time: {stop_date}

🔍 ERROR INFORMATION:
   • Error: {error}
   • Cause: {cause}

🔗 LINKS:
   • Execution ARN: {execution_arn}
   • State Machine ARN: {state_machine_arn}

⚡ IMMEDIATE ACTIONS REQUIRED:
   1. Check AWS Step Functions console for detailed logs
   2. Verify SageMaker endpoint health
   3. Validate Lambda function permissions
   4. Review input data format
   5. Check S3 bucket accessibility

🏥 HEALTH CHECK SUGGESTIONS:
   • Test individual Lambda functions
   • Verify SageMaker endpoint is InService
   • Check CloudWatch logs for detailed error traces
   • Validate IAM roles and permissions
   • Review recent Step Function execution history

📊 BUSINESS IMPACT:
   • Delivery vehicle classification temporarily unavailable
   • Manual routing may be required for new orders
   • Operations team should implement fallback procedures

This is an automated alert from the Scones Unlimited ML monitoring system.
Time: {datetime.utcnow().isoformat()}Z
Incident ID: {str(uuid.uuid4())[:8]}
    """
    
    try:
        # Send SNS notification
        response = sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message,
            MessageAttributes={
                'AlertType': {
                    'DataType': 'String',
                    'StringValue': 'StepFunctionFailure'
                },
                'Severity': {
                    'DataType': 'String', 
                    'StringValue': 'HIGH'
                },
                'Service': {
                    'DataType': 'String',
                    'StringValue': 'SconesUnlimited-ML'
                }
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Error notification sent successfully',
                'messageId': response['MessageId'],
                'execution': execution_name,
                'status': status
            })
        }
        
    except Exception as e:
        print(f"Failed to send SNS notification: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to send notification',
                'details': str(e)
            })
        }


# =============================================================================
# CloudFormation Template for SNS Setup
# =============================================================================

SNS_CLOUDFORMATION_TEMPLATE = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Description": "SNS Error Notification Setup for Scones Unlimited ML Workflow",
    "Parameters": {
        "EmailAddress": {
            "Type": "String",
            "Description": "Email address for error notifications",
            "Default": "operations@sconesunlimited.com"
        },
        "SlackWebhookUrl": {
            "Type": "String", 
            "Description": "Slack webhook URL for notifications (optional)",
            "Default": ""
        }
    },
    "Resources": {
        "SconesMLErrorTopic": {
            "Type": "AWS::SNS::Topic",
            "Properties": {
                "TopicName": "scones-unlimited-ml-errors",
                "DisplayName": "Scones Unlimited ML Workflow Errors"
            }
        },
        "EmailSubscription": {
            "Type": "AWS::SNS::Subscription",
            "Properties": {
                "Protocol": "email",
                "TopicArn": {"Ref": "SconesMLErrorTopic"},
                "Endpoint": {"Ref": "EmailAddress"}
            }
        },
        "StepFunctionErrorRule": {
            "Type": "AWS::Events::Rule",
            "Properties": {
                "Name": "scones-ml-stepfunction-errors",
                "Description": "Capture Step Function failures for Scones Unlimited ML workflow",
                "EventPattern": {
                    "source": ["aws.states"],
                    "detail-type": ["Step Functions Execution Status Change"],
                    "detail": {
                        "status": ["FAILED", "TIMED_OUT", "ABORTED"],
                        "stateMachineArn": [
                            {"wildcard": "*ImageClassStateMachine*"}
                        ]
                    }
                },
                "State": "ENABLED",
                "Targets": [
                    {
                        "Id": "ErrorHandlerLambda",
                        "Arn": {
                            "Fn::GetAtt": ["ErrorHandlerFunction", "Arn"]
                        }
                    }
                ]
            }
        },
        "ErrorHandlerFunction": {
            "Type": "AWS::Lambda::Function",
            "Properties": {
                "FunctionName": "StepFunctionErrorHandler",
                "Runtime": "python3.8",
                "Handler": "lambda_function.lambda_handler",
                "Code": {
                    "ZipFile": "# Error handler code would be uploaded separately"
                },
                "Environment": {
                    "Variables": {
                        "SNS_TOPIC_ARN": {"Ref": "SconesMLErrorTopic"}
                    }
                },
                "Role": {"Fn::GetAtt": ["ErrorHandlerRole", "Arn"]}
            }
        },
        "ErrorHandlerRole": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "AssumeRolePolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "lambda.amazonaws.com"},
                            "Action": "sts:AssumeRole"
                        }
                    ]
                },
                "ManagedPolicyArns": [
                    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                ],
                "Policies": [
                    {
                        "PolicyName": "SNSPublishPolicy",
                        "PolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Action": ["sns:Publish"],
                                    "Resource": {"Ref": "SconesMLErrorTopic"}
                                }
                            ]
                        }
                    }
                ]
            }
        },
        "LambdaInvokePermission": {
            "Type": "AWS::Lambda::Permission",
            "Properties": {
                "Action": "lambda:InvokeFunction",
                "FunctionName": {"Ref": "ErrorHandlerFunction"},
                "Principal": "events.amazonaws.com",
                "SourceArn": {"Fn::GetAtt": ["StepFunctionErrorRule", "Arn"]}
            }
        }
    },
    "Outputs": {
        "SNSTopicArn": {
            "Description": "ARN of the SNS topic for error notifications",
            "Value": {"Ref": "SconesMLErrorTopic"}
        },
        "EventRuleArn": {
            "Description": "ARN of the EventBridge rule",
            "Value": {"Fn::GetAtt": ["StepFunctionErrorRule", "Arn"]}
        }
    }
}


# =============================================================================
# Slack Integration Lambda Function
# =============================================================================
"""
Function Name: SlackErrorNotifier
Runtime: Python 3.8
Description: Sends formatted error notifications to Slack
Handler: lambda_function.lambda_handler
"""

def slack_lambda_handler(event, context):
    """Send Step Function errors to Slack"""
    
    import urllib3
    import json
    
    # Slack webhook URL (store in environment variable or Parameter Store)
    SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')
    
    if not SLACK_WEBHOOK_URL:
        return {
            'statusCode': 400,
            'body': json.dumps('Slack webhook URL not configured')
        }
    
    # Parse SNS message
    sns_message = json.loads(event['Records'][0]['Sns']['Message'])
    
    # Extract execution details
    detail = sns_message.get('detail', {})
    execution_name = detail.get('executionArn', 'Unknown').split(':')[-1]
    status = detail.get('status', 'Unknown')
    error = detail.get('error', 'No error details')
    
    # Format Slack message
    slack_message = {
        "text": "🚨 Scones Unlimited ML Workflow Alert",
        "attachments": [
            {
                "color": "danger",
                "fields": [
                    {
                        "title": "Execution",
                        "value": execution_name,
                        "short": True
                    },
                    {
                        "title": "Status", 
                        "value": status,
                        "short": True
                    },
                    {
                        "title": "Error",
                        "value": error,
                        "short": False
                    }
                ],
                "footer": "Scones Unlimited ML Monitoring",
                "ts": int(datetime.utcnow().timestamp())
            }
        ]
    }
    
    # Send to Slack
    http = urllib3.PoolManager()
    response = http.request(
        'POST',
        SLACK_WEBHOOK_URL,
        body=json.dumps(slack_message),
        headers={'Content-Type': 'application/json'}
    )
    
    return {
        'statusCode': response.status,
        'body': json.dumps('Slack notification sent')
    }


# =============================================================================
# Setup Scripts
# =============================================================================

def setup_sns_notifications():
    """Setup SNS topic and subscriptions"""
    
    sns = boto3.client('sns')
    events = boto3.client('events')
    
    # Create SNS topic
    topic_response = sns.create_topic(
        Name='scones-unlimited-ml-errors',
        Attributes={
            'DisplayName': 'Scones Unlimited ML Errors'
        }
    )
    
    topic_arn = topic_response['TopicArn']
    print(f"Created SNS topic: {topic_arn}")
    
    # Create EventBridge rule
    rule_response = events.put_rule(
        Name='scones-ml-stepfunction-errors',
        EventPattern=json.dumps({
            "source": ["aws.states"],
            "detail-type": ["Step Functions Execution Status Change"],
            "detail": {
                "status": ["FAILED", "TIMED_OUT", "ABORTED"],
                "stateMachineArn": [{"wildcard": "*ImageClassStateMachine*"}]
            }
        }),
        State='ENABLED',
        Description='Capture Step Function failures for Scones ML workflow'
    )
    
    print(f"Created EventBridge rule: {rule_response['RuleArn']}")
    
    return {
        'topic_arn': topic_arn,
        'rule_arn': rule_response['RuleArn']
    }


def subscribe_email_to_topic(topic_arn, email_address):
    """Subscribe email address to SNS topic"""
    
    sns = boto3.client('sns')
    
    response = sns.subscribe(
        TopicArn=topic_arn,
        Protocol='email',
        Endpoint=email_address
    )
    
    print(f"Subscribed {email_address} to topic {topic_arn}")
    print(f"Subscription ARN: {response['SubscriptionArn']}")
    
    return response['SubscriptionArn']


if __name__ == "__main__":
    print("SNS Error Notification System for Scones Unlimited")
    print("=" * 55)
    
    # Example setup (uncomment to run)
    # setup_result = setup_sns_notifications()
    # subscribe_email_to_topic(setup_result['topic_arn'], 'your-email@example.com')
    
    print("CloudFormation template available in SNS_CLOUDFORMATION_TEMPLATE")
    print("Deploy the template to set up complete error notification system")