#!/usr/bin/env python3
"""
Dummy Data Generator for Scones Unlimited ML Workflow
Simulates continuous stream of delivery vehicle images for testing
"""

import json
import boto3
import random
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

class DummyDataGenerator:
    def __init__(self, bucket_name, step_function_arn):
        """
        Initialize the dummy data generator
        
        Args:
            bucket_name (str): S3 bucket containing test images
            step_function_arn (str): ARN of the Step Function to invoke
        """
        self.bucket_name = bucket_name
        self.step_function_arn = step_function_arn
        self.s3_client = boto3.client('s3')
        self.stepfunctions_client = boto3.client('stepfunctions')
        self.test_images = self._get_test_images()
        
    def _get_test_images(self):
        """Get list of test images from S3 bucket"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix="test/"
            )
            
            if 'Contents' in response:
                images = [obj['Key'] for obj in response['Contents'] 
                         if obj['Key'].endswith('.png')]
                print(f"Found {len(images)} test images")
                return images
            else:
                print("No test images found in bucket")
                return []
                
        except Exception as e:
            print(f"Error listing test images: {e}")
            return []
    
    def generate_test_case(self):
        """Generate a single test case for Step Function execution"""
        if not self.test_images:
            raise ValueError("No test images available")
            
        image_key = random.choice(self.test_images)
        
        return {
            "image_data": "",
            "s3_bucket": self.bucket_name,
            "s3_key": image_key
        }
    
    def execute_step_function(self, test_input, execution_name=None):
        """
        Execute Step Function with test input
        
        Args:
            test_input (dict): Input for Step Function
            execution_name (str): Optional name for execution
            
        Returns:
            dict: Execution result with status and timing
        """
        if not execution_name:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            execution_name = f"dummy-test-{timestamp}"
        
        start_time = time.time()
        
        try:
            response = self.stepfunctions_client.start_execution(
                stateMachineArn=self.step_function_arn,
                name=execution_name,
                input=json.dumps(test_input)
            )
            
            execution_arn = response['executionArn']
            
            # Wait for execution to complete (with timeout)
            max_wait_time = 60  # seconds
            wait_start = time.time()
            
            while time.time() - wait_start < max_wait_time:
                status_response = self.stepfunctions_client.describe_execution(
                    executionArn=execution_arn
                )
                
                status = status_response['status']
                
                if status in ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']:
                    end_time = time.time()
                    duration = end_time - start_time
                    
                    result = {
                        'execution_name': execution_name,
                        'execution_arn': execution_arn,
                        'status': status,
                        'duration': duration,
                        'input': test_input,
                        'start_time': start_time,
                        'end_time': end_time
                    }
                    
                    if status == 'SUCCEEDED':
                        result['output'] = status_response.get('output')
                    elif status == 'FAILED':
                        result['error'] = status_response.get('error', 'Unknown error')
                        result['cause'] = status_response.get('cause', 'Unknown cause')
                    
                    return result
                
                time.sleep(1)  # Wait 1 second before checking again
            
            # Timeout case
            return {
                'execution_name': execution_name,
                'execution_arn': execution_arn,
                'status': 'TIMEOUT',
                'duration': time.time() - start_time,
                'input': test_input,
                'error': 'Execution timed out waiting for completion'
            }
            
        except Exception as e:
            return {
                'execution_name': execution_name,
                'status': 'ERROR',
                'duration': time.time() - start_time,
                'input': test_input,
                'error': str(e)
            }
    
    def run_load_test(self, num_executions=10, max_workers=3, delay_between_executions=1):
        """
        Run a load test with multiple parallel executions
        
        Args:
            num_executions (int): Number of executions to run
            max_workers (int): Maximum parallel workers
            delay_between_executions (float): Delay in seconds between starting executions
            
        Returns:
            list: Results from all executions
        """
        print(f"🚀 Starting load test with {num_executions} executions...")
        print(f"   Max parallel workers: {max_workers}")
        print(f"   Delay between executions: {delay_between_executions}s")
        
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all executions
            future_to_execution = {}
            
            for i in range(num_executions):
                test_case = self.generate_test_case()
                execution_name = f"load-test-{i+1:03d}-{int(time.time())}"
                
                future = executor.submit(self.execute_step_function, test_case, execution_name)
                future_to_execution[future] = {
                    'execution_number': i + 1,
                    'execution_name': execution_name,
                    'test_case': test_case
                }
                
                # Add delay between submissions
                if i < num_executions - 1:
                    time.sleep(delay_between_executions)
            
            # Collect results as they complete
            for future in as_completed(future_to_execution):
                execution_info = future_to_execution[future]
                try:
                    result = future.result()
                    result['execution_number'] = execution_info['execution_number']
                    results.append(result)
                    
                    # Print progress
                    status_emoji = "✅" if result['status'] == 'SUCCEEDED' else "❌"
                    print(f"{status_emoji} Execution {execution_info['execution_number']:3d}: "
                          f"{result['status']} ({result['duration']:.2f}s)")
                    
                except Exception as e:
                    error_result = {
                        'execution_number': execution_info['execution_number'],
                        'execution_name': execution_info['execution_name'],
                        'status': 'EXCEPTION',
                        'error': str(e),
                        'input': execution_info['test_case']
                    }
                    results.append(error_result)
                    print(f"❌ Execution {execution_info['execution_number']:3d}: EXCEPTION - {e}")
        
        return results
    
    def run_continuous_stream(self, duration_minutes=5, executions_per_minute=6):
        """
        Run a continuous stream of executions for testing
        
        Args:
            duration_minutes (int): How long to run the stream
            executions_per_minute (int): Rate of executions
        """
        total_executions = duration_minutes * executions_per_minute
        delay_between_executions = 60.0 / executions_per_minute
        
        print(f"🌊 Starting continuous stream for {duration_minutes} minutes...")
        print(f"   Rate: {executions_per_minute} executions per minute")
        print(f"   Total executions: {total_executions}")
        print(f"   Delay between executions: {delay_between_executions:.2f}s")
        
        results = []
        start_time = time.time()
        
        for i in range(total_executions):
            test_case = self.generate_test_case()
            execution_name = f"stream-{i+1:03d}-{int(time.time())}"
            
            result = self.execute_step_function(test_case, execution_name)
            result['execution_number'] = i + 1
            results.append(result)
            
            # Print progress
            status_emoji = "✅" if result['status'] == 'SUCCEEDED' else "❌"
            elapsed_minutes = (time.time() - start_time) / 60
            print(f"{status_emoji} [{elapsed_minutes:4.1f}m] Execution {i+1:3d}: "
                  f"{result['status']} ({result['duration']:.2f}s) - {result['input']['s3_key']}")
            
            # Wait before next execution
            if i < total_executions - 1:
                time.sleep(delay_between_executions)
        
        return results
    
    def analyze_results(self, results):
        """Analyze and print statistics from test results"""
        if not results:
            print("No results to analyze")
            return
        
        total = len(results)
        succeeded = len([r for r in results if r['status'] == 'SUCCEEDED'])
        failed = len([r for r in results if r['status'] == 'FAILED'])
        errors = len([r for r in results if r['status'] in ['ERROR', 'EXCEPTION', 'TIMEOUT']])
        
        durations = [r['duration'] for r in results if 'duration' in r]
        avg_duration = sum(durations) / len(durations) if durations else 0
        min_duration = min(durations) if durations else 0
        max_duration = max(durations) if durations else 0
        
        print(f"\\n📊 Test Results Analysis:")
        print(f"   Total Executions: {total}")
        print(f"   ✅ Succeeded: {succeeded} ({succeeded/total*100:.1f}%)")
        print(f"   ❌ Failed: {failed} ({failed/total*100:.1f}%)")
        print(f"   🚫 Errors: {errors} ({errors/total*100:.1f}%)")
        print(f"   ⏱️  Average Duration: {avg_duration:.2f}s")
        print(f"   📈 Min Duration: {min_duration:.2f}s")
        print(f"   📉 Max Duration: {max_duration:.2f}s")
        
        # Show error details
        if failed > 0 or errors > 0:
            print(f"\\n⚠️  Error Details:")
            for result in results:
                if result['status'] in ['FAILED', 'ERROR', 'EXCEPTION', 'TIMEOUT']:
                    print(f"   {result['execution_name']}: {result.get('error', 'Unknown error')}")


def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description='Dummy Data Generator for ML Workflow Testing')
    parser.add_argument('--bucket', required=True, help='S3 bucket name containing test images')
    parser.add_argument('--step-function-arn', required=True, help='Step Function ARN to test')
    parser.add_argument('--mode', choices=['single', 'load', 'stream'], default='single',
                       help='Test mode: single execution, load test, or continuous stream')
    parser.add_argument('--count', type=int, default=10, help='Number of executions for load test')
    parser.add_argument('--workers', type=int, default=3, help='Max parallel workers for load test')
    parser.add_argument('--duration', type=int, default=5, help='Duration in minutes for stream test')
    parser.add_argument('--rate', type=int, default=6, help='Executions per minute for stream test')
    
    args = parser.parse_args()
    
    generator = DummyDataGenerator(args.bucket, args.step_function_arn)
    
    if args.mode == 'single':
        print("Running single test execution...")
        test_case = generator.generate_test_case()
        print(f"Test case: {test_case}")
        result = generator.execute_step_function(test_case)
        generator.analyze_results([result])
        
    elif args.mode == 'load':
        print(f"Running load test with {args.count} executions...")
        results = generator.run_load_test(
            num_executions=args.count,
            max_workers=args.workers
        )
        generator.analyze_results(results)
        
    elif args.mode == 'stream':
        print(f"Running continuous stream for {args.duration} minutes...")
        results = generator.run_continuous_stream(
            duration_minutes=args.duration,
            executions_per_minute=args.rate
        )
        generator.analyze_results(results)


if __name__ == "__main__":
    # Example usage when run directly
    bucket_name = "sagemaker-us-east-1-135808922609"
    step_function_arn = "arn:aws:states:us-east-1:135808922609:stateMachine:ImageClassStateMachine"
    
    generator = DummyDataGenerator(bucket_name, step_function_arn)
    
    print("🎲 Dummy Data Generator for Scones Unlimited")
    print("=" * 50)
    
    # Run a small load test
    results = generator.run_load_test(num_executions=5, max_workers=2)
    generator.analyze_results(results)