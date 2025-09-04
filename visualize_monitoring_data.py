#!/usr/bin/env python3
"""
Visualization script for SageMaker Model Monitor captured data
"""
import json
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib.dates as mdates

def parse_captured_data(file_path):
    """Parse the captured JSONL data"""
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line.strip()))
    return data

def extract_inference_data(data):
    """Extract inference results and timestamps"""
    timestamps = []
    confidences = []
    predictions = []
    
    for record in data:
        # Extract inference output
        output_data = record['captureData']['endpointOutput']['data']
        inference = json.loads(output_data)
        
        # Extract timestamp
        timestamp_str = record['eventMetadata']['inferenceTime']
        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        
        # Calculate confidence (max of the two probabilities)
        max_confidence = max(inference)
        
        # Determine prediction (0=bicycle, 1=motorcycle)
        prediction = 0 if inference[0] > inference[1] else 1
        
        timestamps.append(timestamp)
        confidences.append(max_confidence)
        predictions.append(prediction)
    
    return timestamps, confidences, predictions

def create_visualizations(timestamps, confidences, predictions):
    """Create visualizations of the monitoring data"""
    
    # Create figure with subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    # Plot 1: Confidence levels over time
    colors = ['red' if conf < 0.93 else 'blue' for conf in confidences]
    ax1.scatter(timestamps, confidences, c=colors, alpha=0.7, s=50)
    ax1.axhline(y=0.93, color='green', linestyle='--', linewidth=2, 
                label='Confidence Threshold (93%)')
    ax1.set_ylabel('Confidence Level')
    ax1.set_title('Model Confidence Levels Over Time')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(0.8, 1.0)
    
    # Format x-axis for time
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax1.xaxis.set_major_locator(mdates.SecondLocator(interval=10))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
    
    # Plot 2: Predictions over time
    prediction_labels = ['Bicycle', 'Motorcycle']
    pred_colors = ['green' if p == 0 else 'orange' for p in predictions]
    
    ax2.scatter(timestamps, predictions, c=pred_colors, alpha=0.7, s=50)
    ax2.set_ylabel('Prediction')
    ax2.set_xlabel('Time')
    ax2.set_title('Model Predictions Over Time')
    ax2.set_yticks([0, 1])
    ax2.set_yticklabels(prediction_labels)
    ax2.grid(True, alpha=0.3)
    
    # Format x-axis for time
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax2.xaxis.set_major_locator(mdates.SecondLocator(interval=10))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
    
    plt.tight_layout()
    plt.savefig('model_monitoring_visualization.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    # Print summary statistics
    print("\n=== Model Monitoring Summary ===")
    print(f"Total inferences captured: {len(confidences)}")
    print(f"Average confidence: {sum(confidences)/len(confidences):.4f}")
    print(f"Minimum confidence: {min(confidences):.4f}")
    print(f"Maximum confidence: {max(confidences):.4f}")
    print(f"Inferences above threshold (93%): {sum(1 for c in confidences if c >= 0.93)}")
    print(f"Inferences below threshold (93%): {sum(1 for c in confidences if c < 0.93)}")
    print(f"Bicycle predictions: {sum(1 for p in predictions if p == 0)}")
    print(f"Motorcycle predictions: {sum(1 for p in predictions if p == 1)}")

if __name__ == "__main__":
    # Parse the captured data
    print("Parsing captured monitoring data...")
    data = parse_captured_data('captured_data.jsonl')
    
    # Extract inference information
    timestamps, confidences, predictions = extract_inference_data(data)
    
    # Create visualizations
    print("Creating visualizations...")
    create_visualizations(timestamps, confidences, predictions)
    
    print("\nVisualization saved as 'model_monitoring_visualization.png'")