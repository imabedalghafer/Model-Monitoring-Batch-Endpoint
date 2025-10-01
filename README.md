# Model Monitor Batch Endpoint

This project implements model monitoring for a credit card default prediction model using Azure Machine Learning batch endpoints. The monitoring system tracks prediction drift and data drift to ensure model performance over time.

## Project Structure

```text
.
├── batch_endpoint_monitoring.yaml      # Main monitoring configuration with schedule
├── README.md                          # This file
├── code/
│   └── batch_driver.py               # Batch scoring driver script
├── components/
│   └── custom_preprocessing/          # Custom preprocessing component
│       ├── spec.yaml                 # Component specification
│       └── src/
│           └── run.py               # Preprocessing logic
├── configurations/
│   └── training_pipeline.yaml       # Training pipeline configuration
├── environments/
│   ├── score.yaml                   # Scoring environment definition
│   └── train.yaml                   # Training environment definition
└── notebooks/
    └── model-monitoring-e2e.ipynb  # End-to-end example notebook
```

## Overview

This solution monitors a credit card default prediction model by:

- **Prediction Drift Monitoring**: Tracks changes in model predictions over time using collected production data
- **Scheduled Execution**: Runs daily at 3:15 AM to analyze recent model performance
- **Custom Preprocessing**: Filters data based on time windows for accurate monitoring
- **Alert System**: Configurable thresholds for drift detection (currently disabled)

## Key Components

### 1. Monitoring Configuration (`batch_endpoint_monitoring.yaml`)

The main configuration file that defines:

- **Schedule**: Daily monitoring at 3:15 AM
- **Monitoring Signals**: Prediction drift analysis using Pearson's Chi-squared test
- **Data Sources**:
  - Production data from `azureml:credit-default-output-folder:1`
  - Reference data from `azureml:credit-default-reference:1`
- **Compute**: Standard E4s v3 instance for processing

### 2. Batch Driver (`code/batch_driver.py`)

The scoring script that:

- Loads the trained model using MLflow
- Processes batch inference requests
- Collects input/output data for monitoring
- Handles credit card default prediction logic

### 3. Custom Preprocessing Component

Located in `components/custom_preprocessing/`, this Spark-based component:

- Filters production data based on time windows
- Prepares data for monitoring analysis
- Outputs preprocessed data in MLTable format

### 4. End-to-End Notebook

The `notebooks/model-monitoring-e2e.ipynb` provides a complete walkthrough:

- Environment setup
- Data asset registration
- Model training and deployment
- Inference simulation
- Monitoring setup and validation

## Setup Instructions

1. **Environment Setup**: Ensure you have access to an Azure ML workspace
2. **Data Registration**: Register your datasets as Azure ML assets
3. **Model Deployment**: Deploy your model to a batch endpoint
4. **Monitor Configuration**: Update the monitoring YAML with your specific asset names and versions

## Important Notes

⚠️ **Critical Configuration Notes:**

### Schedule Submission

- **DO NOT fill the last input field** when submitting the schedule manually through the Azure ML Studio UI
- The schedule configuration should be handled entirely through the YAML file parameters

### Timestamp Management

- **Keep timestamps within the correct range** for your data window
- Incorrect timestamp ranges will cause the monitoring pipeline to fail
- Ensure `data_window_start` and `data_window_end` parameters align with your actual data availability
- The custom preprocessing component relies on these timestamps to filter relevant data

## Monitoring Metrics

Currently configured thresholds:

- **Prediction Drift**: Pearson's Chi-squared test with threshold of 0.02
- **Alert Status**: Disabled (set `alert_enabled: false`)

## Usage

1. **Manual Execution**: Submit the monitoring job through Azure ML Studio or CLI
2. **Scheduled Execution**: The system runs automatically daily at 3:15 AM
3. **Monitoring Results**: Check the Azure ML Studio for drift analysis results and alerts

## Troubleshooting

- **Pipeline Failures**: Verify timestamp ranges and data asset availability
- **Preprocessing Errors**: Check that custom preprocessing component version matches registered component
- **Data Issues**: Ensure production data follows the expected schema and format

## Data Assets Required

- `azureml:credit-default-output-folder:1` - Production/collected data for monitoring
- `azureml:credit-default-reference:1` - Reference dataset (typically training data)
- `azureml:custom_preprocessor:2.13.0` - Custom preprocessing component

Make sure these assets are registered in your Azure ML workspace before running the monitoring pipeline.