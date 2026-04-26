# Streamify ML — Machine Learning with CI/CD

A machine learning system for content recommendation and stream forecasting on a streaming platform, deployed on AWS using Lambda, DynamoDB, and S3.



   ∧＿＿∧　 ♡      dont give up,
  (｡･ω･｡)つ━☆    every bug you fix
  (　　　 )         makes you stronger!
  | | |          with love & coffee ☕
  (＿)＿)          for lkscc fighter!

         /\_/\       lks is good
        / 0 0 \      but
       =   ^   =     in my eyes you are better
        \_()_/
          U





## Architecture

| Component | Description |
|-----------|-------------|
| ETL (AWS Glue / Spark) | Processes raw interaction data from S3 and prepares features |
| Lambda - Prediction | Predicts the probability that a user will stream a given content |
| Lambda - Forecasting | Forecasts content stream counts using time-series methods |
| Flask App | Web frontend that proxies requests to Lambda via API Gateway |
| Dataset Generator | Generates synthetic streaming platform datasets for development |

## Environment Variables

### API Gateway

```
PREDICTION_API_URL=<API Gateway URL for prediction>
FORECAST_API_URL=<API Gateway URL for forecasting>
API_GATEWAY_KEY=<API Gateway key>
```

### AWS Credentials

```
AWS_ACCESS_KEY_ID=<access key>
AWS_SECRET_ACCESS_KEY=<secret access key>
AWS_SESSION_TOKEN=<session token>
```

### Flask Configuration

```
FLASK_HOST=0.0.0.0
FLASK_PORT=5000
FLASK_DEBUG=true
```

## Project Structure

```
core/
├── dataset/                  # Synthetic dataset generator
├── ETL/                      # AWS Glue / Spark ETL job
├── lambda/
│   ├── lambda_forecasting/   # Stream forecasting Lambda function
│   └── lambda_prediction/    # Content prediction Lambda function
├── machine_learning/         # Model training notebook
└── newfront/                 # Flask web application
```

## Datasets

| File | Description |
|------|-------------|
| `user_profiles.csv` | 1,000 user accounts with subscription plan and watch behavior |
| `content_catalog.csv` | 500 content items (movies, series, music, podcasts) |
| `user_interactions.csv` | 10,000 interaction events (play, like, complete, skip, add to watchlist) |
| `subscription_history.csv` | Subscription records per paying user |

## Getting Started

1. Configure environment variables as listed above.
2. Upload trained model artifacts to S3 at the path specified by `MODEL_KEY` / `FORECASTING_MODEL_KEY`.
3. Ensure DynamoDB tables exist with the names configured in each Lambda's environment variables.
4. Deploy Lambda functions and expose them through API Gateway.
5. Run the Flask application:

```bash
cd newfront
pip install -r requirements.txt
python app.py
```