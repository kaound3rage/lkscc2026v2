import json
import boto3
import os
from datetime import datetime, timedelta
import logging
import random
from typing import Dict, Any, List, Tuple, Optional
from boto3.dynamodb.conditions import Key
import math

# Only import pickle if needed to avoid potential conflicts
try:
    import pickle
except ImportError:
    pickle = None

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

USER_INTERACTIONS_TABLE = os.environ.get('USER_INTERACTIONS_TABLE', 'UserInteractions')
CONTENT_EMBEDDINGS_TABLE = os.environ.get('CONTENT_EMBEDDINGS_TABLE', 'ContentEmbeddings')
STREAM_HISTORY_TABLE = os.environ.get('STREAM_HISTORY_TABLE', 'StreamHistory')

def calculate_mean(data: List[float]) -> float:
    """Calculate arithmetic mean."""
    return sum(data) / len(data) if data else 0

def calculate_stdev(data: List[float]) -> float:
    """Calculate sample standard deviation."""
    if len(data) < 2:
        return 0
    mean_val = calculate_mean(data)
    variance = sum((x - mean_val) ** 2 for x in data) / (len(data) - 1)
    return math.sqrt(variance)


class SalesForecastingModel:
    """Sales forecasting model with multiple methods"""
    def __init__(self):
        self.model_version = "1.0"
        
    @staticmethod
    def moving_average_forecast(data: List[float], window: int = 7, periods: int = 30) -> List[float]:
        """Simple moving average forecasting."""
        if len(data) < window:
            avg = calculate_mean(data) if data else 0
            return [avg] * periods

        forecasts = []
        for i in range(periods):
            if i == 0:
                window_data = data[-window:]
            else:
                window_data = data[-(window-i):] + forecasts[:i] if i < window else forecasts[-window:]
            forecasts.append(calculate_mean(window_data))

        return forecasts

    @staticmethod
    def exponential_smoothing_forecast(data: List[float], alpha: float = 0.3, periods: int = 30) -> List[float]:
        """Exponential smoothing forecasting."""
        if not data:
            return [0] * periods

        smoothed = data[0]
        for value in data[1:]:
            smoothed = alpha * value + (1 - alpha) * smoothed

        return [smoothed] * periods

    @staticmethod
    def linear_trend_forecast(data: List[float], periods: int = 30) -> List[float]:
        """Linear trend forecasting."""
        if len(data) < 2:
            avg = calculate_mean(data) if data else 0
            return [avg] * periods

        n = len(data)
        x = list(range(n))
        y = data

        x_mean = calculate_mean(x)
        y_mean = calculate_mean(y)

        numerator = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            return [y_mean] * periods

        slope = numerator / denominator
        intercept = y_mean - slope * x_mean

        return [max(0, intercept + slope * (n + i)) for i in range(periods)]

    @staticmethod
    def seasonal_forecast(data: List[float], seasonality: int = 7, periods: int = 30) -> List[float]:
        """Simple seasonal forecasting."""
        if len(data) < seasonality:
            avg = calculate_mean(data) if data else 0
            return [avg] * periods

        forecasts = []
        for i in range(periods):
            seasonal_index = i % seasonality
            seasonal_data = [data[j] for j in range(seasonal_index, len(data), seasonality)]
            forecasts.append(calculate_mean(seasonal_data) if seasonal_data else 0)

        return forecasts

def load_forecasting_model(bucket: str, key: str) -> SalesForecastingModel:
    """Load forecasting model from S3, falling back to default implementation."""
    try:
        if pickle is None:
            logger.warning("Pickle not available, using fallback implementation")
            return SalesForecastingModel()

        local_path = '/tmp/forecasting_model.pkl'
        s3.download_file(bucket, key, local_path)

        with open(local_path, 'rb') as f:
            model = pickle.load(f)
            logger.info("Successfully loaded forecasting model from S3")
            return model

    except Exception as e:
        logger.warning(f"Model load failed: {str(e)}. Using fallback implementation")
        return SalesForecastingModel()


def get_historical_stream_data(content_id: str = None, content_type: str = None, days: int = 90) -> List[Dict[str, Any]]:
    """Retrieve historical stream data from DynamoDB."""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        user_interactions_table = dynamodb.Table(USER_INTERACTIONS_TABLE)

        filter_expression = Key('interaction_type').eq('play')
        if content_id:
            filter_expression = filter_expression & Key('content_id').eq(content_id)

        response = user_interactions_table.scan(
            FilterExpression=filter_expression,
            Limit=1000
        )

        stream_data = []
        for item in response.get('Items', []):
            try:
                item_date = datetime.fromisoformat(item['timestamp'].replace('Z', '+00:00'))
                if start_date <= item_date <= end_date:
                    stream_data.append({
                        'date': item_date.strftime('%Y-%m-%d'),
                        'content_id': item.get('content_id'),
                        'streams': 1,
                        'watch_duration': int(item.get('watch_duration_seconds', 0)),
                        'content_type': item.get('content_type', 'unknown')
                    })
            except (ValueError, KeyError) as e:
                logger.warning(f"Skipping invalid stream record: {e}")
                continue

        logger.info(f"Retrieved {len(stream_data)} stream records")
        return stream_data

    except Exception as e:
        logger.error(f"Failed to get historical stream data: {str(e)}")
        return []


def aggregate_streams_by_date(stream_data: List[Dict[str, Any]], metric: str = 'streams') -> Dict[str, float]:
    """Aggregate stream data by date."""
    aggregated = {}
    for record in stream_data:
        date = record['date']
        value = record.get(metric, 0)
        aggregated[date] = aggregated.get(date, 0) + value
    return aggregated


def fill_missing_dates(data: Dict[str, float], start_date: datetime, end_date: datetime) -> List[float]:
    """Fill missing dates with zeros and return as an ordered list."""
    result = []
    current_date = start_date
    while current_date <= end_date:
        result.append(data.get(current_date.strftime('%Y-%m-%d'), 0))
        current_date += timedelta(days=1)
    return result


def generate_forecast(content_id: str = None, content_type: str = None, method: str = 'moving_average',
                     periods: int = 30, metric: str = 'streams') -> Dict[str, Any]:
    """Generate a stream forecast using the specified method."""
    try:
        historical_data = get_historical_stream_data(content_id, content_type, days=90)

        if not historical_data:
            logger.warning("No historical data found, using random forecast")
            return {
                'forecast': [random.uniform(50, 300) for _ in range(periods)],
                'method': 'fallback',
                'confidence': 'low',
                'historical_data_points': 0
            }

        aggregated_data = aggregate_streams_by_date(historical_data, metric)

        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        time_series = fill_missing_dates(aggregated_data, start_date, end_date)

        model = load_forecasting_model(
            os.environ.get('FORECASTING_MODEL_BUCKET', 'techmart-ml-handi'),
            os.environ.get('FORECASTING_MODEL_KEY', 'models/forecasting_model.pkl')
        )

        method_map = {
            'moving_average': model.moving_average_forecast,
            'exponential_smoothing': model.exponential_smoothing_forecast,
            'linear_trend': model.linear_trend_forecast,
            'seasonal': model.seasonal_forecast,
        }
        forecast_fn = method_map.get(method, model.moving_average_forecast)
        forecast = forecast_fn(time_series, periods=periods)

        confidence = 'high' if len(historical_data) > 50 else 'medium' if len(historical_data) > 20 else 'low'

        return {
            'forecast': forecast,
            'method': method,
            'confidence': confidence,
            'historical_data_points': len(historical_data),
            'historical_average': calculate_mean(time_series) if time_series else 0,
            'forecast_dates': [
                (datetime.now() + timedelta(days=i)).strftime('%Y-%m-%d')
                for i in range(1, periods + 1)
            ]
        }

    except Exception as e:
        logger.error(f"Forecast generation failed: {str(e)}")
        return {
            'forecast': [random.uniform(50, 300) for _ in range(periods)],
            'method': 'fallback',
            'confidence': 'low',
            'error': str(e)
        }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        body = event
        if isinstance(event.get('body'), str):
            body = json.loads(event['body'])

        content_id = body.get('content_id')
        content_type = body.get('content_type')
        method = body.get('method', 'moving_average')
        periods = int(body.get('periods', 30))
        metric = body.get('metric', 'streams')

        valid_methods = ['moving_average', 'exponential_smoothing', 'linear_trend', 'seasonal']
        if method not in valid_methods:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Invalid method',
                    'valid_methods': valid_methods
                })
            }

        if periods > 365:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Periods cannot exceed 365 days'})
            }

        logger.info(f"Generating forecast - content_id: {content_id}, content_type: {content_type}, method: {method}")

        forecast_result = generate_forecast(content_id, content_type, method, periods, metric)

        forecast_values = forecast_result['forecast']
        summary = {
            'total_forecast': sum(forecast_values),
            'average_daily': calculate_mean(forecast_values),
            'min_daily': min(forecast_values),
            'max_daily': max(forecast_values),
            'std_deviation': calculate_stdev(forecast_values) if len(forecast_values) > 1 else 0
        }

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'forecast': forecast_result['forecast'],
                'forecast_dates': forecast_result.get('forecast_dates', []),
                'method': forecast_result['method'],
                'confidence': forecast_result['confidence'],
                'metric': metric,
                'periods': periods,
                'summary': summary,
                'historical_data_points': forecast_result.get('historical_data_points', 0),
                'historical_average': forecast_result.get('historical_average', 0),
                'timestamp': datetime.now().isoformat(),
                'status': 'success'
            })
        }

    except Exception as e:
        logger.error(f"Handler error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal server error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }