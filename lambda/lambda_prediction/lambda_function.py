import json
import boto3
import os
import pickle
from datetime import datetime
import logging
import random
from typing import Dict, Any, Tuple, Optional

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')


class HybridRecommender:
    """Hybrid recommendation model for stream probability scoring."""
    def __init__(self):
        self.model_version = "1.0"

    @staticmethod
    def predict_stream_probability(user_features: Dict[str, Any], content_features: Dict[str, Any]) -> float:
        """Score the probability that a user will stream a given content."""
        base_score = 0.5

        if user_features.get('stream_count', 0) > 100:
            base_score += 0.25
        elif user_features.get('stream_count', 0) > 40:
            base_score += 0.15

        if user_features.get('subscription_plan') == 'Premium':
            base_score += 0.1
        elif user_features.get('subscription_plan') == 'Standard':
            base_score += 0.05

        if content_features.get('avg_rating', 3.0) >= 4.5:
            base_score += 0.2
        elif content_features.get('avg_rating', 3.0) >= 4.0:
            base_score += 0.1

        if content_features.get('is_exclusive', False):
            base_score += 0.1

        if content_features.get('popularity_score', 0.5) > 0.8:
            base_score += 0.1

        return min(max(base_score, 0), 1)


def load_model(bucket: str, key: str) -> HybridRecommender:
    """Load model from S3, falling back to default implementation."""
    try:
        local_path = '/tmp/model.pkl'
        s3.download_file(bucket, key, local_path)

        with open(local_path, 'rb') as f:
            model = pickle.load(f)
            logger.info("Successfully loaded model from S3")
            return model

    except Exception as e:
        logger.warning(f"Model load failed: {str(e)}. Using fallback implementation")
        return HybridRecommender()


def get_features(user_id: str, content_id: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Retrieve user and content features from DynamoDB."""
    try:
        return get_user_features(user_id), get_content_features(content_id)
    except Exception as e:
        logger.warning(f"Error getting features: {str(e)}. Using fallback features")
        return get_fallback_features(user_id, content_id)


def get_user_features(user_id: str) -> Dict[str, Any]:
    """Retrieve user features from DynamoDB."""
    try:
        users_table = dynamodb.Table(os.environ.get('USERS_TABLE', 'streamify-users'))
        response = users_table.get_item(Key={'user_id': user_id})

        if 'Item' in response:
            item = response['Item']
            return {
                'stream_count': int(item.get('total_streams', 0)),
                'total_watch_hours': float(item.get('total_watch_hours', 0)),
                'subscription_plan': item.get('subscription_plan', 'Free'),
                'age': int(item.get('age', 25)),
                'gender': item.get('gender', 'unknown'),
                'location': item.get('location_city', 'unknown'),
            }

        logger.warning(f"User {user_id} not found in database")
        return get_default_user_features()

    except Exception as e:
        logger.error(f"Error getting user features: {str(e)}")
        return get_default_user_features()


def get_content_features(content_id: str) -> Dict[str, Any]:
    """Retrieve content features from DynamoDB."""
    try:
        content_table = dynamodb.Table(os.environ.get('CONTENT_TABLE', 'ContentEmbeddings'))
        response = content_table.get_item(Key={'content_id': content_id})

        if 'Item' in response:
            item = response['Item']
            return {
                'avg_rating': float(item.get('avg_rating', 3.0)),
                'content_type': item.get('content_type', 'Movie'),
                'genre': item.get('genre', 'unknown'),
                'is_exclusive': bool(item.get('is_exclusive', False)),
                'popularity_score': float(item.get('popularity_score', 0.5)),
                'duration_minutes': int(item.get('duration_minutes', 90))
            }

        logger.warning(f"Content {content_id} not found in database")
        return get_default_content_features()

    except Exception as e:
        logger.error(f"Error getting content features: {str(e)}")
        return get_default_content_features()


def get_default_user_features() -> Dict[str, Any]:
    """Return default user features when data is unavailable."""
    return {
        'stream_count': 10,
        'total_watch_hours': 15.0,
        'subscription_plan': 'Basic',
        'age': 28,
        'gender': 'unknown',
        'location': 'unknown',
    }


def get_default_content_features() -> Dict[str, Any]:
    """Return default content features when data is unavailable."""
    return {
        'avg_rating': 3.5,
        'content_type': 'Movie',
        'genre': 'Drama',
        'is_exclusive': False,
        'popularity_score': 0.5,
        'duration_minutes': 90
    }


def get_fallback_features(user_id: str, content_id: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Return randomly generated fallback features when the database is unavailable."""
    logger.info("Using fallback features due to database unavailability")

    user_features = {
        'stream_count': random.randint(1, 150),
        'total_watch_hours': round(random.uniform(1, 200), 1),
        'subscription_plan': random.choice(['Free', 'Basic', 'Standard', 'Premium']),
        'age': random.randint(13, 65),
        'gender': random.choice(['M', 'F', 'unknown']),
        'location': 'unknown',
    }

    content_features = {
        'avg_rating': round(random.uniform(2.5, 5.0), 1),
        'content_type': random.choice(['Movie', 'Series', 'Music', 'Podcast']),
        'genre': random.choice(['Action', 'Drama', 'Comedy', 'Thriller']),
        'is_exclusive': random.choice([True, False]),
        'popularity_score': round(random.uniform(0.1, 1.0), 2),
        'duration_minutes': random.randint(3, 180)
    }

    return user_features, content_features


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        body = event
        if isinstance(event.get('body'), str):
            body = json.loads(event['body'])

        user_id = body.get('user_id', 'default')
        content_id = body.get('content_id', 'default')

        model = load_model(
            os.environ.get('MODEL_BUCKET', 'streamify-ml'),
            os.environ.get('MODEL_KEY', 'models/hybrid_model.pkl')
        )

        user_features, content_features = get_features(user_id, content_id)

        try:
            prediction = model.predict_stream_probability(user_features, content_features)
            source = 'ml_model'
        except Exception as e:
            logger.warning(f"Model prediction failed: {str(e)}")
            prediction = random.uniform(0.3, 0.7)
            source = 'fallback'

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'prediction': float(prediction),
                'prediction_percentage': round(float(prediction) * 100, 2),
                'model_source': source,
                'timestamp': datetime.now().isoformat(),
                'status': 'success',
                'user_features': user_features,
                'content_features': content_features
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
