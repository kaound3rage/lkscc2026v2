import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid
import os

# Tentukan folder output = folder tempat script ini berada
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

def save_csv(df, filename):
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False)
    print(f"Saved: {path} ({len(df):,} records)")
    return path

np.random.seed(42)
random.seed(42)

# 1. GENERATE USER PROFILES DATASET
print("Generating user_profiles.csv...")

cities = ['Jakarta', 'Surabaya', 'Bandung', 'Medan', 'Semarang', 'Makassar', 'Palembang', 'Tangerang', 'Depok', 'Bekasi']
provinces = ['DKI Jakarta', 'Jawa Timur', 'Jawa Barat', 'Sumatera Utara', 'Jawa Tengah', 'Sulawesi Selatan', 'Sumatera Selatan', 'Banten', 'Jawa Barat', 'Jawa Barat']
subscription_plans = ['Free', 'Basic', 'Standard', 'Premium']

users_data = []
for i in range(1, 1001):
    user_id = f"user_{i:05d}"
    reg_date = datetime.now() - timedelta(days=random.randint(30, 730))
    age = random.randint(13, 65)
    gender = random.choice(['M', 'F'])
    city_idx = random.randint(0, len(cities) - 1)
    city = cities[city_idx]
    province = provinces[city_idx]
    plan = random.choice(subscription_plans)
    total_streams = random.randint(0, 500)
    total_watch_hours = round(total_streams * random.uniform(0.5, 2.5), 1)
    last_active = datetime.now() - timedelta(days=random.randint(0, 30))

    if total_streams >= 200:
        segment = 'Power User'
    elif total_streams >= 80:
        segment = 'Regular'
    elif total_streams >= 20:
        segment = 'Casual'
    else:
        segment = 'New'

    users_data.append({
        'user_id': user_id,
        'registration_date': reg_date.strftime('%Y-%m-%d'),
        'age': age,
        'gender': gender,
        'location_city': city,
        'location_province': province,
        'subscription_plan': plan,
        'total_streams': total_streams,
        'total_watch_hours': total_watch_hours,
        'last_active': last_active.strftime('%Y-%m-%d %H:%M:%S'),
        'customer_segment': segment
    })

users_df = pd.DataFrame(users_data)
save_csv(users_df, 'user_profiles.csv')

# 2. GENERATE CONTENT CATALOG DATASET
print("\nGenerating content_catalog.csv...")

content_types = ['Movie', 'Series', 'Music', 'Podcast']
genres = ['Action', 'Comedy', 'Drama', 'Horror', 'Romance', 'Sci-Fi', 'Documentary', 'Thriller']
languages = ['Indonesian', 'English', 'Korean', 'Japanese', 'Spanish']
studios = ['Netflix Original', 'Disney+', 'HBO', 'Warner Bros', 'Universal', 'Paramount', 'A24', 'Blumhouse']

content_names = {
    'Movie': [
        'The Last Horizon', 'Midnight in Jakarta', 'Code Red', 'Beyond the Storm',
        'Silent Frequency', 'The Wandering Soul', 'Urban Shadows', 'Fire and Rain'
    ],
    'Series': [
        'Dark Corners', 'The Agency', 'Neon District', 'Blood & Tide',
        'Fracture', 'The Compound', 'Echo Protocol', 'Black Mirror Effect'
    ],
    'Music': [
        'Neon Dreams Album', 'Acoustic Sessions Vol.1', 'City Beats Collection',
        'Midnight Vibes Playlist', 'Indie Gold Compilation', 'Electronic Pulse Mix'
    ],
    'Podcast': [
        'Tech Decoded', 'True Crime Weekly', 'Mind & Body', 'History Unfolded',
        'Startup Stories', 'Science Today', 'The Finance Hour', 'Culture Shift'
    ]
}

content_data = []
for i in range(1, 501):
    content_id = f"content_{i:05d}"
    content_type = random.choice(content_types)
    genre = random.choice(genres)
    language = random.choice(languages)
    studio = random.choice(studios)
    title = random.choice(content_names[content_type]) + f" {random.randint(1, 9)}"

    duration_minutes = (
        random.randint(80, 180) if content_type == 'Movie'
        else random.randint(20, 60) if content_type == 'Series'
        else random.randint(3, 5) if content_type == 'Music'
        else random.randint(20, 90)
    )

    avg_rating = round(random.uniform(2.5, 5.0), 1)
    num_ratings = random.randint(50, 50000)
    is_exclusive = random.choice([True, False])
    release_date = datetime.now() - timedelta(days=random.randint(0, 1825))

    content_data.append({
        'content_id': content_id,
        'title': title,
        'content_type': content_type,
        'genre': genre,
        'language': language,
        'studio': studio,
        'duration_minutes': duration_minutes,
        'avg_rating': avg_rating,
        'num_ratings': num_ratings,
        'is_exclusive': is_exclusive,
        'release_date': release_date.strftime('%Y-%m-%d')
    })

content_df = pd.DataFrame(content_data)
save_csv(content_df, 'content_catalog.csv')

# 3. GENERATE USER INTERACTIONS DATASET
print("\nGenerating user_interactions.csv...")

interaction_types = ['play', 'like', 'add_to_watchlist', 'complete', 'skip']
device_types = ['mobile', 'smart_tv', 'desktop', 'tablet']

interactions_data = []
for i in range(10000):
    user_id = f"user_{random.randint(1, 1000):05d}"
    content_id = f"content_{random.randint(1, 500):05d}"

    if random.random() < 0.55:
        interaction_type = 'play'
    elif random.random() < 0.20:
        interaction_type = 'complete'
    elif random.random() < 0.12:
        interaction_type = 'like'
    elif random.random() < 0.08:
        interaction_type = 'add_to_watchlist'
    else:
        interaction_type = 'skip'

    timestamp = datetime.now() - timedelta(days=random.randint(0, 90))
    session_id = f"sess_{uuid.uuid4().hex[:8]}"
    device_type = random.choice(device_types)
    watch_duration = random.randint(30, 7200)

    rating = round(random.uniform(1, 5), 1) if interaction_type == 'complete' else None

    interactions_data.append({
        'user_id': user_id,
        'content_id': content_id,
        'interaction_type': interaction_type,
        'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        'session_id': session_id,
        'device_type': device_type,
        'watch_duration_seconds': watch_duration,
        'rating': rating
    })

interactions_df = pd.DataFrame(interactions_data)
save_csv(interactions_df, 'user_interactions.csv')

# 4. GENERATE SUBSCRIPTION HISTORY DATASET
print("\nGenerating subscription_history.csv...")

billing_periods = ['monthly', 'annual']
payment_methods = ['credit_card', 'debit_card', 'e_wallet', 'bank_transfer']
subscription_statuses = ['active', 'expired', 'cancelled']

plan_prices = {
    'Basic': {'monthly': 49000, 'annual': 499000},
    'Standard': {'monthly': 79000, 'annual': 799000},
    'Premium': {'monthly': 119000, 'annual': 1199000}
}

subscribed_users = users_df[users_df['subscription_plan'] != 'Free'].copy()

subscriptions_data = []
for _, user in subscribed_users.iterrows():
    subscription_id = f"sub_{uuid.uuid4().hex[:8]}"
    user_id = user['user_id']
    plan = user['subscription_plan']
    billing_period = random.choice(billing_periods)
    amount = plan_prices[plan][billing_period]
    payment_method = random.choice(payment_methods)

    subscription_date = datetime.now() - timedelta(days=random.randint(1, 365))
    renewal_days = 30 if billing_period == 'monthly' else 365
    renewal_date = subscription_date + timedelta(days=renewal_days)

    status = random.choices(
        subscription_statuses,
        weights=[0.65, 0.20, 0.15]
    )[0]

    subscriptions_data.append({
        'subscription_id': subscription_id,
        'user_id': user_id,
        'plan': plan,
        'billing_period': billing_period,
        'amount': amount,
        'payment_method': payment_method,
        'subscription_date': subscription_date.strftime('%Y-%m-%d'),
        'renewal_date': renewal_date.strftime('%Y-%m-%d'),
        'status': status
    })

subscriptions_df = pd.DataFrame(subscriptions_data)
save_csv(subscriptions_df, 'subscription_history.csv')

# 5. SUMMARY
print("\nDATASET SUMMARY")
print("-" * 50)
print(f"Users         : {len(users_df):,} records")
print(f"Content       : {len(content_df):,} records")
print(f"Interactions  : {len(interactions_df):,} records")
print(f"Subscriptions : {len(subscriptions_df):,} records")

print(f"\nInteraction Types:")
print(interactions_df['interaction_type'].value_counts())

print(f"\nContent Types:")
print(content_df['content_type'].value_counts())

print(f"\nCustomer Segments:")
print(users_df['customer_segment'].value_counts())

print(f"\nSubscription Revenue Summary:")
total_revenue = subscriptions_df['amount'].sum()
avg_subscription = subscriptions_df['amount'].mean()
print(f"Total Revenue        : Rp {total_revenue:,.0f}")
print(f"Avg Subscription Fee : Rp {avg_subscription:,.0f}")

print(f"\nSemua file dataset disimpan di: {OUTPUT_DIR}")