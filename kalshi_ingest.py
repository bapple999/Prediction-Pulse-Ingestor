import os
import requests
import psycopg2
from datetime import datetime

# Connect to Supabase Postgres using env vars
conn = psycopg2.connect(
    host=os.environ['DB_HOST'],
    database=os.environ['DB_NAME'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASS'],
    port=os.environ.get('DB_PORT', 5432)
)

cur = conn.cursor()

# Kalshi API endpoint (elections only)
KALSHI_API = 'https://api.elections.kalshi.com/trade-api/v2/markets'

try:
    print("🔍 Requesting markets from Kalshi Elections API...")
    response = requests.get(KALSHI_API)
    response.raise_for_status()

    markets = response.json().get('markets', [])
    print(f"✅ Retrieved {len(markets)} markets")

    # Filter markets with volume > 0
    filtered_markets = [m for m in markets if m.get('volume', 0) > 0]
    print(f"📊 Filtered down to {len(filtered_markets)} markets with volume > 0")

    for market in filtered_markets:
        market_id = market.get('ticker')
        market_name = market.get('title')
        yes_bid = market.get('yes_bid')
        no_bid = market.get('no_bid')

        if yes_bid is not None and no_bid is not None:
            probability = (yes_bid + (1 - no_bid)) / 2
        else:
            continue  # Skip if bid prices are missing

        volume = market.get('volume', 0)
        liquidity = market.get('open_interest', 0)

        cur.execute(
            """
            INSERT INTO market_snapshots
            (market_id, market_name, timestamp, probability, volume, liquidity)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (market_id, market_name, datetime.utcnow(), probability, volume, liquidity)
        )

    conn.commit()
    print(f"🎉 Inserted {len(filtered_markets)} markets into Supabase at {datetime.utcnow()}")

except Exception as e:
    print("❌ Error pulling or inserting Kalshi data:", e)

finally:
    cur.close()
    conn.close()
