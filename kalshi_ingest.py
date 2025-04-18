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

# Use the Kalshi Elections API
KALSHI_API = 'https://api.elections.kalshi.com/trade-api/v2/markets'
all_markets = []
next_url = KALSHI_API

print("🔁 Starting Kalshi market pagination...")

try:
    while next_url:
        response = requests.get(next_url)
        response.raise_for_status()
        data = response.json()

        markets = data.get("markets", [])
        all_markets.extend(markets)
        print(f"📥 Fetched {len(markets)} markets (total so far: {len(all_markets)})")

        # Pagination: update next_url or exit loop
        next_url = data.get("next")

    # Filter markets with volume > 0
    filtered_markets = [m for m in all_markets if m.get('volume', 0) > 0]
    print(f"📊 Filtered down to {len(filtered_markets)} markets with volume > 0")

    # Insert each filtered market into Supabase
    for market in filtered_markets:
        market_id = market.get('ticker')
        market_name = market.get('title')
        yes_bid = market.get('yes_bid')
        no_bid = market.get('no_bid')

        if yes_bid is not None and no_bid is not None:
            probability = (yes_bid + (1 - no_bid)) / 2
        else:
            continue  # Skip if bid data is incomplete

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
    print(f"✅ Inserted {len(filtered_markets)} markets into Supabase at {datetime.utcnow()}")

except Exception as e:
    print("❌ Error pulling or inserting Kalshi data:", e)

finally:
    cur.close()
    conn.close()
