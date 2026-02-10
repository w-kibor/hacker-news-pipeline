import requests
import pandas as pd
from datetime import datetime

def extract_hn_tech_news(limit=20):
    # 1. Get the IDs of the top 500 stories
    top_stories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    ids = requests.get(top_stories_url).json()
    
    posts = []
    print(f"🚀 Fetching details for the top {limit} stories...")
    
    # 2. Fetch details for each ID
    for item_id in ids[:limit]:
        item_url = f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
        data = requests.get(item_url).json()
        
        posts.append({
            "id": data.get("id"),
            "title": data.get("title"),
            "author": data.get("by"),
            "score": data.get("score"),
            "url": data.get("url"),
            "time_utc": datetime.fromtimestamp(data.get("time")),
            "descendants": data.get("descendants") # This is the comment count
        })
    
    return pd.DataFrame(posts)

# Run the extraction
df = extract_hn_tech_news()

# 3. Impact Step: Save as Parquet
filename = f"hn_tech_news_{datetime.now().strftime('%Y%m%d_%H')}.parquet"
df.to_parquet(filename, engine='pyarrow')

print(f"✅ Success! Saved {len(df)} tech stories to {filename}")