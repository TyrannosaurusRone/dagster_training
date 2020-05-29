import os
import uuid
import json
import shutil
import pandas as pd
from datetime import date, datetime, timedelta
from product_matching.schema import fake
from grada_pyspark_utils.timestamp import format_timestamp
 
 
ARCHIVE = "./dagster_training_data/RAW_BASKET"
 
def main():
   mock_data()
 
 
def load():
   filenames = [p for p in os.listdir(ARCHIVE) if p.endswith('.json')]
 
   data = []
   for filename in sorted(filenames):
       p = os.path.join(ARCHIVE, filename)
       with open(p) as f:
           data.append(json.load(f))
 
   basket_ids = []
   timestamps = []
   item_ids = []
   product_names = []
   categories = []
 
   for row in data:
       for item in row["items"]:
           basket_ids.append(row["basket_id"])
           timestamps.append(row["timestamp"])
           item_ids.append(item["item_id"])
           product_names.append(item["product_name"])
           categories.append(item["category"])
 
   return pd.DataFrame({
       "basket_id": basket_ids,
       "timestamp": timestamps,
       "item_id": item_ids,
       "product_name": product_names,
       "category": categories,
   })
 
 
def mock_data(count=1000):
   if not os.path.exists(ARCHIVE):
       os.makedirs(ARCHIVE)
   else:
       shutil.rmtree(ARCHIVE)
 
   for i in range(count):
       b = basket()
       p = f"{ARCHIVE}/{b['basket_id']}.json"
       with open(p, "w") as f:
           f.write(json.dumps(b))
 
 
def archive_start_date():
   return date.today() - timedelta(days=90)
 
 
def basket():
   t = datetime.utcnow() - timedelta(fake.integer(min=1, max=90))
   c = fake.integer(min=1, max=20)
 
   return {
       "basket_id": str(uuid.uuid4()),
       "timestamp": format_timestamp(t),
       "items": [basket_item() for _ in range(c)],
   }
 
def basket_item():
  
   return {
       "item_id": str(uuid.uuid4()),
       "product_name": fake.product_name(fake.retailer_brand()),
       "category": fake.category()
   }
 
if __name__ == "__main__":
   main()
