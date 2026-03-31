import pandas as pd
import random
from datetime import datetime, timedelta


num_orders = 20
start_date = datetime(2024, 1, 1)

data = []
for i in range(1, num_orders + 1):
    order_id = i
    customer_id = random.randint(100, 110)
    status = random.choice(["completed", "pending", "shipped", "cancelled"])
    total_amount = round(random.uniform(20.0, 500.0), 2)
    order_date = start_date + timedelta(days=random.randint(0, 5))
    data.append([order_id, customer_id, status, total_amount, order_date.strftime("%Y-%m-%d")])


df = pd.DataFrame(data, columns=["order_id", "customer_id", "status", "total_amount", "order_date"])


output_path = "C:/Users/lenovo/OneDrive/Desktop/Iceberg/data/orders_2024.parquet"
df.to_parquet(output_path, index=False)

print(f"Dummy Parquet file created at: {output_path}")