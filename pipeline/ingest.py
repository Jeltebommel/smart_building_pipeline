# pipeline/ingest.py

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def generate_iot_data(num_rows=1000):
    now = datetime.now()
    timestamps = [now - timedelta(minutes=5 * i) for i in range(num_rows)]

    data = {
        "timestamp": list(reversed(timestamps)),
        "temperature": np.random.normal(21, 2, num_rows),
        "co2": np.random.normal(450, 50, num_rows),
        "light": np.random.uniform(100, 1000, num_rows),
        "energy_kwh": np.random.exponential(0.5, num_rows)
    }

    df = pd.DataFrame(data)
    
    os.makedirs("../azure_simulation/raw", exist_ok=True)
    df.to_csv("../azure_simulation/raw/iot_raw.csv", index=False)
    print("✅ Raw data generated and saved to azure_simulation/raw/iot_raw.csv")

if __name__ == "__main__":
    generate_iot_data()
