import random

import numpy as np
import pandas as pd
from faker import Faker


def generate_fake_data(num_rows, null_percentages):
    fake = Faker()
    Faker.seed(42)
    random.seed(42)

    # Generating columns with meaningful names
    data = {
        "date_of_birth": [fake.date_of_birth() for _ in range(num_rows)],
        "timestamp": [fake.date_time_this_year() for _ in range(num_rows)],
        "full_name": [fake.name() for _ in range(num_rows)],
        "parent_name": [fake.name() for _ in range(num_rows)],
        "age": [random.randint(18, 99) for _ in range(num_rows)],
        "salary": [round(random.uniform(30000, 150000), 2) for _ in range(num_rows)],
        "is_employed": [random.choice([True, False]) for _ in range(num_rows)],
    }

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Adding null values based on percentages
    for column, null_percentage in null_percentages.items():
        if column in df.columns:
            num_nulls = int(num_rows * null_percentage / 100)
            null_indices = random.sample(range(num_rows), num_nulls)
            if df[column].dtype == bool:
                df.loc[null_indices, column] = np.nan
            else:
                df.loc[null_indices, column] = None

    return df


# Example usage
num_rows = 10_000_000
null_percentages = {
    "date_of_birth": 5,
    "timestamp": 2,
    "full_name": 1,
    "parent_name": 0,
    "age": 0,
    "salary": 10,
    "is_employed": 3,
}

df = generate_fake_data(num_rows, null_percentages)
print(df.head())

# Save to CSV
df.to_csv("fake_data.csv", index=False)
df.to_parquet("fake_data.parquet", index=False)
