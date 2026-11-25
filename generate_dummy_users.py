import csv
import os
import random
from datetime import datetime, timedelta

def generate_dummy_users(output_path="data/users.csv", num_records=100):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson"]
    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "company.com"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin"]
    genders = ["Male", "Female", "Other"]

    headers = ["user_id", "first_name", "last_name", "email", "gender", "age", "city", "signup_date", "is_active"]
    
    start_date = datetime(2023, 1, 1)
    
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for user_id in range(1001, 1001 + num_records):
            first = random.choice(first_names)
            last = random.choice(last_names)
            email = f"{first.lower()}.{last.lower()}{random.randint(10, 99)}@{random.choice(domains)}"
            gender = random.choice(genders)
            age = random.randint(18, 65)
            city = random.choice(cities)
            signup_date = (start_date + timedelta(days=random.randint(0, 700))).strftime("%Y-%m-%d")
            is_active = random.choice([True, True, True, False])  # 75% active

            writer.writerow([user_id, first, last, email, gender, age, city, signup_date, is_active])

    print(f"Generated {num_records} dummy user records at '{output_path}'.")

if __name__ == "__main__":
    generate_dummy_users(num_records=50)
