import mysql.connector
import random
from datetime import datetime, timedelta

conn = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="******",
    password="********",
    database="ecommerce"
)

cursor = conn.cursor()

categories = [
    "electronics",
    "fashion",
    "books",
    "sports",
    "furniture"
]

products = []

for i in range(1, 10001):

    name = f"Product_{i}"

    category = random.choice(categories)

    price = round(random.uniform(100, 100000), 2)

    last_updated = datetime.now() - timedelta(
        minutes=random.randint(0, 50000)
    )

    products.append(
        (
            i,
            name,
            category,
            price,
            last_updated
        )
    )

query = """
INSERT INTO product
(id,name,category,price,last_updated)
VALUES (%s,%s,%s,%s,%s)
"""

cursor.executemany(query, products)

conn.commit()

print("10000 records inserted successfully")

cursor.close()
conn.close()