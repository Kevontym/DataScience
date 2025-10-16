# create_sample_data.py
import os

# Create directories
os.makedirs('data/raw/reviews', exist_ok=True)
os.makedirs('data/processed', exist_ok=True)

# Create sample review files
reviews = [
    "The customer service was excellent and very helpful! The representative went above and beyond to solve my issue quickly.",
    "Product arrived damaged, very disappointed. The packaging was inadequate and the item was broken upon arrival.",
    "Amazing product quality and fast shipping. I will definitely purchase from this company again. Five stars!",
    "Average experience. The product works as described but the setup instructions were confusing and customer support was slow to respond."
]

for i, review in enumerate(reviews, 1):
    with open(f'data/raw/reviews/review{i}.txt', 'w') as f:
        f.write(review)

# Create sample CSV
csv_content = """id,name,email,rating,review_text,date
1,John Doe,john@email.com,5,"Great product, loved it!",2024-01-15
2,Jane Smith,jane@email.com,3,"It was okay, could be better",2024-01-16
3,Bob Wilson,bob@email.com,4,"Pretty good overall",2024-01-17
4,Alice Brown,alice@email.com,2,"The product was fine but shipping was slow",2024-01-18
5,Charlie Davis,,5,"Excellent quality and fast delivery",
6,Diana Evans,diana@email.com,1,"Terrible experience, product didn't work",2024-01-19"""

with open('data/raw/customer_data.csv', 'w') as f:
    f.write(csv_content)

print("✅ Sample data created successfully!")