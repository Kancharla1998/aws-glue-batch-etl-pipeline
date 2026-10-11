# ---------- 1. TOOLS ----------
import csv
import random
import sys
from datetime import datetime, timedelta

# ---------- 2. THE STORE ----------
CUSTOMERS = ["C001", "C002", "C003", "C004", "C005"]
PRODUCTS = {
    "Laptop": 899.99,
    "Phone": 599.99,
    "Headphones": 149.99,
    "Keyboard": 49.99,
    "Monitor": 229.99,
    "Mouse": 24.99,
}
STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]


# ---------- 3. RECIPE: CREATE ONE DAY OF ORDERS ----------
def generate_orders(run_date, num_rows):
    start = datetime.strptime(run_date, "%Y-%m-%d")          # midnight of the chosen day
    orders = []
    for i in range(1, num_rows + 1):
        product = random.choice(list(PRODUCTS))
        quantity = random.randint(1, 5)
        unit_price = PRODUCTS[product]
        order_ts = start + timedelta(seconds=random.randint(0, 86399))   # random time that day
        order = {
            "order_id": f"{run_date.replace('-', '')}-{i:05d}",          # e.g. 20261012-00001
            "customer_id": random.choice(CUSTOMERS),
            "product": product,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": round(quantity * unit_price, 2),
            "order_status": random.choice(STATUSES),
            "order_date": order_ts.strftime("%Y-%m-%d %H:%M:%S"),
        }

        # ---------- plant messy data ----------
        if random.random() < 0.03:
            order["customer_id"] = ""                                  # missing customer
        if random.random() < 0.02:
            order["quantity"] = -1                                     # impossible quantity
        if random.random() < 0.30:
            order["order_status"] = " " + order["order_status"].lower() + " "   # messy status

        orders.append(order)

    orders += random.sample(orders, 5)                                 # 5 duplicate orders
    return orders


# ---------- 4. RECIPE: SAVE AS CSV ----------
def write_csv(orders, path):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=orders[0].keys())
        writer.writeheader()
        writer.writerows(orders)


# ---------- 5. START HERE ----------
if __name__ == "__main__":
    run_date = sys.argv[1]
    num_rows = int(sys.argv[2]) if len(sys.argv) > 2 else 500
    output_file = f"orders_{run_date}.csv"

    orders = generate_orders(run_date, num_rows)
    write_csv(orders, output_file)
    print(f"Wrote {len(orders)} orders to {output_file}")