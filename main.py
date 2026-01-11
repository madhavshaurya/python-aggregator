from fastapi import FastAPI, Request
from collections import defaultdict
from datetime import datetime

app = FastAPI(
    title="Sales Aggregation Service",
    version="1.0.0"
)

@app.post("/api/aggregate-invoices")
async def aggregate_sales(request: Request):
    sales = await request.json()

    invoice_count = len(sales)
    total_sales = 0.0
    online_sales = 0.0
    in_store_sales = 0.0
    coupon_orders = 0

    item_revenue = defaultdict(float)
    store_revenue = defaultdict(float)

    for sale in sales:
        sale_total = 0.0

        for item in sale.get("items", []):
            price = float(item.get("price", 0))
            quantity = int(item.get("quantity", 0))
            revenue = price * quantity

            sale_total += revenue
            item_revenue[item.get("name", "unknown")] += revenue

        total_sales += sale_total
        store_revenue[sale.get("storeLocation", "unknown")] += sale_total

        if sale.get("purchaseMethod") == "Online":
            online_sales += sale_total
        else:
            in_store_sales += sale_total

        if sale.get("couponUsed"):
            coupon_orders += 1

    top_items = sorted(
        item_revenue.items(),
        key=lambda x: x[1],
        reverse=True
    )[:3]

    top_store = max(store_revenue, key=store_revenue.get) if store_revenue else "data not available"

    return {
        "invoice_count": invoice_count,
        "total_sales": round(total_sales, 2),
        "average_invoice_value": round(total_sales / invoice_count, 2) if invoice_count else 0,
        "online_sales": round(online_sales, 2),
        "in_store_sales": round(in_store_sales, 2),
        "coupon_orders": coupon_orders,
        "top_store": top_store,
        "top_items": [
            {"name": name, "revenue": round(revenue, 2)}
            for name, revenue in top_items
        ]
    }
