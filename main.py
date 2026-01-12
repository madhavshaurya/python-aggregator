from fastapi import FastAPI, Request, Header, HTTPException
from collections import defaultdict
import os

app = FastAPI(
    title="Sales Aggregation Service",
    version="1.0.0"
)

API_KEY = os.getenv("API_KEY")


def parse_price(value):
    """
    Handles MongoDB Decimal128 and normal numeric values safely.
    """
    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, dict) and "$numberDecimal" in value:
        try:
            return float(value["$numberDecimal"])
        except (TypeError, ValueError):
            return 0.0

    return 0.0


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/api/aggregate-invoices")
async def aggregate_sales(
    request: Request,
    x_api_key: str = Header(None)
):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    sales = await request.json()

    if not isinstance(sales, list):
        raise HTTPException(
            status_code=400,
            detail="Expected a list of sales documents"
        )

    invoice_count = 0
    total_sales = 0.0
    online_sales = 0.0
    in_store_sales = 0.0
    coupon_orders = 0

    item_revenue = defaultdict(float)
    store_revenue = defaultdict(float)

    for sale in sales:
        if not sale or "items" not in sale:
            continue

        sale_total = 0.0

        for item in sale.get("items", []):
            price = parse_price(item.get("price"))
            try:
                quantity = int(item.get("quantity", 0))
            except (TypeError, ValueError):
                quantity = 0

            revenue = price * quantity
            sale_total += revenue
            item_revenue[item.get("name", "unknown")] += revenue

        if sale_total <= 0:
            continue

        invoice_count += 1
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

    top_store = (
        max(store_revenue, key=store_revenue.get)
        if store_revenue else "data not available"
    )

    return {
        "invoice_count": invoice_count,
        "total_sales": round(total_sales, 2),
        "average_invoice_value": (
            round(total_sales / invoice_count, 2)
            if invoice_count > 0 else 0
        ),
        "online_sales": round(online_sales, 2),
        "in_store_sales": round(in_store_sales, 2),
        "coupon_orders": coupon_orders,
        "top_store": top_store,
        "top_items": [
            {"name": name, "revenue": round(revenue, 2)}
            for name, revenue in top_items
        ]
    }
