import os
import sys
from datetime import datetime
from typing import Any, Dict, Iterable, List

import psycopg2
from psycopg2.extras import execute_values
import requests

BASE_URL = "https://fakestoreapi.com"


def get_conn():
    """Create a database connection using PG_CONN_STR with SSL required."""
    conn_str = os.getenv("PG_CONN_STR")
    if not conn_str:
        raise RuntimeError("PG_CONN_STR environment variable is not set.")
    return psycopg2.connect(conn_str, sslmode="require")


def fetch_json(path: str) -> Any:
    """Fetch JSON from the FakeStore API."""
    url = f"{BASE_URL}/{path.lstrip('/')}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, (list, dict)):
        raise ValueError(f"Unexpected response shape for {url}")
    return data


def upsert_products(conn) -> None:
    print("Fetching products...")
    products: List[Dict[str, Any]] = fetch_json("products")
    rows: List[Iterable[Any]] = []
    for product in products:
        rating = product.get("rating") or {}
        rows.append(
            (
                product.get("id"),
                product.get("title"),
                product.get("price"),
                product.get("description"),
                product.get("category"),
                product.get("image"),
                rating.get("rate"),
                rating.get("count"),
            )
        )

    if not rows:
        print("No products fetched; skipping upsert.")
        return

    print(f"Upserting {len(rows)} products...")
    sql = """
        INSERT INTO products (
            id,
            title,
            price,
            description,
            category,
            image,
            rating_rate,
            rating_count,
            updated_at
        )
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            title = EXCLUDED.title,
            price = EXCLUDED.price,
            description = EXCLUDED.description,
            category = EXCLUDED.category,
            image = EXCLUDED.image,
            rating_rate = EXCLUDED.rating_rate,
            rating_count = EXCLUDED.rating_count,
            updated_at = now()
    """

    with conn.cursor() as cur:
        execute_values(
            cur,
            sql,
            rows,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,now())",
        )
    conn.commit()
    print("Products upserted.")


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def upsert_customers(conn) -> None:
    print("Fetching customers...")
    users: List[Dict[str, Any]] = fetch_json("users")
    rows: List[Iterable[Any]] = []
    for user in users:
        address = user.get("address") or {}
        geolocation = address.get("geolocation") or {}
        name = user.get("name") or {}
        rows.append(
            (
                user.get("id"),
                user.get("email"),
                user.get("username"),
                name.get("firstname"),
                name.get("lastname"),
                address.get("city"),
                address.get("street"),
                address.get("number"),
                address.get("zipcode"),
                _safe_float(geolocation.get("lat")),
                _safe_float(geolocation.get("long")),
                user.get("phone"),
            )
        )

    if not rows:
        print("No customers fetched; skipping upsert.")
        return

    print(f"Upserting {len(rows)} customers...")
    sql = """
        INSERT INTO customers (
            id,
            email,
            username,
            first_name,
            last_name,
            city,
            street,
            street_number,
            zipcode,
            lat,
            lng,
            phone,
            updated_at
        )
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            email = EXCLUDED.email,
            username = EXCLUDED.username,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            city = EXCLUDED.city,
            street = EXCLUDED.street,
            street_number = EXCLUDED.street_number,
            zipcode = EXCLUDED.zipcode,
            lat = EXCLUDED.lat,
            lng = EXCLUDED.lng,
            phone = EXCLUDED.phone,
            updated_at = now()
    """

    with conn.cursor() as cur:
        execute_values(
            cur,
            sql,
            rows,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())",
        )
    conn.commit()
    print("Customers upserted.")


def _parse_cart_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def upsert_carts_and_items(conn) -> None:
    print("Fetching carts...")
    carts: List[Dict[str, Any]] = fetch_json("carts")
    cart_rows: List[Iterable[Any]] = []
    item_rows: List[Iterable[Any]] = []

    for cart in carts:
        cart_id = cart.get("id")
        cart_rows.append(
            (
                cart_id,
                cart.get("userId"),
                _parse_cart_date(cart.get("date")),
            )
        )
        for item in cart.get("products") or []:
            item_rows.append(
                (
                    cart_id,
                    item.get("productId"),
                    item.get("quantity"),
                )
            )

    with conn.cursor() as cur:
        if cart_rows:
            print(f"Upserting {len(cart_rows)} carts...")
            sql = """
                INSERT INTO carts (
                    id,
                    customer_id,
                    cart_date,
                    updated_at
                )
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    cart_date = EXCLUDED.cart_date,
                    updated_at = now()
            """
            execute_values(
                cur,
                sql,
                cart_rows,
                template="(%s,%s,%s,now())",
            )
        else:
            print("No carts fetched; skipping cart upsert.")

        print("Refreshing cart_items table...")
        cur.execute("TRUNCATE TABLE cart_items")
        if item_rows:
            sql_items = """
                INSERT INTO cart_items (
                    cart_id,
                    product_id,
                    quantity,
                    updated_at
                )
                VALUES %s
            """
            execute_values(
                cur,
                sql_items,
                item_rows,
                template="(%s,%s,%s,now())",
            )
            print(f"Inserted {len(item_rows)} cart items.")
        else:
            print("No cart items found; cart_items table left empty.")

    conn.commit()
    print("Carts and cart items refreshed.")


def main() -> None:
    print("Starting FakeStore ETL run...")
    try:
        conn = get_conn()
    except Exception as err:
        print(f"Failed to connect to database: {err}")
        sys.exit(1)

    try:
        upsert_products(conn)
        upsert_customers(conn)
        upsert_carts_and_items(conn)
    except Exception as err:
        print(f"ETL failed: {err}")
        conn.rollback()
        conn.close()
        sys.exit(1)

    conn.close()
    print("ETL run completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Unexpected error: {exc}")
        sys.exit(1)
