"""Generate CDC events for task 08 by changing the MySQL source.

Applies one INSERT, one UPDATE and one DELETE to classicmodels.customers.
Debezium captures these from the binlog and streams them to Kafka; running
cdc_pipeline.py again then incrementally MERGEs them into the Delta table.

Usage:
    uv run python src/data_lake/08_cdc/make_changes.py
"""

import logging

import pymysql

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("changes")

# A high customerNumber that is not present in the original dataset, so the
# INSERT/DELETE are unambiguous across re-runs.
NEW_CUSTOMER = 9001


def apply_changes() -> None:
    conn = pymysql.connect(
        host="localhost",
        port=3307,
        user="root",
        password="debezium",
        database="classicmodels",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            # Clean slate for repeatable demos.
            cur.execute("DELETE FROM customers WHERE customerNumber = %s", (NEW_CUSTOMER,))

            # INSERT: a brand-new customer.
            cur.execute(
                "INSERT INTO customers "
                "(customerNumber, customerName, contactLastName, contactFirstName, "
                " phone, addressLine1, city, country, creditLimit) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (NEW_CUSTOMER, "Delta Lake Traders", "Doe", "Jane",
                 "555-0100", "1 Lakehouse Way", "Berlin", "Germany", 90000.00),
            )
            log.info(f"INSERT customer {NEW_CUSTOMER}")

            # UPDATE: change an existing customer's credit limit.
            cur.execute(
                "UPDATE customers SET creditLimit = 999999.00 WHERE customerNumber = 103"
            )
            log.info("UPDATE customer 103 creditLimit -> 999999.00")

            # DELETE: remove an existing customer (125 has no orders/payments,
            # so the delete is not blocked by a foreign key).
            cur.execute("DELETE FROM customers WHERE customerNumber = 125")
            log.info("DELETE customer 125")
    finally:
        conn.close()

    log.info("Changes applied. Re-run cdc_pipeline.py to load them incrementally.")


if __name__ == "__main__":
    apply_changes()
