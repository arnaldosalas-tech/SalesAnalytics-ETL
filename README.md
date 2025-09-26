# SalesAnalytics-ETL

ETL en Python que lee `customers.csv`, `products.csv`, `orders.csv`, `order_details.csv`,
hace limpieza/validación mínima, carga a staging y hace MERGE a SQL Server (Customer,
Product, Orders, OrderDetail). Idempotente y con rechazos guardados en `data/rejects/`.

## Ejecutar
- Configura variables: CSV_PATH, MSSQL_DB, MSSQL_HOST/PORT (o MSSQL_SERVER), y (opcional) MSSQL_UID/MSSQL_PWD.
- `py -3.12 .\etl_sales_analytics.py`
