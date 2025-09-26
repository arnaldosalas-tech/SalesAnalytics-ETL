# SalesAnalytics-ETL

ETL en Python que lee `customers.csv`, `products.csv`, `orders.csv`, `order_details.csv`,
hace limpieza/validación mínima, carga a staging y hace MERGE a SQL Server (Customer,
Product, Orders, OrderDetail). Idempotente y con rechazos guardados en `data/rejects/`.

## Ejecutar
- Configura variables: CSV_PATH, MSSQL_DB, MSSQL_HOST/PORT (o MSSQL_SERVER), y (opcional) MSSQL_UID/MSSQL_PWD.
- `py -3.12 .\etl_sales_analytics.py`

Requisitos

Python 3.12 (o 3.10+)

Paquetes: pandas, sqlalchemy, pyodbc

ODBC Driver 17 o 18 para SQL Server

SQL Server (local) con DB SalesAnalytics (el script crea tablas si no existen)

requirements.txt:

pandas
SQLAlchemy>=2
pyodbc

Configuración (variables de entorno)

cd "C:\Users\Arnaldo\OneDrive\Desktop\Sistema de Análisis de Ventas"
$env:CSV_PATH = "$PWD\data"
$env:MSSQL_DB = "SalesAnalytics"

Verificación rápida en SQL Server

USE SalesAnalytics;
SELECT 'Customer' t, COUNT(*) c FROM dbo.Customer
UNION ALL SELECT 'Product', COUNT(*) FROM dbo.Product
UNION ALL SELECT 'Orders', COUNT(*) FROM dbo.Orders
UNION ALL SELECT 'OrderDetail', COUNT(*) FROM dbo.OrderDetail;

SELECT TOP (10) * FROM dbo.Customer ORDER BY CustomerID DESC;
SELECT TOP (10) * FROM dbo.Product ORDER BY ProductID DESC;
SELECT TOP (10) * FROM dbo.Orders ORDER BY OrderID DESC;
SELECT TOP (10) * FROM dbo.OrderDetail ORDER BY OrderID DESC;

SELECT TOP (20)
  CONVERT(date, o.OrderDate) AS [Date],
  SUM(od.TotalPrice) AS TotalSales
FROM dbo.OrderDetail od
JOIN dbo.Orders o ON o.OrderID = od.OrderID
GROUP BY CONVERT(date, o.OrderDate)
ORDER BY [Date] DESC;

$env:MSSQL_HOST = "127.0.0.1"
$env:MSSQL_PORT = "1433"

# SQL Auth (recomendado en local)
$env:MSSQL_UID = "etlArnaldo"
$env:MSSQL_PWD = "TuPasswordFuerte"
