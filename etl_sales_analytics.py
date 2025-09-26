# etl_sales_analytics.py
# pip install pandas sqlalchemy pyodbc

import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ========= Configuración =========
CSV_PATH = os.environ.get("CSV_PATH", "./data")
DB       = os.environ.get("MSSQL_DB", "SalesAnalytics")
DRIVER   = os.environ.get("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")

# Conexión: si defines MSSQL_PORT usa host/puerto; si no, instancia nombrada
HOST   = os.environ.get("MSSQL_HOST", "127.0.0.1")
PORT   = os.environ.get("MSSQL_PORT", "1433")  # deja "1433" si fijaste TCP
SERVER = os.environ.get("MSSQL_SERVER", r"LAPTOP-O0EGI91O\SQLDEV1")  # solo si usas instancia y SQL Browser

# Soporte de credenciales SQL
UID = os.environ.get("MSSQL_UID", "")
PWD = os.environ.get("MSSQL_PWD", "")

def build_conn_str():
    drv = DRIVER.replace(" ", "+")
    if UID:  # SQL Login
        u = quote_plus(UID)
        p = quote_plus(PWD or "")
        if PORT:
            return f"mssql+pyodbc://{u}:{p}@{HOST},{PORT}/{DB}?driver={drv}&Encrypt=no&TrustServerCertificate=yes&Trusted_Connection=no"
        else:
            server_url = SERVER.replace("\\", "%5C")
            return f"mssql+pyodbc://{u}:{p}@{server_url}/{DB}?driver={drv}&Encrypt=no&TrustServerCertificate=yes&Trusted_Connection=no"
    else:    # Windows Integrated
        if PORT:
            return f"mssql+pyodbc://@{HOST},{PORT}/{DB}?driver={drv}&trusted_connection=yes&Encrypt=no&TrustServerCertificate=yes"
        else:
            server_url = SERVER.replace("\\", "%5C")
            return f"mssql+pyodbc://@{server_url}/{DB}?driver={drv}&trusted_connection=yes&Encrypt=no&TrustServerCertificate=yes"

engine = create_engine(build_conn_str(), fast_executemany=True)

# ========= Utilidades =========
def path_csv(name: str) -> str:
    return os.path.join(CSV_PATH, name)

def load_csv_exact(name: str, required_cols: list[str]) -> pd.DataFrame:
    p = path_csv(name)
    if not os.path.exists(p):
        print(f"[WARN] Falta CSV: {p}")
        return pd.DataFrame(columns=required_cols)
    df = pd.read_csv(p, encoding="utf-8")
    # conservar SOLO las columnas pedidas (rellenar faltantes con NA)
    missing = [c for c in required_cols if c not in df.columns]
    for m in missing:
        df[m] = pd.NA
    df = df[required_cols].copy()
    # trim strings
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype("string").str.strip()
    return df

# ========= DDL opcional (solo si MSSQL_CREATE_DDL=1) =========
def ensure_tables():
    ddl = """
IF OBJECT_ID('dbo.Customer','U') IS NULL
BEGIN
  CREATE TABLE dbo.Customer(
    CustomerID  INT NOT NULL PRIMARY KEY,
    FirstName   NVARCHAR(100) NULL,
    LastName    NVARCHAR(100) NULL,
    Email       NVARCHAR(255) NULL,
    Phone       NVARCHAR(50)  NULL,
    City        NVARCHAR(100) NULL,
    Country     NVARCHAR(100) NULL
  );
END;

IF OBJECT_ID('dbo.Product','U') IS NULL
BEGIN
  CREATE TABLE dbo.Product(
    ProductID    INT           NOT NULL PRIMARY KEY,
    ProductName  NVARCHAR(200) NULL,
    Category     NVARCHAR(100) NULL,
    Price        DECIMAL(18,2) NULL,
    Stock        INT           NULL
  );
END;

IF OBJECT_ID('dbo.Orders','U') IS NULL
BEGIN
  CREATE TABLE dbo.Orders(
    OrderID    INT           NOT NULL PRIMARY KEY,
    CustomerID INT           NOT NULL,
    OrderDate  DATETIME2     NULL,
    Status     NVARCHAR(50)  NULL,
    CONSTRAINT FK_Orders_Customer FOREIGN KEY(CustomerID) REFERENCES dbo.Customer(CustomerID)
  );
END;

IF OBJECT_ID('dbo.OrderDetail','U') IS NULL
BEGIN
  CREATE TABLE dbo.OrderDetail(
    OrderID    INT           NOT NULL,
    ProductID  INT           NOT NULL,
    Quantity   INT           NULL,
    TotalPrice DECIMAL(18,2) NULL,
    CONSTRAINT PK_OrderDetail PRIMARY KEY(OrderID, ProductID),
    CONSTRAINT FK_OD_Orders  FOREIGN KEY(OrderID)  REFERENCES dbo.Orders(OrderID),
    CONSTRAINT FK_OD_Product FOREIGN KEY(ProductID) REFERENCES dbo.Product(ProductID)
  );
END;
"""
    with engine.begin() as con:
        con.execute(text(ddl))
    print("[INFO] Tablas destino listas (DDL ejecutado).")

def recreate_stage_tables():
    ddl = """
IF OBJECT_ID('dbo.Stage_Customer','U') IS NOT NULL DROP TABLE dbo.Stage_Customer;
IF OBJECT_ID('dbo.Stage_Product','U')  IS NOT NULL DROP TABLE dbo.Stage_Product;
IF OBJECT_ID('dbo.Stage_Orders','U')   IS NOT NULL DROP TABLE dbo.Stage_Orders;
IF OBJECT_ID('dbo.Stage_OrderDetail','U') IS NOT NULL DROP TABLE dbo.Stage_OrderDetail;

CREATE TABLE dbo.Stage_Customer(
  CustomerID  INT NOT NULL,
  FirstName   NVARCHAR(100) NULL,
  LastName    NVARCHAR(100) NULL,
  Email       NVARCHAR(255) NULL,
  Phone       NVARCHAR(50)  NULL,
  City        NVARCHAR(100) NULL,
  Country     NVARCHAR(100) NULL
);

CREATE TABLE dbo.Stage_Product(
  ProductID    INT           NOT NULL,
  ProductName  NVARCHAR(200) NULL,
  Category     NVARCHAR(100) NULL,
  Price        DECIMAL(18,2) NULL,
  Stock        INT           NULL
);

CREATE TABLE dbo.Stage_Orders(
  OrderID    INT          NOT NULL,
  CustomerID INT          NOT NULL,
  OrderDate  DATETIME2    NULL,
  Status     NVARCHAR(50) NULL
);

CREATE TABLE dbo.Stage_OrderDetail(
  OrderID    INT           NOT NULL,
  ProductID  INT           NOT NULL,
  Quantity   INT           NULL,
  TotalPrice DECIMAL(18,2) NULL
);
"""
    with engine.begin() as con:
        con.execute(text(ddl))
    print("[INFO] Tablas staging recreadas.")

def to_int(df: pd.DataFrame, cols: list[str]):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

def to_decimal(df: pd.DataFrame, cols: list[str]):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def transform_minimal(customers, products, orders, details):
    # Casts
    to_int(customers, ["CustomerID"])
    to_int(products,  ["ProductID","Stock"])
    to_decimal(products, ["Price"])
    to_int(orders,    ["OrderID","CustomerID"])
    if "OrderDate" in orders.columns:
        orders["OrderDate"] = pd.to_datetime(orders["OrderDate"], errors="coerce")
    to_int(details,   ["OrderID","ProductID","Quantity"])
    to_decimal(details, ["TotalPrice"])

    # Dedupe por PK natural
    customers = customers.dropna(subset=["CustomerID"]).drop_duplicates(subset=["CustomerID"], keep="last")
    products  = products.dropna(subset=["ProductID"]).drop_duplicates(subset=["ProductID"], keep="last")
    orders    = orders.dropna(subset=["OrderID","CustomerID"]).drop_duplicates(subset=["OrderID"], keep="last")
    details   = details.dropna(subset=["OrderID","ProductID"]).drop_duplicates(subset=["OrderID","ProductID"], keep="last")

    # Validación FK mínima (rechazos a /rejects)
    rejects_dir = os.path.join(CSV_PATH, "rejects")
    os.makedirs(rejects_dir, exist_ok=True)

    if not orders.empty and not customers.empty:
        valid_cust = set(customers["CustomerID"].dropna().astype(int).tolist())
        bad = ~orders["CustomerID"].isin(valid_cust)
        if bad.any():
            orders[bad].to_csv(os.path.join(rejects_dir, "orders_invalid_customer.csv"), index=False, encoding="utf-8")
            print(f"[WARN] Orders rechazados por FK(CustomerID): {bad.sum()} (rejects/orders_invalid_customer.csv)")
        orders = orders[~bad]

    if not details.empty:
        valid_orders = set(orders["OrderID"].dropna().astype(int).tolist()) if not orders.empty else set()
        valid_prods  = set(products["ProductID"].dropna().astype(int).tolist()) if not products.empty else set()
        mask = details["OrderID"].isin(valid_orders) & details["ProductID"].isin(valid_prods)
        if (~mask).any():
            details[~mask].to_csv(os.path.join(rejects_dir, "order_details_invalid_fk.csv"), index=False, encoding="utf-8")
            print(f"[WARN] OrderDetail rechazados por FK: {(~mask).sum()} (rejects/order_details_invalid_fk.csv)")
        details = details[mask]

    return customers, products, orders, details

def stage_load(customers, products, orders, details):
    with engine.begin() as con:
        if not customers.empty:
            customers.to_sql("Stage_Customer", con=con, schema="dbo", if_exists="append", index=False)
        if not products.empty:
            products.to_sql("Stage_Product",  con=con, schema="dbo", if_exists="append", index=False)
        if not orders.empty:
            orders.to_sql("Stage_Orders",     con=con, schema="dbo", if_exists="append", index=False)
        if not details.empty:
            details.to_sql("Stage_OrderDetail", con=con, schema="dbo", if_exists="append", index=False)
    print("[INFO] Cargas a staging listas.")

def merge_and_counts():
    with engine.begin() as con:
        # Cada MERGE no devuelve filas -> NO usar .all()
        r = con.execute(text("""
MERGE dbo.Customer AS T
USING dbo.Stage_Customer AS S
ON T.CustomerID = S.CustomerID
WHEN MATCHED THEN UPDATE SET
  T.FirstName = S.FirstName,
  T.LastName  = S.LastName,
  T.Email     = S.Email,
  T.Phone     = S.Phone,
  T.City      = S.City,
  T.Country   = S.Country
WHEN NOT MATCHED BY TARGET THEN
  INSERT(CustomerID,FirstName,LastName,Email,Phone,City,Country)
  VALUES(S.CustomerID,S.FirstName,S.LastName,S.Email,S.Phone,S.City,S.Country);
"""))
        print(f"[MERGE] Customer rows affected: {r.rowcount if r.rowcount is not None else 'N/A'}")

        r = con.execute(text("""
MERGE dbo.Product AS T
USING dbo.Stage_Product AS S
ON T.ProductID = S.ProductID
WHEN MATCHED THEN UPDATE SET
  T.ProductName = S.ProductName,
  T.Category    = S.Category,
  T.Price       = S.Price,
  T.Stock       = S.Stock
WHEN NOT MATCHED BY TARGET THEN
  INSERT(ProductID,ProductName,Category,Price,Stock)
  VALUES(S.ProductID,S.ProductName,S.Category,S.Price,S.Stock);
"""))
        print(f"[MERGE] Product rows affected: {r.rowcount if r.rowcount is not None else 'N/A'}")

        r = con.execute(text("""
MERGE dbo.Orders AS T
USING dbo.Stage_Orders AS S
ON T.OrderID = S.OrderID
WHEN MATCHED THEN UPDATE SET
  T.CustomerID = S.CustomerID,
  T.OrderDate  = S.OrderDate,
  T.Status     = S.Status
WHEN NOT MATCHED BY TARGET THEN
  INSERT(OrderID,CustomerID,OrderDate,Status)
  VALUES(S.OrderID,S.CustomerID,S.OrderDate,S.Status);
"""))
        print(f"[MERGE] Orders rows affected: {r.rowcount if r.rowcount is not None else 'N/A'}")

        r = con.execute(text("""
MERGE dbo.OrderDetail AS T
USING dbo.Stage_OrderDetail AS S
ON T.OrderID = S.OrderID AND T.ProductID = S.ProductID
WHEN MATCHED THEN UPDATE SET
  T.Quantity   = S.Quantity,
  T.TotalPrice = S.TotalPrice
WHEN NOT MATCHED BY TARGET THEN
  INSERT(OrderID,ProductID,Quantity,TotalPrice)
  VALUES(S.OrderID,S.ProductID,S.Quantity,S.TotalPrice);
"""))
        print(f"[MERGE] OrderDetail rows affected: {r.rowcount if r.rowcount is not None else 'N/A'}")

def drop_stage_tables():
    with engine.begin() as con:
        con.execute(text("""
IF OBJECT_ID('dbo.Stage_Customer','U') IS NOT NULL DROP TABLE dbo.Stage_Customer;
IF OBJECT_ID('dbo.Stage_Product','U')  IS NOT NULL DROP TABLE dbo.Stage_Product;
IF OBJECT_ID('dbo.Stage_Orders','U')   IS NOT NULL DROP TABLE dbo.Stage_Orders;
IF OBJECT_ID('dbo.Stage_OrderDetail','U') IS NOT NULL DROP TABLE dbo.Stage_OrderDetail;
"""))
    print("[INFO] Staging eliminado.")

def print_counts():
    with engine.begin() as con:
        for t in ["Customer","Product","Orders","OrderDetail"]:
            cnt = con.execute(text(f"SELECT COUNT(*) FROM dbo.{t}")).scalar()
            print(f"[COUNT] {t}: {cnt}")

# ========= Main =========
def main():
    print("[INFO] Iniciando ETL (Extract-Transform-Load)")

    # DDL solo si lo pides explícitamente
    if os.environ.get("MSSQL_CREATE_DDL", "0") == "1":
        ensure_tables()

    recreate_stage_tables()

    # EXTRACT (headers exactos)
    customers = load_csv_exact("customers.csv",
        ["CustomerID","FirstName","LastName","Email","Phone","City","Country"])
    products  = load_csv_exact("products.csv",
        ["ProductID","ProductName","Category","Price","Stock"])
    orders    = load_csv_exact("orders.csv",
        ["OrderID","CustomerID","OrderDate","Status"])
    details   = load_csv_exact("order_details.csv",
        ["OrderID","ProductID","Quantity","TotalPrice"])

    # TRANSFORM
    customers, products, orders, details = transform_minimal(customers, products, orders, details)

    # LOAD (staging -> merge)
    stage_load(customers, products, orders, details)
    merge_and_counts()
    drop_stage_tables()
    print_counts()

    print("[INFO] ETL finalizado correctamente.")

if __name__ == "__main__":
    main()
