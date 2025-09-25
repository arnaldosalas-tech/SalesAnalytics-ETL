# ETL Pipeline for SalesAnalytics
# Requires: pandas, sqlalchemy, pyodbc
# pip install pandas sqlalchemy pyodbc
#
# 1) Ensure the database is created by executing the provided SQL schema.
# 2) Place your CSV files in a folder (see CSV_PATH below).
# 3) Update the connection string for your SQL Server.
# 4) Run: python etl_sales_analytics.py

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# --- Configuration ---
# --- Configuration ---
CSV_PATH = os.environ.get("CSV_PATH", "./data")
SERVER   = os.environ.get("MSSQL_SERVER", "localhost")
DATABASE = os.environ.get("MSSQL_DB", "SalesAnalytics")
DRIVER   = os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")  # 17 o 18
UID      = os.environ.get("MSSQL_UID", "")
PWD      = os.environ.get("MSSQL_PWD", "")

from urllib.parse import quote_plus
from sqlalchemy import create_engine

# Ruta de CSVs
CSV_PATH = os.environ.get(
    "CSV_PATH",
    r"C:\Users\Arnaldo\OneDrive\Desktop\Sistema de Análisis de Ventas\data"
)

# Conexión por URL (sin odbc_connect), más tolerante con instancias nombradas
SERVER_URL = "LAPTOP-O0EGI91O%5CSQLDEV1"  # LAPTOP-O0EGI91O\SQLDEV1 con \ -> %5C
DATABASE   = "SalesAnalytics"

conn_str = (
    f"mssql+pyodbc://@{SERVER_URL}/{DATABASE}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
    "&Encrypt=no"
    "&TrustServerCertificate=yes"
)

engine = create_engine(conn_str, fast_executemany=True)





# --- Helper functions ---
def read_csv_safe(filename, dtype=None):
    path = os.path.join(CSV_PATH, filename)
    if not os.path.exists(path):
        print(f"[WARN] Missing CSV: {path}")
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=dtype, encoding="utf-8", keep_default_na=True, na_values=["", "NULL", "NaN", None])
    return df

def clean_str(x):
    if pd.isna(x): return None
    s = str(x).strip()
    return s if s else None

def clean_decimal(x):
    if pd.isna(x): return None
    try:
        if isinstance(x, str):
            x = x.replace(",", "").replace("$", "")
        return float(x)
    except Exception:
        return None

def parse_date(x):
    if pd.isna(x): return None
    try:
        return pd.to_datetime(x, errors="coerce")
    except Exception:
        return None

def drop_dupes_and_nas(df, subset_cols):
    if df.empty: return df
    df = df.drop_duplicates(subset=subset_cols)
    for c in subset_cols:
        df = df[~df[c].isna()]
    return df

def get_table_df(sql):
    with engine.connect() as con:
        return pd.read_sql(sql, con)

def insert_dataframe(df, table):
    if df.empty:
        return 0
    with engine.begin() as con:
        df.to_sql(table, con=con, schema="dbo", if_exists="append", index=False)
        return len(df)

# --- Load CSVs ---
products = read_csv_safe("products.csv")
customers = read_csv_safe("customers.csv")
sales     = read_csv_safe("sales.csv")             # expects header & lines
surveys   = read_csv_safe("surveys.csv")
comments  = read_csv_safe("social_comments.csv")
reviews   = read_csv_safe("web_reviews.csv")
sources   = read_csv_safe("sources.csv")

# --- Cleaning & Normalization ---
# Products
if not products.empty:
    products = products.rename(columns={
        "sku": "Sku", "name": "ProductName", "category": "Category",
        "subcategory": "Subcategory", "brand": "Brand", "price": "UnitPrice",
        "currency": "Currency"
    })
    for col in ["Sku","ProductName","Category","Subcategory","Brand","Currency"]:
        if col in products.columns: products[col] = products[col].map(clean_str)
    if "UnitPrice" in products.columns:
        products["UnitPrice"] = products["UnitPrice"].map(clean_decimal)
    products = drop_dupes_and_nas(products, ["Sku"])[["Sku","ProductName","Category","Subcategory","Brand","UnitPrice","Currency"]]

# Customers
if not customers.empty:
    customers = customers.rename(columns={
        "code": "ExternalCustomerCode", "fullname": "FullName",
        "email": "Email", "phone": "Phone", "country": "Country"
    })
    for col in ["ExternalCustomerCode","FullName","Email","Phone","Country"]:
        if col in customers.columns: customers[col] = customers[col].map(clean_str)
    customers = drop_dupes_and_nas(customers, ["Email"])[["ExternalCustomerCode","FullName","Email","Phone","Country"]]

# Sources
if not sources.empty:
    sources = sources.rename(columns={"name":"SourceName","type":"SourceType","url":"Url"})
    for col in ["SourceName","SourceType","Url"]:
        if col in sources.columns: sources[col] = sources[col].map(clean_str)
    sources = drop_dupes_and_nas(sources, ["SourceName"])[["SourceName","SourceType","Url"]]

# Sales (requires mapping to invoices & products)
if not sales.empty:
    sales = sales.rename(columns={
        "invoice_number":"InvoiceNumber","invoice_date":"InvoiceDate",
        "customer_email":"Email","sku":"Sku","qty":"Quantity",
        "unit_price":"UnitPrice","discount_pct":"DiscountPct","tax_pct":"TaxPct",
        "currency":"Currency","source_name":"SourceName"
    })
    # normalize
    for col in ["InvoiceNumber","Email","Sku","Currency","SourceName"]:
        if col in sales.columns: sales[col] = sales[col].map(clean_str)
    for col in ["Quantity","UnitPrice","DiscountPct","TaxPct"]:
        if col in sales.columns: sales[col] = sales[col].map(clean_decimal)
    if "InvoiceDate" in sales.columns:
        sales["InvoiceDate"] = sales["InvoiceDate"].map(parse_date)
    # compute line total
    if not sales.empty:
        sales["DiscountPct"] = sales["DiscountPct"].fillna(0.0)
        sales["TaxPct"] = sales["TaxPct"].fillna(0.0)
        net_price = sales["UnitPrice"] * (1 - sales["DiscountPct"]/100.0)
        sales["LineTotal"] = sales["Quantity"] * net_price * (1 + sales["TaxPct"]/100.0)

# Surveys
if not surveys.empty:
    surveys = surveys.rename(columns={
        "source_name":"SourceName","customer_email":"Email","sku":"Sku",
        "date":"SurveyDate","rating":"Rating","comment":"Comment","external_id":"RawExternalID"
    })
    for col in ["SourceName","Email","Sku","Comment","RawExternalID"]:
        if col in surveys.columns: surveys[col] = surveys[col].map(clean_str)
    if "SurveyDate" in surveys.columns:
        surveys["SurveyDate"] = surveys["SurveyDate"].map(parse_date)
    if "Rating" in surveys.columns:
        surveys["Rating"] = pd.to_numeric(surveys["Rating"], errors="coerce").clip(1,5)

# Social comments
if not comments.empty:
    comments = comments.rename(columns={
        "source_name":"SourceName","platform":"Platform","handle":"Handle",
        "customer_email":"Email","sku":"Sku","date":"CommentDate","text":"Text",
        "sentiment":"SentimentScore","url":"Url"
    })
    for col in ["SourceName","Platform","Handle","Email","Sku","Text","Url"]:
        if col in comments.columns: comments[col] = comments[col].map(clean_str)
    if "CommentDate" in comments.columns:
        comments["CommentDate"] = comments["CommentDate"].map(parse_date)
    if "SentimentScore" in comments.columns:
        comments["SentimentScore"] = pd.to_numeric(comments["SentimentScore"], errors="coerce")

# Web reviews
if not reviews.empty:
    reviews = reviews.rename(columns={
        "source_name":"SourceName","customer_email":"Email","sku":"Sku",
        "date":"ReviewDate","rating":"Rating","title":"Title","body":"Body","url":"Url"
    })
    for col in ["SourceName","Email","Sku","Title","Body","Url"]:
        if col in reviews.columns: reviews[col] = reviews[col].map(clean_str)
    if "ReviewDate" in reviews.columns:
        reviews["ReviewDate"] = reviews["ReviewDate"].map(parse_date)
    if "Rating" in reviews.columns:
        reviews["Rating"] = pd.to_numeric(reviews["Rating"], errors="coerce").clip(1,5)

# --- Load Dimensions (de-dup by checking existing) ---
def load_dim(table, key_col, df_cols, df):
    if df.empty: 
        print(f"[SKIP] {table}: no rows")
        return 0
    existing = get_table_df(f"SELECT {key_col} FROM dbo.{table}")
    existing_set = set(existing[key_col].astype(str).str.lower().tolist())
    df2 = df.copy()
    df2[key_col] = df2[key_col].astype(str).str.lower()
    new_rows = df2[~df2[key_col].isin(existing_set)].copy()
    # restore original case by merging
    new_rows = new_rows.merge(df[[key_col] + [c for c in df_cols if c != key_col]], on=key_col, how="left", suffixes=("",""))
    new_rows = new_rows[df_cols].drop_duplicates(subset=[key_col])
    inserted = insert_dataframe(new_rows, table)
    print(f"[INFO] {table}: inserted {inserted} new rows (existing {len(existing_set)})")
    return inserted

ins = 0
ins += load_dim("DataSource", "SourceName", ["SourceName","SourceType","Url"], sources) if not sources.empty else 0
ins += load_dim("Customer", "Email", ["ExternalCustomerCode","FullName","Email","Phone","Country"], customers) if not customers.empty else 0
ins += load_dim("Product", "Sku", ["Sku","ProductName","Category","Subcategory","Brand","UnitPrice","Currency"], products) if not products.empty else 0

# --- Helper: get surrogate keys ---
def dim_map(table, natural_key_col, surrogate_key_col):
    df = get_table_df(f"SELECT {surrogate_key_col},{natural_key_col} FROM dbo.{table}")
    df[natural_key_col] = df[natural_key_col].astype(str).str.lower()
    return df

src_map = dim_map("DataSource", "SourceName", "SourceID")
cust_map = dim_map("Customer", "Email", "CustomerID")
prod_map = dim_map("Product", "Sku", "ProductID")

# --- Build/Load Invoice + Sale ---
inserted_invoices = 0
inserted_sales = 0
if not sales.empty:
    s = sales.copy()
    # Join surrogate keys
    for col, map_df in [("SourceName", src_map), ("Email", cust_map), ("Sku", prod_map)]:
        s[col] = s[col].astype(str).str.lower()
    s = s.merge(src_map, on="SourceName", how="left") \
         .merge(cust_map, on="Email", how="left") \
         .merge(prod_map, on="Sku", how="left")
    s = s.dropna(subset=["SourceID","CustomerID","ProductID","InvoiceNumber","InvoiceDate"])

    # Build invoice header aggregation
    inv = s.groupby(["InvoiceNumber","InvoiceDate","SourceID","CustomerID","Currency"], dropna=False).agg(
        Subtotal=("LineTotal", "sum")  # We'll treat LineTotal as final amount; adjust if needed
    ).reset_index()
    inv["TaxAmount"] = None
    inv["ShippingAmount"] = None
    inv["DiscountAmount"] = None
    inv["TotalAmount"] = inv["Subtotal"]

    # Insert only new invoices (unique by InvoiceNumber)
    existing_inv = get_table_df("SELECT InvoiceID, InvoiceNumber FROM dbo.Invoice")
    existing_inv_set = set(existing_inv["InvoiceNumber"].astype(str).str.lower().tolist())
    inv2 = inv.copy()
    inv2["InvoiceNumber"] = inv2["InvoiceNumber"].astype(str).str.lower()
    new_invoices = inv2[~inv2["InvoiceNumber"].isin(existing_inv_set)].copy()
    # restore original case
    new_invoices = new_invoices.merge(inv, on=["InvoiceNumber","InvoiceDate","SourceID","CustomerID","Currency","Subtotal","TaxAmount","ShippingAmount","DiscountAmount","TotalAmount"], how="left")
    inserted_invoices = insert_dataframe(new_invoices[[
        "InvoiceNumber","InvoiceDate","SourceID","CustomerID","Subtotal","TaxAmount","ShippingAmount","DiscountAmount","TotalAmount","Currency"
    ]], "Invoice")

    # Re-read invoices to get IDs
    inv_map = get_table_df("SELECT InvoiceID, InvoiceNumber FROM dbo.Invoice")
    inv_map["InvoiceNumber"] = inv_map["InvoiceNumber"].astype(str).str.lower()

    # Build sale lines
    s["InvoiceNumber"] = s["InvoiceNumber"].astype(str).str.lower()
    s = s.merge(inv_map, on="InvoiceNumber", how="left")
    sale_lines = s[[
        "InvoiceID","ProductID","Quantity","UnitPrice","DiscountPct","TaxPct","LineTotal"
    ]].dropna(subset=["InvoiceID","ProductID"])

    inserted_sales = insert_dataframe(sale_lines, "Sale")

# --- Load Surveys ---
inserted_surveys = 0
if not surveys.empty:
    t = surveys.copy()
    for col, map_df in [("SourceName", src_map), ("Email", cust_map), ("Sku", prod_map)]:
        if col in t.columns:
            t[col] = t[col].astype(str).str.lower()
    t = t.merge(src_map, on="SourceName", how="left") \
         .merge(cust_map, on="Email", how="left") \
         .merge(prod_map, on="Sku", how="left")
    t = t.dropna(subset=["SourceID","SurveyDate"])
    t = t.rename(columns={"CustomerID":"CustomerID","ProductID":"ProductID"})
    inserted_surveys = insert_dataframe(t[[
        "SourceID","CustomerID","ProductID","SurveyDate","Rating","Comment","RawExternalID"
    ]], "Survey")

# --- Load Social Comments ---
inserted_comments = 0
if not comments.empty:
    t = comments.copy()
    for col, map_df in [("SourceName", src_map), ("Email", cust_map), ("Sku", prod_map)]:
        if col in t.columns:
            t[col] = t[col].astype(str).str.lower()
    t = t.merge(src_map, on="SourceName", how="left") \
         .merge(cust_map, on="Email", how="left") \
         .merge(prod_map, on="Sku", how="left")
    t = t.dropna(subset=["SourceID","CommentDate","Text"])
    inserted_comments = insert_dataframe(t[[
        "SourceID","Platform","Handle","CustomerID","ProductID","CommentDate","Text","SentimentScore","Url"
    ]], "SocialComment")

# --- Load Web Reviews ---
inserted_reviews = 0
if not reviews.empty:
    t = reviews.copy()
    for col, map_df in [("SourceName", src_map), ("Email", cust_map), ("Sku", prod_map)]:
        if col in t.columns:
            t[col] = t[col].astype(str).str.lower()
    t = t.merge(src_map, on="SourceName", how="left") \
         .merge(cust_map, on="Email", how="left") \
         .merge(prod_map, on="Sku", how="left")
    t = t.dropna(subset=["SourceID","ProductID","ReviewDate"])
    inserted_reviews = insert_dataframe(t[[
        "SourceID","CustomerID","ProductID","ReviewDate","Rating","Title","Body","Url"
    ]], "WebReview")

# --- Summary ---
with engine.connect() as con:
    tables = ["DataSource","Customer","Product","Invoice","Sale","Survey","SocialComment","WebReview"]
    print("\nRow counts:")
    for tname in tables:
        try:
            cnt = con.execute(text(f"SELECT COUNT(*) FROM dbo.{tname}")).scalar()
            print(f"  {tname}: {cnt}")
        except Exception as e:
            print(f"  {tname}: (err: {e})")

print("\nInserted rows ->",
      f"DataSource: {ins}, Invoices: {inserted_invoices}, Sales: {inserted_sales},",
      f"Surveys: {inserted_surveys}, Comments: {inserted_comments}, Reviews: {inserted_reviews}")
