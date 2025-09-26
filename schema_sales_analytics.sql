-- SalesAnalytics - Core Schema (Customer, Product, Orders, OrderDetail)
-- Target: Microsoft SQL Server (T-SQL)

IF DB_ID(N'SalesAnalytics') IS NULL
BEGIN
    EXEC('CREATE DATABASE SalesAnalytics');
END;
GO

USE SalesAnalytics;
GO

-- Asegurar esquema dbo
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dbo')
    EXEC('CREATE SCHEMA dbo');
GO

/* ============================================================
   Limpiar (DROP) en orden de dependencias
   ============================================================ */
IF OBJECT_ID('dbo.OrderDetail','U') IS NOT NULL DROP TABLE dbo.OrderDetail;
IF OBJECT_ID('dbo.Orders','U')      IS NOT NULL DROP TABLE dbo.Orders;
IF OBJECT_ID('dbo.Product','U')     IS NOT NULL DROP TABLE dbo.Product;
IF OBJECT_ID('dbo.Customer','U')    IS NOT NULL DROP TABLE dbo.Customer;
GO

/* ============================================================
   1) Customer
   ============================================================ */
CREATE TABLE dbo.Customer(
    CustomerID  INT           NOT NULL,            -- viene del CSV
    FirstName   NVARCHAR(100) NULL,
    LastName    NVARCHAR(100) NULL,
    Email       NVARCHAR(255) NULL,
    Phone       NVARCHAR(50)  NULL,
    City        NVARCHAR(100) NULL,
    Country     NVARCHAR(100) NULL,
    CONSTRAINT PK_Customer PRIMARY KEY (CustomerID)
);
GO

/* ============================================================
   2) Product
   ============================================================ */
CREATE TABLE dbo.Product(
    ProductID    INT            NOT NULL,          -- viene del CSV
    ProductName  NVARCHAR(200)  NULL,
    Category     NVARCHAR(100)  NULL,
    Price        DECIMAL(18,2)  NULL,
    Stock        INT            NULL,
    CONSTRAINT PK_Product PRIMARY KEY (ProductID),
    CONSTRAINT CK_Product_Price  CHECK (Price  IS NULL OR Price  >= 0),
    CONSTRAINT CK_Product_Stock  CHECK (Stock  IS NULL OR Stock  >= 0)
);
GO

/* ============================================================
   3) Orders (encabezado)
   ============================================================ */
CREATE TABLE dbo.Orders(
    OrderID     INT            NOT NULL,           -- viene del CSV
    CustomerID  INT            NOT NULL,
    OrderDate   DATETIME2      NULL,
    Status      NVARCHAR(50)   NULL,
    CONSTRAINT PK_Orders PRIMARY KEY (OrderID),
    CONSTRAINT FK_Orders_Customer FOREIGN KEY (CustomerID)
        REFERENCES dbo.Customer(CustomerID)
);
GO

-- Índices útiles
CREATE INDEX IX_Orders_OrderDate   ON dbo.Orders(OrderDate);
CREATE INDEX IX_Orders_CustomerID  ON dbo.Orders(CustomerID);
GO

/* ============================================================
   4) OrderDetail (detalle)
   ============================================================ */
CREATE TABLE dbo.OrderDetail(
    OrderID     INT            NOT NULL,
    ProductID   INT            NOT NULL,
    Quantity    INT            NULL,
    TotalPrice  DECIMAL(18,2)  NULL,
    CONSTRAINT PK_OrderDetail PRIMARY KEY (OrderID, ProductID),
    CONSTRAINT FK_OD_Orders   FOREIGN KEY (OrderID)  REFERENCES dbo.Orders(OrderID),
    CONSTRAINT FK_OD_Product  FOREIGN KEY (ProductID) REFERENCES dbo.Product(ProductID),
    CONSTRAINT CK_OD_Quantity   CHECK (Quantity   IS NULL OR Quantity   >= 0),
    CONSTRAINT CK_OD_TotalPrice CHECK (TotalPrice IS NULL OR TotalPrice >= 0)
);
GO

-- Índices útiles
CREATE INDEX IX_OrderDetail_ProductID ON dbo.OrderDetail(ProductID);
GO

/* ============================================================
   Vistas de apoyo
   ============================================================ */
IF OBJECT_ID('dbo.vw_SalesByDay','V') IS NOT NULL DROP VIEW dbo.vw_SalesByDay;
GO
CREATE VIEW dbo.vw_SalesByDay AS
SELECT
    CONVERT(date, o.OrderDate) AS [Date],
    SUM(od.TotalPrice)         AS TotalSales
FROM dbo.OrderDetail od
JOIN dbo.Orders o ON o.OrderID = od.OrderID
GROUP BY CONVERT(date, o.OrderDate);
GO

-- (Opcional) Vista de totales por pedido
IF OBJECT_ID('dbo.vw_OrderTotals','V') IS NOT NULL DROP VIEW dbo.vw_OrderTotals;
GO
CREATE VIEW dbo.vw_OrderTotals AS
SELECT
    od.OrderID,
    SUM(od.TotalPrice)                    AS OrderTotal,
    SUM(COALESCE(od.Quantity,0))          AS TotalItems,
    COUNT(*)                               AS Lines
FROM dbo.OrderDetail od
GROUP BY od.OrderID;
GO
