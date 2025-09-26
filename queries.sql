-- =========================================================
-- 0) Contexto de BD
-- =========================================================
USE SalesAnalytics;
GO

-- =========================================================
-- 1) Conteos rápidos por tabla
-- =========================================================
SELECT 'Customer'    AS TableName, COUNT(*) AS Rows FROM dbo.Customer
UNION ALL SELECT 'Product',      COUNT(*) FROM dbo.Product
UNION ALL SELECT 'Orders',       COUNT(*) FROM dbo.Orders
UNION ALL SELECT 'OrderDetail',  COUNT(*) FROM dbo.OrderDetail;
GO

-- =========================================================
-- 2) Muestras de filas (TOP 10)
-- =========================================================
SELECT TOP (10) * FROM dbo.Customer    ORDER BY CustomerID   DESC;
SELECT TOP (10) * FROM dbo.Product     ORDER BY ProductID    DESC;
SELECT TOP (10) * FROM dbo.Orders      ORDER BY OrderID      DESC;
SELECT TOP (10) * FROM dbo.OrderDetail ORDER BY OrderID DESC, ProductID DESC;
GO

-- =========================================================
-- 3) Ventas por día (usa OrderDetail.TotalPrice)
-- =========================================================
SELECT
  CAST(o.OrderDate AS date) AS [Date],
  SUM(od.TotalPrice)        AS TotalSales
FROM dbo.Orders o
JOIN dbo.OrderDetail od ON od.OrderID = o.OrderID
GROUP BY CAST(o.OrderDate AS date)
ORDER BY [Date] DESC;
GO

-- =========================================================
-- 4) Ventas por mes (año-mes)
-- =========================================================
SELECT
  CONVERT(char(7), o.OrderDate, 126) AS YearMonth,  -- 'YYYY-MM'
  SUM(od.TotalPrice)                 AS TotalSales
FROM dbo.Orders o
JOIN dbo.OrderDetail od ON od.OrderID = o.OrderID
GROUP BY CONVERT(char(7), o.OrderDate, 126)
ORDER BY YearMonth DESC;
GO

-- =========================================================
-- 5) Top 10 productos por ventas
-- =========================================================
SELECT TOP (10)
  p.ProductID,
  p.ProductName,
  SUM(od.TotalPrice) AS SalesAmount,
  SUM(od.Quantity)   AS Units
FROM dbo.OrderDetail od
JOIN dbo.Product p ON p.ProductID = od.ProductID
GROUP BY p.ProductID, p.ProductName
ORDER BY SalesAmount DESC;
GO

-- =========================================================
-- 6) Top 10 clientes por ventas
-- =========================================================
SELECT TOP (10)
  c.CustomerID,
  CONCAT(ISNULL(c.FirstName,''),' ',ISNULL(c.LastName,'')) AS CustomerName,
  SUM(od.TotalPrice) AS SalesAmount,
  COUNT(DISTINCT o.OrderID) AS OrdersCount
FROM dbo.Orders o
JOIN dbo.Customer c   ON c.CustomerID = o.CustomerID
JOIN dbo.OrderDetail od ON od.OrderID = o.OrderID
GROUP BY c.CustomerID, c.FirstName, c.LastName
ORDER BY SalesAmount DESC;
GO

-- =========================================================
-- 7) Distribución por estado del pedido
-- =========================================================
SELECT
  ISNULL(o.Status,'(NULL)') AS [Status],
  COUNT(*)                  AS OrdersCount
FROM dbo.Orders o
GROUP BY ISNULL(o.Status,'(NULL)')
ORDER BY OrdersCount DESC;
GO

-- =========================================================
-- 8) Ticket promedio por pedido (Average Order Value)
--    (suma líneas por pedido y luego promedia)
-- =========================================================
WITH per_order AS (
  SELECT o.OrderID, SUM(od.TotalPrice) AS order_total
  FROM dbo.Orders o
  JOIN dbo.OrderDetail od ON od.OrderID = o.OrderID
  GROUP BY o.OrderID
)
SELECT
  COUNT(*)                               AS NumOrders,
  SUM(order_total)                       AS GrossSales,
  AVG(CAST(order_total AS decimal(18,2))) AS AvgOrderValue
FROM per_order;
GO

-- =========================================================
-- 9) Productos sin ventas (no aparecen en OrderDetail)
-- =========================================================
SELECT p.ProductID, p.ProductName, p.Category, p.Price, p.Stock
FROM dbo.Product p
LEFT JOIN dbo.OrderDetail od ON od.ProductID = p.ProductID
WHERE od.ProductID IS NULL
ORDER BY p.ProductID;
GO

-- =========================================================
-- 10) Clientes sin pedidos
-- =========================================================
SELECT c.CustomerID, c.FirstName, c.LastName, c.Email, c.Country
FROM dbo.Customer c
LEFT JOIN dbo.Orders o ON o.CustomerID = c.CustomerID
WHERE o.CustomerID IS NULL
ORDER BY c.CustomerID;
GO

-- =========================================================
-- 11) Detalle de un pedido (muestra unit price estimado = TotalPrice/Qty)
--     Cambia @OrderID por uno existente si quieres probar uno específico.
-- =========================================================
DECLARE @OrderID int = (
  SELECT TOP (1) OrderID FROM dbo.Orders ORDER BY OrderID DESC
);

SELECT
  od.OrderID,
  o.OrderDate,
  od.ProductID,
  p.ProductName,
  od.Quantity,
  od.TotalPrice,
  CASE WHEN od.Quantity > 0
       THEN CAST(od.TotalPrice / od.Quantity AS decimal(18,2))
       ELSE NULL END AS EstimatedUnitPrice
FROM dbo.OrderDetail od
JOIN dbo.Orders o  ON o.OrderID = od.OrderID
JOIN dbo.Product p ON p.ProductID = od.ProductID
WHERE od.OrderID = @OrderID
ORDER BY od.ProductID;
GO

-- =========================================================
-- 12) Reconciliación: total del pedido vs suma de líneas
-- =========================================================
-- (Si tu modelo tuviera un total en Orders, podrías compararlo aquí.
--  Como no existe, mostramos el monto por pedido según las líneas.)
SELECT
  o.OrderID,
  o.OrderDate,
  SUM(od.TotalPrice) AS LinesTotal
FROM dbo.Orders o
JOIN dbo.OrderDetail od ON od.OrderID = o.OrderID
GROUP BY o.OrderID, o.OrderDate
ORDER BY o.OrderID DESC;
GO

-- =========================================================
-- 13) Ventas por categoría de producto
-- =========================================================
SELECT
  p.Category,
  SUM(od.TotalPrice) AS SalesAmount,
  SUM(od.Quantity)   AS Units
FROM dbo.OrderDetail od
JOIN dbo.Product p ON p.ProductID = od.ProductID
GROUP BY p.Category
ORDER BY SalesAmount DESC;
GO

-- =========================================================
-- 14) Rango de fechas (parámetros) para filtrar informes
-- =========================================================
DECLARE @d1 date = DATEADD(day, -30, CAST(GETDATE() AS date));
DECLARE @d2 date = CAST(GETDATE() AS date);

SELECT
  CAST(o.OrderDate AS date) AS [Date],
  SUM(od.TotalPrice)        AS TotalSales
FROM dbo.Orders o
JOIN dbo.OrderDetail od ON od.OrderID = o.OrderID
WHERE o.OrderDate >= @d1
  AND o.OrderDate <  DATEADD(day, 1, @d2)
GROUP BY CAST(o.OrderDate AS date)
ORDER BY [Date] DESC;
GO



