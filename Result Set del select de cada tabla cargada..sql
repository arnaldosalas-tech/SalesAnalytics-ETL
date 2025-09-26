USE SalesAnalytics;
GO

-- Customer
SELECT TOP (100)
  CustomerID, FirstName, LastName, Email, Phone, City, Country
FROM dbo.Customer
ORDER BY CustomerID;

-- Product
SELECT TOP (100)
  ProductID, ProductName, Category, Price, Stock
FROM dbo.Product
ORDER BY ProductID;

-- Orders
SELECT TOP (100)
  OrderID, CustomerID, OrderDate, Status
FROM dbo.Orders
ORDER BY OrderID;

-- OrderDetail
SELECT TOP (100)
  OrderID, ProductID, Quantity, TotalPrice
FROM dbo.OrderDetail
ORDER BY OrderID, ProductID;