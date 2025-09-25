-- Quick counts for each table
USE SalesAnalytics;
GO

SELECT 'DataSource' AS TableName, COUNT(*) AS Rows FROM dbo.DataSource
UNION ALL SELECT 'Customer', COUNT(*) FROM dbo.Customer
UNION ALL SELECT 'Product', COUNT(*) FROM dbo.Product
UNION ALL SELECT 'Invoice', COUNT(*) FROM dbo.Invoice
UNION ALL SELECT 'Sale', COUNT(*) FROM dbo.Sale
UNION ALL SELECT 'Survey', COUNT(*) FROM dbo.Survey
UNION ALL SELECT 'SocialComment', COUNT(*) FROM dbo.SocialComment
UNION ALL SELECT 'WebReview', COUNT(*) FROM dbo.WebReview;

-- Result sets (adjust TOP as needed)
SELECT TOP (10) * FROM dbo.DataSource ORDER BY SourceID DESC;
SELECT TOP (10) * FROM dbo.Customer ORDER BY CustomerID DESC;
SELECT TOP (10) * FROM dbo.Product ORDER BY ProductID DESC;
SELECT TOP (10) * FROM dbo.Invoice ORDER BY InvoiceID DESC;
SELECT TOP (10) * FROM dbo.Sale ORDER BY SaleID DESC;
SELECT TOP (10) * FROM dbo.Survey ORDER BY SurveyID DESC;
SELECT TOP (10) * FROM dbo.SocialComment ORDER BY CommentID DESC;
SELECT TOP (10) * FROM dbo.WebReview ORDER BY ReviewID DESC;

-- Example analytical query
SELECT TOP (20)
    CONVERT(date, i.InvoiceDate) AS [Date],
    SUM(s.LineTotal) AS TotalSales
FROM dbo.Sale s
JOIN dbo.Invoice i ON i.InvoiceID = s.InvoiceID
GROUP BY CONVERT(date, i.InvoiceDate)
ORDER BY [Date] DESC;
