-- Sales Analytics DB Schema
-- Generated on 2025-09-25
-- Target: Microsoft SQL Server (T-SQL)

IF DB_ID(N'SalesAnalytics') IS NULL
BEGIN
    EXEC('CREATE DATABASE SalesAnalytics');
END
GO

USE SalesAnalytics;
GO

-- SCHEMA
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dbo')
    EXEC('CREATE SCHEMA dbo');
GO

-- 1) Reference: Data Sources
IF OBJECT_ID('dbo.DataSource') IS NOT NULL DROP TABLE dbo.DataSource;
CREATE TABLE dbo.DataSource (
    SourceID            INT IDENTITY(1,1) CONSTRAINT PK_DataSource PRIMARY KEY,
    SourceName          NVARCHAR(200) NOT NULL,
    SourceType          NVARCHAR(100) NULL,        -- CSV | API | WEB | SOCIAL
    Url                 NVARCHAR(500) NULL,
    CreatedAt           DATETIME2(0)  NOT NULL CONSTRAINT DF_DataSource_CreatedAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT UQ_DataSource_SourceName UNIQUE (SourceName)
);

-- 2) Dimension: Customer
IF OBJECT_ID('dbo.Customer') IS NOT NULL DROP TABLE dbo.Customer;
CREATE TABLE dbo.Customer (
    CustomerID          INT IDENTITY(1,1) CONSTRAINT PK_Customer PRIMARY KEY,
    ExternalCustomerCode NVARCHAR(100) NULL,
    FullName            NVARCHAR(200) NOT NULL,
    Email               NVARCHAR(200) NULL,
    Phone               NVARCHAR(50)  NULL,
    Country             NVARCHAR(100) NULL,
    CreatedAt           DATETIME2(0)  NOT NULL CONSTRAINT DF_Customer_CreatedAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT UQ_Customer_Email UNIQUE (Email)
);

-- 3) Dimension: Product
IF OBJECT_ID('dbo.Product') IS NOT NULL DROP TABLE dbo.Product;
CREATE TABLE dbo.Product (
    ProductID           INT IDENTITY(1,1) CONSTRAINT PK_Product PRIMARY KEY,
    Sku                 NVARCHAR(100) NOT NULL,
    ProductName         NVARCHAR(200) NOT NULL,
    Category            NVARCHAR(100) NULL,
    Subcategory         NVARCHAR(100) NULL,
    Brand               NVARCHAR(100) NULL,
    UnitPrice           DECIMAL(18,4) NULL,
    Currency            NCHAR(3)      NULL,     -- ISO like 'USD', 'DOP'
    IsActive            BIT           NOT NULL CONSTRAINT DF_Product_IsActive DEFAULT (1),
    CreatedAt           DATETIME2(0)  NOT NULL CONSTRAINT DF_Product_CreatedAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT UQ_Product_Sku UNIQUE (Sku)
);

-- 4) Header: Invoice (Factura)
IF OBJECT_ID('dbo.Invoice') IS NOT NULL DROP TABLE dbo.Invoice;
CREATE TABLE dbo.Invoice (
    InvoiceID           INT IDENTITY(1,1) CONSTRAINT PK_Invoice PRIMARY KEY,
    InvoiceNumber       NVARCHAR(50)  NOT NULL,
    InvoiceDate         DATETIME2(0)  NOT NULL,
    SourceID            INT           NOT NULL,
    CustomerID          INT           NOT NULL,
    Subtotal            DECIMAL(18,4) NULL,
    TaxAmount           DECIMAL(18,4) NULL,
    ShippingAmount      DECIMAL(18,4) NULL,
    DiscountAmount      DECIMAL(18,4) NULL,
    TotalAmount         DECIMAL(18,4) NULL,
    Currency            NCHAR(3)      NULL,
    CreatedAt           DATETIME2(0)  NOT NULL CONSTRAINT DF_Invoice_CreatedAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT UQ_Invoice_InvoiceNumber UNIQUE (InvoiceNumber),
    CONSTRAINT FK_Invoice_Source  FOREIGN KEY (SourceID)  REFERENCES dbo.DataSource(SourceID),
    CONSTRAINT FK_Invoice_Customer FOREIGN KEY (CustomerID) REFERENCES dbo.Customer(CustomerID)
);
CREATE INDEX IX_Invoice_Date ON dbo.Invoice(InvoiceDate);
CREATE INDEX IX_Invoice_Customer ON dbo.Invoice(CustomerID);

-- 5) Detail: Sale (Ventas)
IF OBJECT_ID('dbo.Sale') IS NOT NULL DROP TABLE dbo.Sale;
CREATE TABLE dbo.Sale (
    SaleID              INT IDENTITY(1,1) CONSTRAINT PK_Sale PRIMARY KEY,
    InvoiceID           INT NOT NULL,
    ProductID           INT NOT NULL,
    Quantity            DECIMAL(18,4) NOT NULL CHECK (Quantity >= 0),
    UnitPrice           DECIMAL(18,4) NOT NULL CHECK (UnitPrice >= 0),
    DiscountPct         DECIMAL(5,2)  NULL CHECK (DiscountPct BETWEEN 0 AND 100),
    TaxPct              DECIMAL(5,2)  NULL CHECK (TaxPct BETWEEN 0 AND 100),
    LineTotal           DECIMAL(18,4) NOT NULL,
    CreatedAt           DATETIME2(0)  NOT NULL CONSTRAINT DF_Sale_CreatedAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT FK_Sale_Invoice FOREIGN KEY (InvoiceID) REFERENCES dbo.Invoice(InvoiceID),
    CONSTRAINT FK_Sale_Product FOREIGN KEY (ProductID) REFERENCES dbo.Product(ProductID)
);
CREATE INDEX IX_Sale_Invoice ON dbo.Sale(InvoiceID);
CREATE INDEX IX_Sale_Product ON dbo.Sale(ProductID);

-- 6) Surveys (Encuestas)
IF OBJECT_ID('dbo.Survey') IS NOT NULL DROP TABLE dbo.Survey;
CREATE TABLE dbo.Survey (
    SurveyID            INT IDENTITY(1,1) CONSTRAINT PK_Survey PRIMARY KEY,
    SourceID            INT           NOT NULL,
    CustomerID          INT           NULL,
    ProductID           INT           NULL,
    SurveyDate          DATETIME2(0)  NOT NULL,
    Rating              TINYINT       NULL CHECK (Rating BETWEEN 1 AND 5),
    Comment             NVARCHAR(MAX) NULL,
    RawExternalID       NVARCHAR(100) NULL,
    CreatedAt           DATETIME2(0)  NOT NULL CONSTRAINT DF_Survey_CreatedAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT FK_Survey_Source   FOREIGN KEY (SourceID)  REFERENCES dbo.DataSource(SourceID),
    CONSTRAINT FK_Survey_Customer FOREIGN KEY (CustomerID) REFERENCES dbo.Customer(CustomerID),
    CONSTRAINT FK_Survey_Product  FOREIGN KEY (ProductID) REFERENCES dbo.Product(ProductID)
);
CREATE INDEX IX_Survey_Product ON dbo.Survey(ProductID);
CREATE INDEX IX_Survey_Customer ON dbo.Survey(CustomerID);

-- 7) Social Comments (Comentarios sociales)
IF OBJECT_ID('dbo.SocialComment') IS NOT NULL DROP TABLE dbo.SocialComment;
CREATE TABLE dbo.SocialComment (
    CommentID           INT IDENTITY(1,1) CONSTRAINT PK_SocialComment PRIMARY KEY,
    SourceID            INT           NOT NULL,
    Platform            NVARCHAR(50)  NULL,       -- X, Facebook, Instagram, etc.
    Handle              NVARCHAR(100) NULL,       -- usuario/autor
    CustomerID          INT           NULL,
    ProductID           INT           NULL,
    CommentDate         DATETIME2(0)  NOT NULL,
    Text                NVARCHAR(MAX) NOT NULL,
    SentimentScore      DECIMAL(5,2)  NULL,       -- -1.00..1.00 or 0..1 (your choice)
    Url                 NVARCHAR(500) NULL,
    CreatedAt           DATETIME2(0)  NOT NULL CONSTRAINT DF_SocialComment_CreatedAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT FK_Social_Source   FOREIGN KEY (SourceID)  REFERENCES dbo.DataSource(SourceID),
    CONSTRAINT FK_Social_Customer FOREIGN KEY (CustomerID) REFERENCES dbo.Customer(CustomerID),
    CONSTRAINT FK_Social_Product  FOREIGN KEY (ProductID) REFERENCES dbo.Product(ProductID)
);
CREATE INDEX IX_Social_Product ON dbo.SocialComment(ProductID);
CREATE INDEX IX_Social_Date ON dbo.SocialComment(CommentDate);

-- 8) Web Reviews (Reseñas)
IF OBJECT_ID('dbo.WebReview') IS NOT NULL DROP TABLE dbo.WebReview;
CREATE TABLE dbo.WebReview (
    ReviewID            INT IDENTITY(1,1) CONSTRAINT PK_WebReview PRIMARY KEY,
    SourceID            INT           NOT NULL,
    CustomerID          INT           NULL,
    ProductID           INT           NOT NULL,
    ReviewDate          DATETIME2(0)  NOT NULL,
    Rating              TINYINT       NULL CHECK (Rating BETWEEN 1 AND 5),
    Title               NVARCHAR(200) NULL,
    Body                NVARCHAR(MAX) NULL,
    Url                 NVARCHAR(500) NULL,
    CreatedAt           DATETIME2(0)  NOT NULL CONSTRAINT DF_WebReview_CreatedAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT FK_WebReview_Source   FOREIGN KEY (SourceID)  REFERENCES dbo.DataSource(SourceID),
    CONSTRAINT FK_WebReview_Customer FOREIGN KEY (CustomerID) REFERENCES dbo.Customer(CustomerID),
    CONSTRAINT FK_WebReview_Product  FOREIGN KEY (ProductID) REFERENCES dbo.Product(ProductID)
);
CREATE INDEX IX_WebReview_Product ON dbo.WebReview(ProductID);
CREATE INDEX IX_WebReview_Date ON dbo.WebReview(ReviewDate);

-- Some helpful views
IF OBJECT_ID('dbo.vw_SalesByDay') IS NOT NULL DROP VIEW dbo.vw_SalesByDay;
GO
CREATE VIEW dbo.vw_SalesByDay AS
SELECT
    CONVERT(date, i.InvoiceDate) AS [Date],
    SUM(s.LineTotal) AS TotalSales
FROM dbo.Sale s
JOIN dbo.Invoice i ON i.InvoiceID = s.InvoiceID
GROUP BY CONVERT(date, i.InvoiceDate);
GO
