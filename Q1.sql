create table `myproject-362310.customer.temp_table` AS 
(SELECT
    InvoiceNo,
    StockCode,
    Description,
    Quantity,
    PARSE_DATETIME("%m/%d/%Y %H:%M", InvoiceDate) AS InvoiceDate,
    UnitPrice,
    CustomerID,
    Country,
    CURRENT_DATETIME() AS Created_time,
    CURRENT_DATETIME() AS Modified_time,
    Quantity*UnitPrice AS sales,
    FORMAT_DATE('%m-%Y',PARSE_DATETIME("%m/%d/%Y %H:%M", InvoiceDate)) AS MonthYear,
    CAST(EXTRACT(DAY FROM PARSE_DATETIME("%m/%d/%Y %H:%M", InvoiceDate)) AS INT64) AS DATE
   
  FROM
    `myproject-362310.customer.data_raw`
  WHERE
    Quantity>0
    AND CustomerID IS NOT NULL)