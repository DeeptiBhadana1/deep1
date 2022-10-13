create table `myproject-362310.customer.temp_table1` AS
(SELECT MonthYear,count(DISTINCT(DATE)) AS No_of_days
FROM `myproject-362310.customer.temp_table`
GROUP BY MonthYear
HAVING No_of_days>=24)