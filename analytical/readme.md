## Analytical SQL

### Select Query
Sometimes products for whatever reason stop selling and a symptom can be an item that was selling well faces a stock out or de-listing (or something else). Write a query that shows products that have sold for more than 30 days in the last 60 days, but hasn't had sales for the last week.
1. Date
2. Product_id
3. Total Items sold to date 
4. number of days with sales
5. number of dates in the recent history where sales have ceased

#### Solution
See query in myquery.py

---

### Implmenting Business logics in Data Marts
What would be the best way to implment this in Date/Product_id/Sum(sales)... type Data Mart with other sales information (i.e. without filters and group by)?

#### Solution

I believe the best way to implement this would be a weekly aggregated audit table loaded regularly independent of the data lake. Additionally, we can set up a View to filter the data internally for latest date for swift business function.
This can be further partitioned for the business unit in question.

Other sales function can be joined on product_id if the business unit so wishes.