from sqlalchemy.sql import *
import pandas as pd
from sqlalchemy import create_engine

import os, sys; sys.path.append('.')
from pipelining.ETLRow.model import *
from datetime import datetime

groupby = [FactOrderItems.order_item_product_id]

## As available data is for 2014 period, use a reasonable subset for today.
myday = datetime(2014,8,1)
my_start_day = myday - timedelta(days=60)

select_fields = select(DimProducts.id, DimProducts.product_name).cte()
## Get latest sold date.
agg_fields1 = select([*groupby,
    func.max(FactOrders.order_datetime).label('latest_sold_date')]).\
        join(FactOrders).select_from(FactOrderItems).\
            group_by(*groupby).\
                cte()

### Get aggregation for total number of days with sales, and total sold qty.
agg_fields2 = select([*groupby,
    func.count(distinct(FactOrders.order_datetime)).label('count_days_with_sales'),
    func.sum(FactOrderItems.order_item_quantity).label('total_sales_qty')]).\
        join(FactOrders).select_from(FactOrderItems).\
        join(DimProducts).select_from(FactOrderItems).\
            filter(FactOrders.order_datetime.between(my_start_day.strftime('%Y-%m-%d'),myday.strftime('%Y-%m-%d'))).\
                group_by(*groupby).\
                    cte()

## Assemble select query for business unit to aggregate necessary fields. This can be discussed and arranged before hand with the unit in question.
my_query = select(
    func.date('now'),
    select_fields, 
    # func.datediff(text('day'), agg_fields1.c.latest_sold_date, '2014-08-01'), #SQLite doesnt have datediff
    (func.julianday('2014-08-01') - func.julianday(agg_fields1.c.latest_sold_date)).label('days_with_no_sales'),
    agg_fields2.c.count_days_with_sales, agg_fields2.c.total_sales_qty).\
    join(agg_fields1, select_fields.c.id==agg_fields1.c.order_item_product_id).select_from(select_fields).\
    join(agg_fields2, select_fields.c.id==agg_fields2.c.order_item_product_id).select_from(select_fields)
    
print(my_query)
myengine = create_engine(f'sqlite:///data/demo.db')

with Session(myengine) as s:
    mydf = pd.read_sql(my_query, con=s.bind)
    print(mydf.head(20))
