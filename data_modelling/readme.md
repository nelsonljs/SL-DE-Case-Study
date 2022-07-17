## Data Modelling

Create_DB_tables_pg.sql depecits how the data should be formatted after it has landed; how would you architecture base layer for OLAP purposes.
Assume the business will need to perform frequent analysis of:
- Revenue
- Item Sales
- Price Changes
- By Product
- By Customer
- By Date

Discuss your architecture considerations as well as pk/fks (soft or hard), indexes and partitions where appropraite - you may assume the real orders table is about 20-30 M records

## Architecture Considerations

1. The source data seems to be coming from an external OLTP source to be landed in an OLAP location for analysis. OLTP data can be obtained from a database snapshot with periodic migration into the OLAP database.

2. Identify facts and dimensions from the provided data. And what we might be performing analysis for. If we have potential to be analyzing Revenue, Item Sales and Price Changes, these should ideally be classified as Facts for ease of aggregation. As a best practice, we should also ensure that Facts should be incremental and never updated.

    - Fact:
        - Orders
        - Order Items
    - Dimensions:
        - Slow changing dimensions:
            - Customer data
            - Product data
        - Very slow changing dimensions:
            - Departments
            - Categories
            - Date

    - Audit Tables:
        - To track other metrics that might be interesting to the business, such as price changes.
        - These are also updated as Facts.

3. Since there is a need to track pricing changes, for slow changing dimensions, implement an audit table to track changes as they are made.

4. We will be making use of sharded NoSQL database in snowflake, hence for quick writes, do not enforce FKs on the database itself. However, implement it on the ORM layer when doing analytics.

5. Preference to also maintain a pythonic ORM representation of portions of the database to capture the FK relationships. This also makes it convenient for prototyping and developing further analytical data software, or BI charts.

6. Partitioning by date on the Fact Table can be done for speediness, alternatively, regularly ETL jobs to generate BI tables in a data mart can be a form of partitioning.

### Data Governance and Quality

There should also be consideration to check the quality of the data that is received. Alternatively, periodic sense checks of data can be performed (checking on duplicity, merging of data.)