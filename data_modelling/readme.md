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

1. Identify facts and dimensions from the provided data. And what we might be performing analysis for.
    - Fact:
        - Orders and Order Items
    - Dimensions:
        - Slow changing dimensions:
            - Customer data
            - Product data
        - Very slow changing dimensions:
            - Departments
            - Categories
            - Date

2. Since there is a need to track pricing changes, for slow changing dimensions, implement an audit table to track changes as they are made.

3. We will be making use of sharded NoSQL database in snowflake, hence for quick writes, do not enforce FKs on the database itself. However, implement it on the ORM layer when doing analytics.

4. Partitioning by date on the Fact Table can be done for speediness, alternatively, regularly ETL jobs to generate BI tables in a data mart can be a form of partitioning.