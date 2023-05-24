QUESTION DESCRIPTIONS
============================================================================================================================================================================

customertype and transaction: -

You have a PySpark DataFrame called "transactions" with the following schema:
root
 |-- transaction_id: integer (nullable = true)
 |-- customer_id: integer (nullable = true)
 |-- product_id: integer (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- price: double (nullable = true)

You need to perform the following tasks:

Calculate the total revenue (quantity * price) for each transaction.
Apply a discount to the total revenue based on the customer type:
If the customer is a "regular" customer, apply a 10% discount.
If the customer is a "premium" customer, apply a 20% discount.
If the customer is a "vip" customer, apply a 30% discount.
Calculate the average discounted revenue for each customer type.

To determine the customer type, you need additional information or another DataFrame that associates customer IDs with their corresponding types

root
 |-- customer_id: integer (nullable = true)
 |-- customer_type: string (nullable = true)

The "customers" DataFrame contains the customer ID and their respective type ("regular", "premium", or "vip"). You can join the "transactions" DataFrame with the "customers" DataFrame on the "customer_id" column to get the customer type for each transaction.
