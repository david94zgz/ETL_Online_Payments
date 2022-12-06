# ETL_Online_Payments
Generates two random datasets about accounts and payments. Also, extract this datasets, make transformation to retrieve the accounts with negative balance after the payments and, lastly, save these negative balance acocunts as CSV and Parquet.


# Instructions
### Generating the test data

```bash
python3 -m data_generator.balances_generator
```
```bash
python3 -m data_generator.payments_generator payments_entries
```
