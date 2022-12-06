# ETL_Online_Payments
Generates two random datasets about accounts and payments. Also, extract this datasets, make transformation to retrieve the accounts with negative balance after the payments and, lastly, save these negative balance acocunts as CSV and Parquet.


# Instructions
### Generating the test data

```bash
python3 -m data_generator.balances_generator

python3 -m data_generator.payments_generator payments_entries
```
where:
  payments_entries: is an integer representing the number of payments you want to be generated
  
### Run the ETL
```bash
python3 -m jobs.balances_negative_job
```

### Test the ETL
```bash
python3 -m test.test_balances_negative_job
```

### Test the data generators
```bash
python3 -m test.test_balances_generator

python3 -m test.test_payments_generator
```
