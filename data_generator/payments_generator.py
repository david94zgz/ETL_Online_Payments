"""
payments_generator.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the code necessary to generate the datasets that will be used later on
in the pipeline at the balances_negative_job.py module. It uses some configurations from the
conf.py module.
The dataset consist of the name of the account which makes the payment, the name of the
account which receive the payment, the amount of the payment and the currency of the amount.
"""

import csv
import sys
import random
from data_generator.conf.conf import payments_dataset_fn, payments_fieldnames, accounts, currency, payments_amount_range


def gen_payments_dataset(payments_entries: int) -> csv:
    """Produces a payment dataset which will be saved in the 'data' directory.

    :param payment_entries: Number of payments to be generated.
    """
    with open(payments_dataset_fn, 'w') as payments_dataset_file:
        dataset_writer = csv.writer(payments_dataset_file, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        dataset_writer.writerow(payments_fieldnames)
        for _ in range(0, payments_entries):
            dataset_writer.writerow([random.sample(accounts, k=1)[0], 
                                    random.sample(accounts, k=1)[0], 
                                    round(random.uniform(payments_amount_range[0], payments_amount_range[1]), 2),
                                    random.sample(currency, k=1)[0]])
    print(f"Wrote {payments_entries} lines in {payments_dataset_fn} file")
    return payments_dataset_file


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: payments.py payments_entries", file=sys.stderr)
        sys.exit(-1)
    
    payments_entries = int(sys.argv[1])
    gen_payments_dataset(payments_entries)
