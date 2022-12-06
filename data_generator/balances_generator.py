"""
balances_generator.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the code necessary to generate the datasets that will be used later on
in the pipeline at the balances_negative_job.py module. It uses some configurations from the
conf.py module.
The dataset consist of records about the name of the account, the amount of money in the
account and the currency of this amount.
"""

import csv
import random
from data_generator.conf.conf import balances_dataset_fn, balances_fieldnames, accounts, currency, balances_amount_range


def gen_balances_dataset() -> csv:
    """Produces a balances dataset which will be saved in the 'data' directory.
    """
    with open(balances_dataset_fn, 'w') as balances_dataset_file:
        dataset_writer = csv.writer(balances_dataset_file, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        dataset_writer.writerow(balances_fieldnames)
        for i in range(len(accounts)):
            dataset_writer.writerow([accounts[i], 
                                    random.randint(balances_amount_range[0], balances_amount_range[1]),
                                    random.sample(currency, k=1)[0]])
    print(f"Wrote {len(accounts)} lines in {balances_dataset_fn} file")
    return balances_dataset_file


if __name__ == "__main__":
    gen_balances_dataset()
