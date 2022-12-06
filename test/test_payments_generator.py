"""
payments_generator.py
~~~~~~~~~~~~~~~

This module contains unit tests for the data set generation defined in 
payments_generator.py.
"""

import unittest
import pandas as pd
import random

from data_generator.payments_generator import gen_payments_dataset
from data_generator.conf.conf import payments_fieldnames, accounts, currency, payments_amount_range


class test_gen_payments_dataset(unittest.TestCase):

    def test_payments_dataset_generated(self):
        # assemble
        self.expected_fieldnames = payments_fieldnames
        self.expected_accounts = accounts
        self.expected_currency = sorted(currency)
        self.expected_payments_amount_range = payments_amount_range
    
        # act
        gen_payments_dataset(random.randint(100,500))
        df = pd.read_csv("data/payments.csv")

        resulting_fieldnames = df.columns.values.tolist()
        resulting_accounts_in_expected_accounts = all(account in self.expected_accounts for account in df["paid_from"].tolist() and df["paid_to"].tolist())
        resulting_currency = sorted(df['currency'].unique().tolist())
        resulting_amounts_between_payments_amount_range = all(self.expected_payments_amount_range[0] <= amount <= self.expected_payments_amount_range[1] 
                                                            for amount in df["amount"].tolist())

        # assert
        self.assertEqual(self.expected_fieldnames, resulting_fieldnames)
        self.assertTrue(resulting_accounts_in_expected_accounts)
        self.assertEqual(self.expected_currency, resulting_currency)
        self.assertTrue(resulting_amounts_between_payments_amount_range)


if __name__ == '__main__':
    unittest.main()