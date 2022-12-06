"""
balances_generator.py
~~~~~~~~~~~~~~~

This module contains unit tests for the data set generation defined in 
balances_generator.py.
"""

import unittest
import pandas as pd

from data_generator.balances_generator import gen_balances_dataset
from data_generator.conf.conf import balances_fieldnames, accounts, currency, balances_amount_range


class test_gen_balances_dataset(unittest.TestCase):

    def test_balances_dataset_generated(self):
        # assemble
        self.expected_fieldnames = balances_fieldnames
        self.expected_accounts = accounts
        self.expected_currency = sorted(currency)
        self.expected_balances_amount_range = balances_amount_range

        # act
        gen_balances_dataset()
        df = pd.read_csv("data/balances.csv")

        resulting_fieldnames = df.columns.tolist()
        resulting_accounts = df['account'].tolist()
        resulting_currency = sorted(df['currency'].unique().tolist())
        resulting_amounts_between_balance_amount_range = all(self.expected_balances_amount_range[0] <= amount <= self.expected_balances_amount_range[1] 
                                                for amount in df["amount"].values.tolist())

        # assert
        self.assertEqual(self.expected_fieldnames, resulting_fieldnames)
        self.assertEqual(self.expected_accounts, resulting_accounts)
        self.assertEqual(self.expected_currency, resulting_currency)
        self.assertTrue(resulting_amounts_between_balance_amount_range)


if __name__ == '__main__':
    unittest.main()