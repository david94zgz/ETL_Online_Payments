"""
conf.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Contains some hard coded values used for generating the payments 
and balances dataset.
"""

payments_dataset_fn = "data/payments.csv"
balances_dataset_fn = "data/balances.csv"

payments_fieldnames = ["paid_from", "paid_to", "amount", "currency"]
balances_fieldnames = ["account", "amount", "currency"]

accounts = ["a10", "a11", "a12", "a13", "a14", "a15",
            "b10", "b11", "b12", "b13", "b14", "b15",
            "c10", "c11", "c12", "c13", "c14", "c15",
            "d10", "d11", "d12", "d13", "d14", "d15",
            "e10", "e11", "e12", "e13", "e14", "e15",
            "f10", "f11", "f12", "f13", "f14", "f15",]
        
balances_amount_range = [100, 1000]
payments_amount_range = [0.95, 499.95]

currency = ["usd", "eur"]
eurusd_rate = 1.05  # 1EUR = 1.05USD  # If changed, expected test data do not match