""" Contains data types for the application """
from dataclasses import dataclass


@dataclass
class TransactionsMonthly:
    """Contains transaction data"""

    transaction_data: dict
