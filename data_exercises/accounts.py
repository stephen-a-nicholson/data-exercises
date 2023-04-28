""" Contains functions for data exercises """
import logging
import pandas as pd

log = logging.getLogger(__name__)


class Accounts:
    """Class for accounts data"""

    def __init__(self, account_data: pd.DataFrame) -> None:
        self.account_data = account_data

    def count_accounts_holders_title(self) -> pd.DataFrame:
        """Counts the number of account holders with each title

        Returns:
            pd.DataFrame: DataFrame
        """
        result = self.account_data.groupby(["title"])["title"].count()
        return result

    def count_account_holders_title_account_type(self) -> pd.DataFrame:
        """Produces a cross-table showing the number of account holders broken down by title and
        account_type (i.e. a count of the number of people with each combination of possible
        account types and titles)

        Returns:
            pd.DataFrame: DataFrame
        """
        result = self.account_data.groupby(["title", "account_type"])["title"].count()
        return result

    def avg_overdraft_limit(self) -> pd.DataFrame:
        """Produces a cross-table showing the number of account holders broken down by title and
        account_type (i.e. a count of the number of people with each combination of possible
        account types and titles)

        Returns:
            pd.DataFrame: DataFrame
        """
        result = self.account_data.groupby(["title", "account_type"])[
            "overdraft_limit"
        ].mean()
        return result

    def sum_overdraft_limit(self) -> pd.DataFrame:
        """Produces a cross-table showing the aggregate overdraft_limit (sum of) by title
        and account_type.

        Returns:
            pd.DataFrame: DataFrame
        """
        result = self.account_data.groupby(["title", "account_type"])[
            "overdraft_limit"
        ].sum()
        return result
