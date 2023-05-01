""" Contains class for data exercises """
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler("results.log")
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)


class BankData:
    """Class for accounts and transactions data"""

    def __init__(
        self, account_data: pd.DataFrame, transaction_data: pd.DataFrame
    ) -> None:
        self.account_data = self._preprocess_accounts(account_data=account_data)
        self.transaction_data = self._preprocess_transactions(
            transaction_data=transaction_data
        )

    @staticmethod
    def _preprocess_accounts(account_data: pd.DataFrame) -> pd.DataFrame:
        account_data.loc[account_data["title"] == "Mr.", "title"] = "Mr"
        account_data.loc[account_data["title"] == "Mrs.", "title"] = "Mrs"

        account_data.replace(" ", np.nan, inplace=True)
        account_data.dropna(subset=["account_type"], inplace=True)

        return account_data

    @staticmethod
    def _preprocess_transactions(transaction_data: pd.DataFrame) -> pd.DataFrame:
        transaction_data["trans_date"] = pd.to_datetime(transaction_data["trans_date"])

        return transaction_data

    def count_accounts_holders_title(self) -> None:
        """Counts the number of account holders with each title

        Returns:
            None
        """
        result: pd.DataFrame = self.account_data.groupby(["title"])[
            "account_number"
        ].count()
        logger.info(
            "Number of account holders with each title - \n %s \n", result.to_string()
        )

    def count_account_holders_title_account_type(self) -> None:
        """Produces a cross-table showing the number of account holders broken down by title and
        account_type (i.e. a count of the number of people with each combination of possible
        account types and titles)

        Returns:
            None
        """
        result: pd.DataFrame = (
            self.account_data.groupby(["title", "account_type"])["account_number"]
            .count()
            .unstack()
        )
        logger.info(
            "Number of account holders by title and account type - \n %s \n",
            result.to_string(),
        )

    def avg_overdraft_limit(self) -> None:
        """Produce a cross-table showing the average overdraft_limit by title and account_type

        Returns:
            None
        """
        result: pd.DataFrame = (
            self.account_data.groupby(["title", "account_type"])["overdraft_limit"]
            .mean()
            .unstack()
        )
        logger.info(
            "average overdraft_limit by title and account_type - \n %s \n",
            result.to_string(),
        )

    def sum_overdraft_limit(self) -> None:
        """Produces a cross-table showing the aggregate overdraft_limit (sum of) by title
        and account_type

        Returns:
            None
        """
        result: pd.DataFrame = (
            self.account_data.groupby(["title", "account_type"])["overdraft_limit"]
            .sum()
            .unstack()
        )
        logger.info(
            "sum of overdraft_limit by title and account type - \n %s \n",
            result.to_string(),
        )

    def transaction_aggregations(self) -> pd.DataFrame:
        """counts the number of transactions and computes
        the total value of those transactions for each customer

        Returns:
            pd.DataFrame: DataFrame
        """
        result: pd.DataFrame = (
            self.transaction_data.groupby("acc_number")["amount"]
            .agg(["sum", "count"])
            .reset_index()
            .rename(
                columns={
                    "acc_number": "account_number",
                    "sum": "trans_total",
                    "count": "n_transactions",
                }
            )
        )

        joined = self.account_data.merge(result, on="account_number", how="left")

        logger.info(
            "transactions aggregations - \n %s \n",
            result.to_string(max_rows=10),
        )

        return joined

    def transaction_aggregations_monthly(self) -> pd.DataFrame:
        """Calculates the number and total value of transactions for
        each month for which there are transactions.

        Returns:
            pd.DataFrame: DataFrame
        """
        result: pd.DataFrame = (
            self.transaction_data.groupby(
                ["acc_number", self.transaction_data.trans_date.dt.strftime("%m-%y")]
            )["amount"]
            .agg(["sum", "count"])
            .reset_index()
            .rename(
                columns={
                    "acc_number": "account_number",
                    "sum": "trans_total",
                    "count": "n_transactions",
                }
            )
        )

        logger.info(
            "transactions aggregations monthly - \n %s \n",
            result.to_string(max_rows=10),
        )

        return result
