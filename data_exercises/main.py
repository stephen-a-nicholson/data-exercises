""" Reads bank data and writes to log file """
from pprint import pprint
import pandas as pd
from sqlalchemy import create_engine, text

from data_exercises.bank_data import BankData


def main() -> None:
    """Reads bank data and writes to log file"""
    account_data: pd.DataFrame = pd.read_csv(
        "/home/stephen/data-exercises/tbankdata/accounts.csv"
    )

    transaction_data: pd.DataFrame = pd.read_csv(
        "/home/stephen/data-exercises/tbankdata/current-acc-trans.csv"
    )

    bank_data: BankData = BankData(
        account_data=account_data, transaction_data=transaction_data
    )

    bank_data.count_accounts_holders_title()
    bank_data.count_account_holders_title_account_type()
    bank_data.avg_overdraft_limit()
    bank_data.sum_overdraft_limit()

    transactions: pd.DataFrame = bank_data.transaction_aggregations()
    transactions_monthly: pd.DataFrame = bank_data.transaction_aggregations_monthly()

    engine = create_engine("sqlite://", echo=False)

    transactions.to_sql("transactions", con=engine)
    transactions_monthly.to_sql("transactions_monthly", con=engine)

    with engine.connect() as conn:
        pprint(
            conn.execute(
                text(
                    """SELECT account_number, trans_total FROM transactions 
                WHERE trans_total = (SELECT MAX(trans_total) FROM transactions)"""
                )
            ).fetchall()
        )

        pprint(
            conn.execute(
                text(
                    """SELECT account_number, n_transactions FROM transactions 
                WHERE n_transactions = (SELECT MAX(n_transactions) FROM transactions)"""
                )
            ).fetchall()
        )

        pprint(
            conn.execute(
                text(
                    """SELECT account_number, trans_total FROM transactions 
                WHERE trans_total = (SELECT MIN(trans_total) FROM transactions)"""
                )
            ).fetchall()
        )

        pprint(
            conn.execute(
                text(
                    "SELECT COUNT(account_number) FROM transactions WHERE n_transactions IS NULL"
                )
            ).fetchall()
        )


if __name__ == "__main__":
    main()
