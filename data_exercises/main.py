""" Reads bank data, performs transformations and writes to log file """
from pprint import pprint
import pandas as pd
from sqlalchemy import create_engine, text

from data_exercises.bank_data import BankData
from data_exercises.data_types import TransactionsMonthly


def main() -> None:
    """Reads bank data, performs transformations and writes to log file"""
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

    transactions_agg: pd.DataFrame = bank_data.transaction_aggregations()
    transactions_monthly_agg: pd.DataFrame = (
        bank_data.transaction_aggregations_monthly()
    )

    trans_monthly: TransactionsMonthly = TransactionsMonthly(
        transactions_monthly_agg.to_dict("records")
    )
    print(type(trans_monthly))

    engine = create_engine("sqlite://", echo=False)

    transactions_agg.to_sql("transactions", con=engine)
    transactions_monthly_agg.to_sql("transactions_monthly", con=engine)
    transaction_data.to_sql("transactions_raw", con=engine)

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
                    """SELECT acc_number, amount FROM transactions_raw
                WHERE amount = (SELECT MIN(amount) FROM transactions_raw)"""
                )
            ).fetchall()
        )

        pprint(
            conn.execute(
                text(
                    """WITH sequence AS(
                        SELECT account_number,
                                trans_date,
                                Row_number()
                                    OVER (
                                    partition BY account_number, Strftime('%Y', trans_date)
                                    ORDER BY Strftime('%M', trans_date)) AS RN
                            FROM transactions_monthly)
                        SELECT COUNT(DISTINCT account_number) FROM sequence WHERE RN >=11"""
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
