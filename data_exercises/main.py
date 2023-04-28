""" Reads bank data and writes to log file """
import pandas as pd

from data_exercises.accounts import Accounts


def main() -> None:
    """Reads bank data and writes to log file"""
    data: pd.DataFrame = pd.read_csv(
        "/home/stephen/projects/data-exercises/tbankdata/accounts.csv"
    )

    accounts: Accounts = Accounts(account_data=data)

    result = accounts.sum_overdraft_limit()
    print(result)


if __name__ == "__main__":
    main()
