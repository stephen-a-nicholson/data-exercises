""" Script to complete data exercises """
import pandas as pd


def main() -> None:
    data = pd.read_csv("/home/stephen/projects/data-exercises/tbankdata/accounts.csv")
    print(data.head())


if __name__ == "__main__":
    main()
