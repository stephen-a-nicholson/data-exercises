""" Script to complete data exercises """
import pandas as pd

def main() -> None:
    data = pd.read_csv("../tbankdata/accounts.csv")
    data.head()

if __name__ == "__main__":
    main()