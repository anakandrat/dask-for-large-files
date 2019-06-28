import dask.dataframe as dd

FILE1 = 'data/big_file1.csv'
FILE2 = 'data/big_file2.csv'


def main():
    df1 = dd.read_csv(FILE1)
    df2 = dd.read_csv(FILE2)

    df3 = dd.merge(df1, df2, on=['name', 'age', 'email'], how='inner')

    print(df3.head())


if __name__ == "__main__":
    main()


