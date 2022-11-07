from dataasset.migrations import upgrade_migrations

DSN = "mssql+pyodbc://sa:MSS#inside00@mssql-db:1433/master?driver=ODBC+Driver+17+for+SQL+Server"


def main():

    print("running upgrade")
    print(f"DSN = {DSN}")
    upgrade_migrations(DSN)
    print("running downgrade")


if __name__ == "__main__":
    main()
