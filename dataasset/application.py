import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["init"])
    parser.add_argument("-u", "--url", type=str, action="store")

    args = parser.parse_args()

    if args.command == "init":
        print("initialising dataasset")
        from dataasset.migrations import upgrade_migrations

        upgrade_migrations(args.url)
