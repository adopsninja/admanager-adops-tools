#!/usr/bin/env python
import logging

import authentication.database as database

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

def main():
    print("\n:::AdOps Python Tools:::")
    options = {
        "1": "Database module"
    }
    while True:
        print("\nWhat module would you like to use next? (press q to quit)")
        [print(f"{key}: {value}") for key, value in options.items()]
        choice = input("Pick a number: ")
        if choice == "1":
            database.Database().database_CLI()
        else:
            break

if __name__ == "__main__":
    main()
