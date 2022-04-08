#!/usr/bin/env python3
import logging

from database import Database
from notification_manager import adx_fillrate_notification, mox_mcm_status_update

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

def main():
    print("\n:::AdOps Python Tools:::")
    options = {
        "1": "Database module",
        "2": "Company M mcm status",
        "3": "ADX fillrate notification"
    }
    while True:
        print("\nWhat module would you like to use next? (press q to quit)")
        [print(f"{key}: {value}") for key, value in options.items()]
        choice = input("Pick a number: ")
        if choice == "1":
            Database().database_CLI()
        elif choice == "2":
            mox_mcm_status_update("mox")
        elif choice == "3":
            adx_fillrate_notification()
        else:
            break

if __name__ == "__main__":
    main()
