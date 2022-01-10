#!/usr/bin/env python
import logging
import sqlite3
from collections import namedtuple
from sqlite3 import OperationalError

from constants import (DEFAULT_APP_NAME, DEFAULT_CLIENT_ID,
                       DEFAULT_CLIENT_SECRET, DEFAULT_DB_PATH)
from refresh_token import generate_refresh_token

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


class Database:
    """Application database class.
    """

    app_name = DEFAULT_APP_NAME
    client_id = DEFAULT_CLIENT_ID
    client_secret = DEFAULT_CLIENT_SECRET
    db_path = DEFAULT_DB_PATH

    def __init__(self):
        self.db_connection = sqlite3.connect(self.db_path)
        self.db_cursor = self.db_connection.cursor()
        self.Credentials = namedtuple("Credentials", [
            "email",
            "refresh_token",
            "access_token",
            "app_name",
            "client_id",
            "client_secret"
        ])

    def credentials_table(self):
        """Creates credentials table if does not exists"""

        try:
            self.db_cursor.execute(
                """CREATE TABLE credentials(
                    email text,
                    refresh_token text,
                    access_token text,
                    app_name text,
                    client_id text,
                    client_secret text
                )"""
            )
        except OperationalError as identifier:
            logger.info(identifier.args[0])

    def add_user_credentials(self):
        """Inserts credentials into database for a user if not exist, updates record otherwise."""

        with self.db_connection:
            user_cred = generate_refresh_token()
            logger.info(f"Generated refresh token for user {user_cred.get('email')}.")
            if self.db_cursor.execute(
                    f"SELECT * FROM credentials WHERE email = '{user_cred.get('email')}'"
                ).fetchone() == None:
                logger.info(f"User {user_cred.get('email')} doesn't exist in database. Inserting user into database.")
                self.db_cursor.execute(
                    f"""INSERT INTO credentials VALUES (
                        :email, 
                        :refresh_token, 
                        :access_token, 
                        :app_name, 
                        :client_id, 
                        :client_secret
                    )""",
                    {
                        "email": user_cred.get("email"),
                        "refresh_token": user_cred.get("refresh_token"),
                        "access_token": user_cred.get("access_token"),
                        "app_name": self.app_name,
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                    },
                )
            else:
                self.db_cursor.execute(
                    f"""UPDATE credentials SET 
                        email = '{user_cred.get('email')}', 
                        refresh_token = '{user_cred.get('refresh_token')}', 
                        access_token = '{user_cred.get('access_token')}', 
                        app_name = '{self.app_name}', 
                        client_id = '{self.client_id}', 
                        client_secret = '{self.client_secret}' 
                        WHERE email = '{user_cred.get('email')}'
                    """)
                logger.info(f"User {user_cred.get('email')} credentials updated.")

    def get_credentials(self, user_email=None):
        """Returns namedtuple of oauth user credentials from database."""
        users = self.get_users()
        if not user_email:
            user = self.db_cursor.execute(
                f"SELECT email, refresh_token, access_token, app_name, client_id, client_secret FROM credentials WHERE email = :email",
                {"email": users.get(int(input("Pick number: ")))},
            ).fetchone()
        else:
            user = self.db_cursor.execute(
                f"SELECT email, refresh_token, access_token, app_name, client_id, client_secret FROM credentials WHERE email = :email",
                {"email": user_email},
            ).fetchone()
        return self.Credentials(*user)

    def remove_user_credentials(self):
        """Removes user from credentials table based on user choice."""
        users = self.get_users()
        user_to_remove = int(input("Pick number: "))
        with self.db_connection:
            self.db_cursor.execute(
                "DELETE FROM credentials WHERE email =:email", 
                {"email": users.get(user_to_remove)},
            )
        return users.get(user_to_remove)

    def get_users(self):
        """Gets all available users from credentials table."""
        users = self.db_cursor.execute("SELECT * FROM credentials").fetchall()
        print("\n::: Available users :::")
        available_users = {}
        for index, user in enumerate(sorted(map(self.Credentials._make, users))):
            print(f"{index}: {user.email}")
            available_users.update({index: user.email})
        return available_users

    def database_CLI(self):
        self.credentials_table()
        options = {
            "1": "Add user", 
            "2": "Remove user", 
            "3": "Print out user credentials (debug only)"
        }

        while True:
            print("\nCurrent users in database:")
            self.get_users()
            print("\nWhat would you like to do next? (press q to quit)")
            for key, value in options.items():
                print(f"{key}: {value}")
            choice = input("Pick a number: ")
            if choice == "1":
                self.add_user_credentials()
            elif choice == "2":
                self.remove_user_credentials()
            elif choice == "3":
                print(self.get_credentials())
            else:
                break
