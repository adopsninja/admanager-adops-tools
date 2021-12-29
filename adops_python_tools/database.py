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

    def user_table(self):
        try:
            self.db_cursor.execute(
                """CREATE TABLE user(
                email text,
                refresh_token text,
                access_token text
            )
            """
            )
        except OperationalError as identifier:
            logger.info(identifier.args[0])

    def app_table(self):
        try:
            self.db_cursor.execute(
                """CREATE TABLE app(
                app_name text,
                client_id text,
                client_secret text
            )
            """
            )
        except OperationalError as identifier:
            logger.info(identifier.args[0])

    def create_tables(self):
        """Creates user and application tables if does not exists
        """
        self.user_table()
        self.app_table()

    def app_credentials(self):
        """Checks if credentials for application defined by environmental variables exists.
        Creates if not exists or updates if already in database.
        """
        with self.db_connection:
            if self.db_cursor.execute(f"SELECT count(*) FROM app WHERE app_name = '{self.app_name}'").fetchone()[0] == 0:
                self.db_cursor.execute(
                    f"INSERT INTO app VALUES (:app_name, :client_id, :client_secret)",
                    {"app_name": self.app_name, "client_id": self.client_id, "client_secret": self.client_secret,},
                )
            else:
                self.db_cursor.execute(f"UPDATE app SET app_name = '{self.app_name}', client_id = '{self.client_id}', client_secret = '{self.client_secret}'")

    def add_user_credentials(self):
        """Checks if credentials exists for given user.
        Creates if not exists or updates if alraedy in database.
        
        Arguments:
            email {str} -- email account used to login to Google Admanager
            refresh_token {str} -- refresh token obtained through authorization process
        """
        with self.db_connection:
            user_cred = generate_refresh_token()
            logger.info(f"Generated refresh token for user {user_cred.get('email')}.")
            if self.db_cursor.execute(f"SELECT count(*) FROM user WHERE email = '{user_cred.get('email')}'").fetchone()[0] == 0:
                logger.info(f"User {user_cred.get('email')} doesn't exist in database. Inserting user into database.")
                self.db_cursor.execute(
                    f"INSERT INTO user VALUES (:email, :refresh_token, :access_token)", {
                        "email": user_cred.get("email"), 
                        "refresh_token": user_cred.get("refresh_token"), 
                        "access_token": user_cred.get("access_token")
                    },
                )
            else:
                self.db_cursor.execute(
                    f"UPDATE user SET email = '{user_cred.get('email')}', refresh_token = '{user_cred.get('refresh_token')}', access_token = '{user_cred.get('access_token')}' WHERE email = '{user_cred.get('email')}'"
                )
                logger.info(f"User {user_cred.get('email')} credentials updated.")


    def get_credentials(self, user_email=None):
        """Search for database specified by input number.
        
        Returns:
            tuple -- credentials nescessary to login into Google Admanager
        """
        app = self.db_cursor.execute(f"SELECT * FROM app WHERE app_name = '{self.app_name}'").fetchone()
        user_choice = self.get_all_users()
        if not user_email:
            user = self.db_cursor.execute(
                f"SELECT email, refresh_token FROM user WHERE email = :email", {"email": user_choice.get(int(input("Pick number: ")))},
            ).fetchone()
        else:
            user = self.db_cursor.execute(f"SELECT email, refresh_token FROM user WHERE email = :email", {"email": user_email},).fetchone()
        credentials = app + user
        logger.debug(credentials)
        return credentials

    def remove_user_credentials(self):
        """Remove user based on user choice. All users with picked email will be removed.

        Returns:
            str: removed email.
        """
        user_choice = self.get_all_users()
        user_to_remove = int(input("Pick number: "))
        with self.db_connection:
            self.db_cursor.execute(
                f"DELETE FROM user WHERE email =:email", {"email": user_choice.get(user_to_remove)},
            )
        return user_choice.get(user_to_remove)

    def get_all_users(self):
        """Gets all available users from database.

        Returns:
            dict: available users.
        """
        User = namedtuple("User", ["email", "refresh_token", "access_token"])
        users = self.db_cursor.execute("SELECT * FROM user").fetchall()
        print("\n::: Available users :::")
        available_users = {}
        for index, user in enumerate(sorted(map(User._make, users))):
            print(f"{index}: {user.email}")
            available_users.update({index: user.email})
        logger.debug(available_users)
        return available_users

    def database_CLI(self):
        self.create_tables()
        self.app_credentials()
        options = {
            "1": "Add user",
            "2": "Remove user"
        }

        while True:
            print("\nCurrent users in database:")
            self.get_all_users()
            print("\nWhat would you like to do next? (press q to quit)")
            for key, value in options.items():
                print(f"{key}: {value}")
            choice = input("Pick a number: ")
            if choice == "1":
                self.add_user_credentials()
            elif choice == "2":
                self.remove_user_credentials()
            else:
                break
