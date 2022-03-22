#!/usr/bin/env python
import os
# The Ad Manager API OAuth2 and GMAIL scope.
SCOPES = [
    "https://www.googleapis.com/auth/dfp",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://mail.google.com/",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DEFAULT_DB_PATH = os.environ["DEFAULT_DB_PATH"]
DEFAULT_APP_NAME = os.environ["DEFAULT_APP_NAME"]

# Your OAuth2 Client ID and Secret. If you do not have an ID and Secret yet,
# please go to https://console.developers.google.com and create a set.
DEFAULT_CLIENT_ID = os.environ["DEFAULT_CLIENT_ID"]
DEFAULT_CLIENT_SECRET = os.environ["DEFAULT_CLIENT_SECRET"]

# The redirect URI set for the given Client ID. The redirect URI for Client ID
# generated for an installed application will always have this value.
_REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"
TOKEN_URI = "https://oauth2.googleapis.com/token"
TOKEN_EXPIRY = "2022-03-21T13:15:16.922093Z"
USER_AGENT = "Python client library"

API_VERSION = "v202105"

PLACEMENT_MANAGER_PATH = "/data/placement_manager.yaml"
REPORT_MANAGER_PATH = "/data/report_manager.yaml"

AD_UNIT_MANAGER_PATH = "/data/ad_unit_manager.yaml"
