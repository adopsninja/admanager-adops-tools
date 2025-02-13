#!/usr/bin/env python
#
# Copyright 2014 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generates a refresh token for use with Ad Manager."""


import sys

from google_auth_oauthlib.flow import InstalledAppFlow
from oauthlib.oauth2.rfc6749.errors import InvalidGrantError

from constants import (_REDIRECT_URI, DEFAULT_CLIENT_ID,
                                      DEFAULT_CLIENT_SECRET, SCOPES)


class ClientConfigBuilder(object):
    """Helper class used to build a client config dict used in the OAuth 2.0 flow.
    """

    _DEFAULT_AUTH_URI = "https://accounts.google.com/o/oauth2/auth"
    _DEFAULT_TOKEN_URI = "https://accounts.google.com/o/oauth2/token"
    CLIENT_TYPE_WEB = "web"
    CLIENT_TYPE_INSTALLED_APP = "installed"

    def __init__(
        self,
        client_type=None,
        client_id=None,
        client_secret=None,
        auth_uri=_DEFAULT_AUTH_URI,
        token_uri=_DEFAULT_TOKEN_URI,
    ):
        self.client_type = client_type
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_uri = auth_uri
        self.token_uri = token_uri

    def Build(self):
        """Builds a client config dictionary used in the OAuth 2.0 flow."""
        if all(
            (
                self.client_type,
                self.client_id,
                self.client_secret,
                self.auth_uri,
                self.token_uri,
            )
        ):
            client_config = {
                self.client_type: {
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "auth_uri": self.auth_uri,
                    "token_uri": self.token_uri,
                }
            }
        else:
            raise ValueError("Required field is missing.")

        return client_config


def generate_refresh_token(client_id=DEFAULT_CLIENT_ID, client_secret=DEFAULT_CLIENT_SECRET, scopes=SCOPES):
    """Retrieve and display the access and refresh token."""
    client_config = ClientConfigBuilder(
        client_type=ClientConfigBuilder.CLIENT_TYPE_WEB,
        client_id=client_id,
        client_secret=client_secret,
    )

    flow = InstalledAppFlow.from_client_config(client_config.Build(), scopes=scopes)
    # Note that from_client_config will not produce a flow with the
    # redirect_uris (if any) set in the client_config. This must be set
    # separately.
    flow.redirect_uri = _REDIRECT_URI

    auth_url, _ = flow.authorization_url(prompt="consent")

    print(
        f"""Log into the Google Account you use to access your 
        Ad Manager account and go to the following URL: \n{auth_url}\n"""
        )
    print("After approving the token enter the verification code (if specified).")
    code = input("Code: ").strip()

    try:
        flow.fetch_token(code=code)
        # set enviromental variable: export OAUTHLIB_RELAX_TOKEN_SCOPE=True
        session = flow.authorized_session()
        profile_info = session.get("https://www.googleapis.com/userinfo/v2/me").json()
    except InvalidGrantError as ex:
        print(f"Authentication has failed: {ex}")
        sys.exit(1)

    print(f"Access token: {flow.credentials.token}")
    print(f"Refresh token: {flow.credentials.refresh_token}")

    user = {
        "email": profile_info.get("email"),
        "refresh_token": flow.credentials.refresh_token,
        "access_token": flow.credentials.token,
    }

    return user
