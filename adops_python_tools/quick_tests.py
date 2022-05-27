#!/usr/bin/env python3
import logging
from adops_ad_manager import AdOpsAdManagerClient
from googleads.ad_manager import StatementBuilder

from constants import API_VERSION
from notification_manager import mox_mcm_status_update


logger = logging.getLogger(__name__)

if __name__ == "__main__":
    mox_mcm_status_update("test")
