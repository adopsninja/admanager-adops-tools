#!/usr/bin/env python3
import logging
from adops_ad_manager import AdOpsAdManagerClient
from mcm_manager import mox_mcm_status_update

logger = logging.getLogger(__name__)

if __name__ == "__main__":
  client = AdOpsAdManagerClient("dariusz.siudak***REMOVED***", "***REMOVED***")
  # statement = client.build_statement("type", "CHILD_PUBLISHER", 1, False)
  # pub = client.get_items_by_statement(statement, client.company_service.getCompaniesByStatement)
  # print(pub)
  statement = client.build_statement("url", "druzina.si", 1, False)
  site = client.get_items_by_statement(statement, client.site_service.getSitesByStatement)
  print(site)

  mox_mcm_status_update("test")
