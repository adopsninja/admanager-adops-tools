# Ad Manager Ad Operations Tools
This repository contains set of CLI tools to work with Google Ad Manager (GAM). Written to automate the boring stuff of Ad Operation role. Since some of the data are anonymized, the tools are not ready to use out of the box. However, the code can be used as a reference to build your own tools.

### What business problems does it solve?

#### Keeps performance ad units placements up to date
Performance placements are used to group ad units based on their performance in terms of CTR, Viewability and Ad requests. It is important to keep placement updated on daily basis to ensure it maximizes the revenue by direct/programmatic deals served on them.

Key aspects:
- Configurable using YAML file
- Pulls the data from GAM in form of `csv` report
- Cleans and transforms the data according to the business rules (each placement has different rules e.g. Viewability > 80%, 50000 ad requests monthly)
- Updates the placements in GAM

#### Multiple Customer Management (MCM)
Publishers can delegate management of their GAM network to a third-party publisher upon request This establishes a parent-child relationship, where the network that requests access is the "parent publisher," and the network that grants access is the "child publisher". The relationship is valid after positive whitelist from GAM support team.

Key aspects:
- Configurable using YAML file
- Stores data in Google Spreadsheet exposed to business users
  - Business users fills basic data, like publisher name, email, domains, child network code for new partnerships
- Checks current status of the partnership in GAM
- Requests whitelist process from GAM support team in bulk
- Updates the status in Google Spreadsheet so business users can track the progress and continue implementaion with Ad Operations team

#### Prebid Order Creator
Prebid is an open-source header bidding solution that allows publishers to maximize their revenue by selling their ad inventory programmatically to a large number of advertisers. One of setup process steps of Prebid is to create Prebid orders in GAM. Ammount of orders can be overwhelming depending on price granularity that publisher wants to achieve. This tool allows to create Prebid orders in bulk with two price buckets: 0.01 - 20.00 with 0.01 step and 21.00 - 100.00 with 1.00 step. Creates total of 5 orders and **2080** line items.

Key aspects:
- Configurable using YAML file
- Creates Prebid orders, line items, key-values and creatives in GAM nescassary for Prebid setup

#### Google Ad Exchange (ADX) fillrate report
ADX is an ad exchange that allows publishers to maximize their revenue by selling their ad inventory programmatically to a large number of advertisers. The fillrate is the percentage of ad requests that are filled with an ad. It is important to keep the fillrate high to maximize the revenue.

Key aspects:
- Pulls the data from GAM in form of `csv` report
- Cleans and transforms the data according to the business rules (fillrate < 10%)
- Sends an email to the user group with the report of domains with low fillrate that requires action

#### Reporting
Pulls the data from GAM in form of `csv` report based on YAML configuration file.

- Configurable using YAML file
- Pulling reports from GAM

#### Line item size update
Small utility to update line item sizes in GAM.


#### Activate/Archive ad units
Small utility to activate or archive ad units in GAM.

### Main tech used
- Python
- Pandas
- Google Ad Manager API
- Google Sheets API
- Google Drive API
- Google Mail API
- OAuth2
- Sqlite3
