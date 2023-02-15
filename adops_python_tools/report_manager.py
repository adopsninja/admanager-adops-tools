import datetime
import logging
import os
import tempfile
from pathlib import PurePath

import pandas
from googleads import errors

from adops_ad_manager import AdOpsAdManagerClient
from config_reader import ConfigReader

logger = logging.getLogger(__name__)

class ReportManager:
    def __init__(self, config_path) -> None:
        self.config = ConfigReader(config_path).read_yaml_config()

    def set_report_job(self, report_type: str="placementPerformance") -> dict:
        if self.config[report_type]["dateRangeType"] == "CUSTOM_DATE":
            default_date_range = {
                "startDate": datetime.date.today() - datetime.timedelta(30),
                "endDate": datetime.date.today() - datetime.timedelta(1),
            }
            self.config[report_type].update(default_date_range)

        query = {
            "reportQuery": self.config[report_type]
        }
        return query

    def get_report(self, client: AdOpsAdManagerClient, report_type: str="placementPerformance"):
        output_path = PurePath(
            self.config["outputFolderPath"], datetime.datetime.now().strftime("%d%m%Y_%H%M")
        )
        self.create_directory(output_path)
        report_job = self.set_report_job(report_type)
        report_path = PurePath(
            output_path,
            f"{client.network_service.getCurrentNetwork()['networkCode']}_{report_type}_{self.config[report_type]['startDate']}_{self.config[report_type]['endDate']}.csv.gz",
        )

        try:
            report_job_id = client.report_downloader.WaitForReport(report_job)
            with tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=False, dir=PurePath(output_path),) as report_file:
                client.report_downloader.DownloadReportToFile(report_job_id, "CSV_DUMP", report_file)
                temp_file_path = report_file.name
            try:
                os.rename(temp_file_path, report_path)
            except FileExistsError:
                logger.info(f"Report already exist. Deleting old report {report_path}")
                os.remove(report_path)
                os.rename(temp_file_path, report_path)
            finally:
                logger.info(f"Report job with id {report_job_id} downloaded to: {report_path}")
        except errors.AdManagerReportError as e:
            logger.error(f"Failed to generate report. Error was: {e}")

        return report_path

    @staticmethod
    def create_directory(directory: PurePath):
        if not os.path.exists(directory):
            logger.info(f"Path {directory} does not exist. Trying to create.")
            os.makedirs(directory)
            return directory


def process_adx_fillrate_report(report_path: PurePath, min_adrequests: int = 50000) -> pandas.DataFrame:
    excluded_domains = "***REMOVED***"
    dataframe = pandas.read_csv(report_path, compression="gzip")
    dataframe.rename(
            columns={
                "Dimension.AD_EXCHANGE_DATE": "Date",
                "Dimension.AD_EXCHANGE_URL": "Domain",
                "Dimension.AD_EXCHANGE_PRODUCT_NAME": "Product",
                "Column.AD_EXCHANGE_AD_REQUESTS": "Ad requests",
                "Column.AD_EXCHANGE_COVERAGE": "Coverage",
            },
            inplace=True
        )
    dataframe = dataframe.loc[~dataframe["Domain"].str.contains(excluded_domains)]
    dataframe = dataframe[(dataframe["Ad requests"] > min_adrequests) & (dataframe["Coverage"] <= 0.1)]
    dataframe["Coverage"] = dataframe["Coverage"].apply(lambda x: "{0:.2f} %".format(x * 100))
    dataframe = dataframe.sort_values(["Coverage"], ascending=False).reset_index(drop=True)
    dataframe.index += 1

    return dataframe

def dataframe_to_html(dataframe):
    if dataframe is None:
        return None
    result = """<html><head><style>
    h2 {text-align: center;font-family: Helvetica, Arial, sans-serif;}
    table, th, td {border: 1px solid black;border-collapse: collapse;}
    th, td {padding: 1px;text-align: left;font-family: Helvetica, Arial, sans-serif;font-size: 90%;}
    table tbody tr:hover {background-color: #dddddd;}
    .wide {width: 35%;}
    </style></head><body>"""

    if type(dataframe) == pandas.io.formats.style.Styler:  # type: ignore
        result += dataframe.render()
    else:
        result += dataframe.to_html(classes="wide", escape=False)
    result += """</body></html>"""

    return result.replace("\n", "")

