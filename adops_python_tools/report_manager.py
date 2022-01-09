import datetime
import logging
import os
import tempfile
from pathlib import PurePath

from googleads import errors

from config_reader import ConfigReader
from adops_ad_manager import AdOpsAdManagerClient

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

    def get_report(self, client: AdOpsAdManagerClient, report_type: str="placementPerformance") -> str:
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
    def create_directory(directory: str) -> str:
        if not os.path.exists(directory):
            logger.info(f"Path {directory} does not exist. Trying to create.")
            os.makedirs(directory)
            return directory
