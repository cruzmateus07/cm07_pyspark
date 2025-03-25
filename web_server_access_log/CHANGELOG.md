# Web Server Access Log Change Log

All notable changes to this project will be documented in this file.


# 1.0.0 - 2025-03-21

Project Launched

## Added

* [access_log.zip](/web_server_access_log/access_log.zip) PATCH Added log sample in zip format (.txt file ziped)
* [main.py](/web_server_access_log/main.py) PATCH Added logic to unzip log data, parse text file into spark dataframe using standard regexp format


# 1.1.0 - 2025-03-24

Unit tests added, modeling for saving parsed logs as table with minimal Arch best practices like a Hash PK and ingested_timestamp for control

## Added

* [web_server_access_log_modeling.png](/web_server_access_log/web_server_access_log_modeling.png) PATCH Added table modeling proposal
* [unit_tests.py](/web_server_access_log/unit_tests.py) PATCH Added a few unit tests to the parsed logs

## Changed

* [main.py](/web_server_access_log/main.py) PATCH Added logic to save the parsed log as table on the working directory