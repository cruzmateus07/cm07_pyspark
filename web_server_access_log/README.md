# Web Server Access Log Analysis

A Spark solution to parse a standard Web Server Access Log text file into a Spark DataFrame and futher analyse it's content.

This distribution is supposed to be used directly into the user's machine terminal.


# Requirements

[requirements.txt](/web_server_access_log/requirements.txt)


# Setup

## 1. Install Requirements

Since this distribution is supposed to be used directly into own machine terminal, make sure it has:
* Python 3.13.1 Installed
* access_log.zip file in the same folder as main.py (working directory)
* Install requirements -> pip install -r requirements.txt


# Execution

## 1. Run main.py

If Setup step was correctly executed, just run main.py file in your terminal.


# Current Features

The solution provides the analysis of a few indicators for the log file provided such as:
  * Top 10 Client IPs based on access numbers
  * Top 6 accessed endpoints (disregarding the ones that are files)
  * Distinct Client IPs
  * How many different days are in the file
  * Request's Volume analysis such as - Total Volume, Max Volume and Min Volume Size, Average Volume Size


# Change Log

[CHANGELOG.md](/web_server_access_log/CHANGELOG.md)
