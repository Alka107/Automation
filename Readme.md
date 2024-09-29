

## Overview

This Python script automates the generation of CSV files containing dummy sensitive data and uploads them to an Amazon S3 bucket. It's designed to run on a schedule, allowing you to create and upload files automatically without manual intervention.

## Features

- Generates CSV files with  dummy data (Name, Email, Phone, SSN, Address).
- Uploads the generated files to a specified AWS S3 bucket.
- Allows configuration of the time interval for file generation and upload.
- Provides functionality to pause after a defined number of file uploads.



### Requirements



- The following Python packages installed:
  - `boto3`: For AWS S3 interactions.
  - `schedule`: For scheduling tasks.
  - `faker`: For generating dummy data.



pip install -r requirements.txt
