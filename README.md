# SFTP to S3 Data Pipeline

## Project Overview
This project extracts files from an SFTP server and uploads them to AWS S3.

## Tech Stack
- Python
- Paramiko (SFTP)
- Boto3 (AWS S3)
- Logging

## How It Works
1. Connect to SFTP
2. Download files
3. Upload to S3 bucket
4. Log success/failure

## How to Run
1. Install requirements:
   pip install -r requirements.txt

2. Run:
   python main.py
