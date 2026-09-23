"""
GH Archive -> AWS S3 Batch Ingestion Pipeline
=============================================

Downloads seven days of hourly GitHub event archives from GH Archive
and uploads each file to Amazon S3.

Dataset:
    February 1-7, 2024

Total:
    7 days x 24 hours = 168 hourly archive files

Pipeline:
    GH Archive
        ↓
    Download one .json.gz file
        ↓
    Temporary local storage
        ↓
    Upload to Amazon S3
        ↓
    Delete local copy
        ↓
    Continue to next file

S3 structure:
    raw/github/
        year=2024/
            month=02/
                day=01/
                    hour=00/
                        2024-02-01-0.json.gz
                    hour=01/
                        2024-02-01-1.json.gz
                    ...
                day=02/
                    ...
                ...
                day=07/

The script also checks whether a file already exists in S3.
Existing files are skipped, making the ingestion process safe to rerun.
"""


import os
from datetime import datetime, timedelta

import boto3
import requests
from botocore.exceptions import ClientError


# ============================================================
# CONFIGURATION
# ============================================================

# GH Archive date range
START_DATE = "2024-02-01"
END_DATE = "2024-02-07"

# AWS S3 bucket
BUCKET_NAME = "tahmid-gharchive-lakehouse"

# Temporary local folder
LOCAL_DATA_DIR = "data"

# GH Archive source
GH_ARCHIVE_BASE_URL = "https://data.gharchive.org"

# Download chunk size: 8 MB
CHUNK_SIZE = 8 * 1024 * 1024


# ============================================================
# INITIAL SETUP
# ============================================================

# Create local data directory if it does not exist.
os.makedirs(LOCAL_DATA_DIR, exist_ok=True)

# Create the S3 client.
s3 = boto3.client("s3")


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def generate_dates(start_date, end_date):
    """
    Generate every date between START_DATE and END_DATE,
    including both dates.
    """

    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    final_date = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= final_date:

        yield current_date

        current_date += timedelta(days=1)


def s3_object_exists(bucket_name, s3_key):
    """
    Check whether the archive already exists in S3.

    Returns:
        True  -> file exists
        False -> file does not exist
    """

    try:

        s3.head_object(
            Bucket=bucket_name,
            Key=s3_key
        )

        return True

    except ClientError as error:

        error_code = error.response["Error"]["Code"]

        if error_code in ("404", "NoSuchKey", "NotFound"):
            return False

        raise


def download_file(url, local_file):
    """
    Download one GH Archive file.

    The file is downloaded in chunks instead of loading
    the entire archive into RAM.

    Returns:
        File size in bytes.
    """

    downloaded_bytes = 0

    with requests.get(
        url,
        stream=True,
        timeout=300
    ) as response:

        # Stop if GH Archive returns an HTTP error.
        response.raise_for_status()

        with open(local_file, "wb") as file:

            for chunk in response.iter_content(
                chunk_size=CHUNK_SIZE
            ):

                if chunk:

                    file.write(chunk)

                    downloaded_bytes += len(chunk)

    return downloaded_bytes


def upload_file(local_file, bucket_name, s3_key):
    """
    Upload one local archive file to Amazon S3.
    """

    s3.upload_file(
        local_file,
        bucket_name,
        s3_key
    )


def bytes_to_mb(size_bytes):
    """
    Convert bytes to megabytes for readable console output.
    """

    return size_bytes / (1024 * 1024)


# ============================================================
# BUILD INGESTION TASK LIST
# ============================================================

tasks = []

for date_object in generate_dates(
    START_DATE,
    END_DATE
):

    date_string = date_object.strftime("%Y-%m-%d")

    for hour in range(24):

        tasks.append(
            (date_object, date_string, hour)
        )


total_files = len(tasks)


# ============================================================
# PIPELINE START
# ============================================================

print("\n")
print("=" * 64)
print("🚀 GH ARCHIVE → AWS S3 BATCH INGESTION")
print("=" * 64)

print(f"📅 Date range : {START_DATE} → {END_DATE}")
print(f"📦 Files      : {total_files}")
print(f"☁️  S3 bucket  : {BUCKET_NAME}")

print("=" * 64)


# Statistics
uploaded_count = 0
skipped_count = 0
failed_count = 0
uploaded_bytes = 0


# ============================================================
# PROCESS EACH HOURLY ARCHIVE
# ============================================================

for file_number, task in enumerate(
    tasks,
    start=1
):

    date_object, date_string, hour = task

    year = date_object.strftime("%Y")
    month = date_object.strftime("%m")
    day = date_object.strftime("%d")

    hour_folder = f"{hour:02d}"

    # --------------------------------------------------------
    # File name
    # --------------------------------------------------------

    file_name = (
        f"{date_string}-{hour}.json.gz"
    )


    # --------------------------------------------------------
    # GH Archive URL
    # --------------------------------------------------------

    url = (
        f"{GH_ARCHIVE_BASE_URL}/"
        f"{file_name}"
    )


    # --------------------------------------------------------
    # Temporary local file
    # --------------------------------------------------------

    local_file = os.path.join(
        LOCAL_DATA_DIR,
        file_name
    )


    # --------------------------------------------------------
    # S3 destination
    # --------------------------------------------------------

    s3_key = (
        f"raw/github/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"hour={hour_folder}/"
        f"{file_name}"
    )


    # --------------------------------------------------------
    # Progress header
    # --------------------------------------------------------

    progress_percentage = (
        file_number / total_files
    ) * 100


    print("\n")
    print("=" * 64)

    print(
        f"📦 FILE {file_number}/{total_files}"
        f" | {file_name}"
    )

    print("=" * 64)


    try:

        # ====================================================
        # CHECK S3 FIRST
        # ====================================================

        if s3_object_exists(
            BUCKET_NAME,
            s3_key
        ):

            skipped_count += 1

            print(
                "⏭️  Already exists in S3 — skipping"
            )

            print(
                f"📊 Progress | "
                f"{file_number}/{total_files} "
                f"({progress_percentage:.0f}%)"
            )

            continue


        # ====================================================
        # DOWNLOAD
        # ====================================================

        print(
            "⬇️  Downloading from GH Archive..."
        )

        file_size = download_file(
            url,
            local_file
        )

        file_size_mb = bytes_to_mb(
            file_size
        )

        print(
            f"✅ Downloaded | "
            f"{file_size_mb:.2f} MB"
        )


        # ====================================================
        # UPLOAD
        # ====================================================

        print(
            "☁️  Uploading to S3..."
        )

        upload_file(
            local_file,
            BUCKET_NAME,
            s3_key
        )

        uploaded_count += 1
        uploaded_bytes += file_size

        print(
            f"✅ Uploaded   | "
            f"s3://{BUCKET_NAME}/{s3_key}"
        )


        # ====================================================
        # DELETE LOCAL FILE
        # ====================================================

        os.remove(local_file)

        print(
            "🗑️  Local copy deleted"
        )


    # ========================================================
    # ERROR HANDLING
    # ========================================================

    except Exception as error:

        failed_count += 1

        print(
            f"❌ FAILED | {error}"
        )

        # Delete partially downloaded files.
        if os.path.exists(local_file):

            os.remove(local_file)

            print(
                "🧹 Partial local file removed"
            )


    # ========================================================
    # PROGRESS
    # ========================================================

    print(
        f"📊 Progress | "
        f"{file_number}/{total_files} "
        f"({progress_percentage:.0f}%)"
    )


# ============================================================
# FINAL SUMMARY
# ============================================================

uploaded_mb = bytes_to_mb(
    uploaded_bytes
)

uploaded_gb = uploaded_mb / 1024


print("\n")
print("=" * 64)
print("🏁 INGESTION COMPLETE")
print("=" * 64)

print(
    f"📦 Total files    : {total_files}"
)

print(
    f"✅ Uploaded       : {uploaded_count}"
)

print(
    f"⏭️  Skipped        : {skipped_count}"
)

print(
    f"❌ Failed         : {failed_count}"
)

print(
    f"☁️  Data uploaded  : {uploaded_gb:.2f} GB"
)

print(
    f"🪣 S3 bucket      : {BUCKET_NAME}"
)

print("=" * 64)