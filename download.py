"""download.py

Small helper to download files with curl into a destination folder.

This script is intentionally minimal: it wraps a subprocess call to curl and is
meant for one-off downloads used during data preparation.
"""

import os
import subprocess

def download_with_curl(url: str, dest_folder: str):
    """Download `url` into `dest_folder` using curl.

    This function creates `dest_folder` if missing, constructs a filename from
    the URL and invokes `curl -L -C - -o <file> <url>` to follow redirects and
    resume partial downloads.

    Parameters
    ----------
    url : str
        HTTP/HTTPS URL to download.
    dest_folder : str
        Directory to save the downloaded file into.
    """
    os.makedirs(dest_folder, exist_ok=True)

    filename = url.split('/')[-1].replace(" ", "_")
    file_path = os.path.join(dest_folder, filename)

    print("Saving to", os.path.abspath(file_path))

    cmd = [
        "curl",
        "-L",        # follow redirects (Zenodo needs this)
        "-C", "-",   # resume
        "-o", os.path.join(dest_folder, url.split("/")[-1]),
        url
    ]

    subprocess.run(cmd, check=True)
    print("\nDownload complete.")

# Example one-shot downloads used by the original project
download_with_curl('https://zenodo.org/records/14949245/files/metadata.csv', 'data')
download_with_curl('https://zenodo.org/records/14949245/files/imputed_goiener_v7.tar.zst', 'data')