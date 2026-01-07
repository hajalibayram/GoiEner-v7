import os
import subprocess

def download_with_curl(url: str, dest_folder: str):
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

download_with_curl('https://zenodo.org/records/14949245/files/metadata.csv', 'data')
download_with_curl('https://zenodo.org/records/14949245/files/imputed_goiener_v7.tar.zst', 'data')