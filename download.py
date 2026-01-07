import os
import urllib.request

def download(url: str, dest_folder: str):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)

    filename = url.split('/')[-1].replace(" ", "_")
    file_path = os.path.join(dest_folder, filename)

    def progress_hook(block_num, block_size, total_size):
        downloaded = block_num * block_size
        if total_size > 0:
            percent = min(downloaded / total_size * 100, 100)
            print(f"\rDownloading: {percent:6.2f}%", end="")
        else:
            print(f"\rDownloaded {downloaded} bytes", end="")

    print("Saving to", os.path.abspath(file_path))
    urllib.request.urlretrieve(url, file_path, reporthook=progress_hook)
    print("\nDownload complete.")


download('https://zenodo.org/records/14949245/files/metadata.csv', 'data')
download('https://zenodo.org/records/14949245/files/imputed_goiener_v7.tar.zst', 'data')