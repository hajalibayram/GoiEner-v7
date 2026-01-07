import tarfile
import zstandard
import os
from pathlib import Path


class Extractor:
    def __init__(self, file_path: str, dataset_dest_dir:str):
        self._file_path = file_path
        self._destination = dataset_dest_dir

        os.makedirs(dataset_dest_dir, exist_ok=True)

    @staticmethod
    def _safe_tar_filter(member, destination_dir):
        """Ensures extracted files do not escape the destination directory."""
        member_path = Path(destination_dir, member.name)
        if not str(member_path).startswith(str(destination_dir)):
            raise ValueError(f"Unsafe path detected: {member.name}")
        return member  # Allow extraction

    def decompress_tzst(self):
        input_file = Path(self._file_path)
        tar_path = Path(self._destination, input_file.stem)

        with open(input_file, 'rb') as compressed, open(tar_path, 'wb') as decompressed:
            decomp = zstandard.ZstdDecompressor()
            decomp.copy_stream(compressed, decompressed)

        if tarfile.is_tarfile(tar_path):
            with tarfile.open(tar_path, 'r') as tar:
                tar.extractall(path = self._destination, filter = self._safe_tar_filter)
            tar_path.unlink()
            print("Extraction complete")
            return
        print("Extraction failed")



