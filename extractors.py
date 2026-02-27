"""extractors.py

Safe extraction helpers for compressed Goiener archives.

Provides an Extractor class that decompresses .tar.zst archives and extracts the
contained tar safely to a destination directory while preventing path-traversal
attacks.
"""

import os
import tarfile
from pathlib import Path

import zstandard


class Extractor:
    """Utility for decompressing .tar.zst archives and extracting them safely.

    Parameters
    ----------
    file_path : str
        Path to the .tar.zst file to decompress.
    dataset_dest_dir : str
        Destination directory where the tar will be written and extracted.
    """
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
        """Decompress the .tar.zst archive and extract its content.

        The method creates a temporary tar file adjacent to the destination and
        uses zstandard to decompress into it. The resulting tar is opened and
        extracted with a safety filter to avoid path traversal.
        """
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

