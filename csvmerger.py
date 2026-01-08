import csv
import glob
import os

class CSVMerger:
    def __init__(self, files_list, source_folder, output_file):
        self._files = files_list
        self._source_folder = source_folder
        self._output_file = output_file



    def combine_csv_files(self):
        if self._files:
            csv_files = self._files
        else:
            # Get a list of all .csv files in the source folder
            csv_files = glob.glob(f"{self._source_folder}/*.csv")

        # Open the output file in write mode
        with open(self._output_file, mode='w', newline='') as outfile:
            writer = csv.writer(outfile)

            # Variable to track if headers have been written
            headers_written = False

            # Loop through each .csv file
            for file_path in csv_files:
                # Extract the id from the file name (without extension)
                id = file_path.split("/")[-1].split(".")[0]

                if not os.path.isfile(file_path):
                    continue

                # Open each CSV file in read mode
                with open(file_path, mode='r', newline='') as infile:
                    reader = csv.reader(infile)

                    # Write headers only once
                    if not headers_written:
                        headers = next(reader)  # Read the header of the first file
                        headers.append("id")
                        writer.writerow(headers)  # Write the header to the output file
                        headers_written = True
                    else:
                        next(reader)  # Skip the header of subsequent files

                    # Write the data rows to the output file
                    for row in reader:
                        row.append(id)  # Add the user_id to each row
                        writer.writerow(row)
