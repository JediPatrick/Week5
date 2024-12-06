import unittest
import os
import tempfile
import polars as pl
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
from Polar_File_Handler import FileHandler  # Assuming the class is in FileHandler.py


class TestFileHandlerIntegration(unittest.TestCase):

    def setUp(self):
        # Create temporary Excel files for URL and meta data
        self.url_file = tempfile.NamedTemporaryFile(delete=False, mode="wb", suffix=".xlsx")
        self.meta_file = tempfile.NamedTemporaryFile(delete=False, mode="wb", suffix=".xlsx")

        # Create sample URL file data (this will be written as Excel)
        url_data = {"BRnum": [1, 2, 3], "Pdf_URL": ["url1", "url2", "url3"], "Report Html Address": ["html1", "html2", "html3"]}
        url_df = pl.DataFrame(url_data)
        url_df.write_excel(self.url_file.name)

        # Create sample meta data with "pdf_downloaded" column (this will also be written as Excel)
        meta_data = {"BRnum": [1, 2, 3], "pdf_downloaded": ["yes", "no", "yes"]}
        meta_df = pl.DataFrame(meta_data)
        meta_df.write_excel(self.meta_file.name)

    def tearDown(self):
        # Close and delete the temporary files if they exist
        try:
            if os.path.exists(self.url_file.name):
                # Make sure the file is closed before removing
                self.url_file.close()
                os.remove(self.url_file.name)

            if os.path.exists(self.meta_file.name):
                # Make sure the file is closed before removing
                self.meta_file.close()
                os.remove(self.meta_file.name)
        except Exception as e:
            print(f"Error during cleanup: {e}")
            raise

    def test_import_data_files_integration(self):
        # Create an instance of FileHandler with the paths to the temporary files
        handler = FileHandler(self.url_file.name, self.meta_file.name, "destination", 4)

        # Call the method
        file_data, meta_data = handler.import_data_files()

        # Verify the file_data (after join and filter)
        self.assertEqual(file_data.shape[0], 1)  # Only one record should remain after filtering
        self.assertEqual(file_data["BRnum"].to_list(), [2])  # BRnum 2 should be removed (pdf_downloaded == "no")

        # Verify the meta_data (filtered with pdf_downloaded == "yes")
        self.assertEqual(meta_data.shape[0], 2)  # Only two rows should remain (pdf_downloaded == "yes")
        self.assertEqual(meta_data["BRnum"].to_list(), [1, 3])  # BRnum 1 and 3 should remain


if __name__ == "__main__":
    unittest.main()
