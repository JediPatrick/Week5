import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path
import polars as pl
import sys
import os
from typing import Optional
from queue import Queue

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
from Downloader import Downloader
from Polar_File_Handler import FileHandler

class Tests_Polar_File_Handler(unittest.TestCase):
    def setUp(self):
        self.mock_handler = MagicMock()
        self.mock_handler.destination = "mock_destination"
        self.mock_handler.meta_file = "mock_meta.xlsx"
        self.mock_handler.ID = "mock_id"
        self.mock_handler.download_status_list = [("id1", "yes"), ("id2", "no")]

    @patch("pathlib.Path.mkdir")
    @patch("polars.DataFrame")
    @patch("polars.concat")
    @patch("Polar_File_Handler.Workbook")  # Mock Workbook where it is imported in your handler code
    def test_both_file_and_meta_data_non_empty(self, mock_workbook, mock_concat, mock_dataframe, mock_mkdir):
        # Mock data
        file_data_mock = MagicMock()
        file_data_mock.is_empty.return_value = False
        meta_data_mock = MagicMock()
        meta_data_mock.is_empty.return_value = False
        self.mock_handler.import_data_files.return_value = (file_data_mock, meta_data_mock)

        # Mock `thread_handler`
        self.mock_handler.thread_handler = MagicMock()

        # Instantiate and run
        handler_instance = FileHandler("url_file_name", "report_file_name", "destination", 1)
        handler_instance.destination = self.mock_handler.destination
        handler_instance.meta_file = self.mock_handler.meta_file
        handler_instance.ID = self.mock_handler.ID
        handler_instance.download_status_list = self.mock_handler.download_status_list
        handler_instance.import_data_files = self.mock_handler.import_data_files
        handler_instance.thread_handler = self.mock_handler.thread_handler

        handler_instance.handler()

        # Assertions
        mock_mkdir.assert_called_once_with(exist_ok=True)
        self.mock_handler.thread_handler.assert_called_once_with(file_data_mock)
        mock_concat.assert_called_once_with([mock_dataframe.return_value, meta_data_mock], rechunk=True)
        mock_workbook.assert_called_once_with(self.mock_handler.meta_file)

    @patch("pathlib.Path.mkdir")
    @patch("polars.DataFrame")
    @patch("Polar_File_Handler.Workbook")  # Mock Workbook where it is imported in your handler code
    def test_file_data_empty(self, mock_workbook, mock_dataframe, mock_mkdir):
        # Mock data
        file_data_mock = MagicMock()
        file_data_mock.is_empty.return_value = True
        meta_data_mock = MagicMock()
        meta_data_mock.is_empty.return_value = False
        self.mock_handler.import_data_files.return_value = (file_data_mock, meta_data_mock)
    
        # Mock `thread_handler`
        self.mock_handler.thread_handler = MagicMock()
    
        # Instantiate and run
        handler_instance = FileHandler("url_file_name", "report_file_name", "destination", 1)
        handler_instance.destination = self.mock_handler.destination
        handler_instance.meta_file = self.mock_handler.meta_file
        handler_instance.ID = self.mock_handler.ID
        handler_instance.download_status_list = self.mock_handler.download_status_list
        handler_instance.import_data_files = self.mock_handler.import_data_files
        handler_instance.thread_handler = self.mock_handler.thread_handler
    
        handler_instance.handler()
    
        # Assertions
        self.mock_handler.thread_handler.assert_not_called()
        mock_dataframe.assert_not_called()
        mock_workbook.assert_not_called()

class TestFileHandlerThreadHandler(unittest.TestCase):
    def setUp(self):
        # Setup the FileHandler instance for tests
        self.file_handler = FileHandler("url_file_name", "report_file_name", "destination", 4)
        self.file_handler.ID = "mock_id"
        self.file_handler.destination = "mock_destination"
        self.file_handler.number_of_threads = 3  # Assuming 3 threads for this test

    @patch("Polar_File_Handler.Queue")
    def test_queue_population(self, mock_queue):
        # Mock file_data with 25 rows (to check the behavior when there are multiple rows)
        file_data_mock = MagicMock()
        file_data_mock.rows.return_value = [
            {"Report Html Address": f"alt_link_{i}", "Pdf_URL": f"pdf_url_{i}", "mock_id": f"id_{i}"}
            for i in range(25)
        ]
    
        # Mock the queue
        mock_queue_instance = MagicMock()
        mock_queue.return_value = mock_queue_instance
    
        # Run the thread_handler function
        self.file_handler.thread_handler(file_data_mock)
    
        # Assertions: Verify the correct number of items were added to the queue (25 items)
        self.assertEqual(mock_queue_instance.put.call_count, 25)
    
        # Verify that the correct data was added to the queue for each row
        for i in range(25):
            mock_queue_instance.put.assert_any_call([f"pdf_url_{i}", "mock_destination", f"id_{i}", f"alt_link_{i}"])
    
    @patch("threading.Thread")
    def test_threads_creation(self, mock_thread):
        # Mock file_data with 25 rows (to check thread creation)
        file_data_mock = MagicMock()
        file_data_mock.rows.return_value = [
            {"Report Html Address": f"alt_link_{i}", "Pdf_URL": f"pdf_url_{i}", "mock_id": f"id_{i}"}
            for i in range(25)
        ]
    
        # Run the thread_handler function
        with patch("Polar_File_Handler.Queue") as mock_queue:
            mock_queue_instance = MagicMock()
            mock_queue.return_value = mock_queue_instance
    
            # Run thread_handler method
            self.file_handler.thread_handler(file_data_mock)
    
            # Assertions: Ensure the correct number of threads are created (3 threads in this case)
            self.assertEqual(mock_thread.call_count, self.file_handler.number_of_threads)
    
            # Verify each thread is started with the correct arguments
            for i in range(self.file_handler.number_of_threads):
                mock_thread.assert_any_call(target=self.file_handler.thread_downloader, args=(mock_queue_instance,))
    
    @patch("Polar_File_Handler.Queue")
    def test_queue_join_called(self, mock_queue):
        # Mock file_data with 25 rows (to check the queue join)
        file_data_mock = MagicMock()
        file_data_mock.rows.return_value = [
            {"Report Html Address": f"alt_link_{i}", "Pdf_URL": f"pdf_url_{i}", "mock_id": f"id_{i}"}
            for i in range(25)
        ]
    
        # Mock the queue
        mock_queue_instance = MagicMock()
        mock_queue.return_value = mock_queue_instance
    
        # Run the thread_handler function
        self.file_handler.thread_handler(file_data_mock)
    
        # Assertions: Ensure that the join method is called once to wait for threads to complete
        mock_queue_instance.join.assert_called_once()



class TestFileHandlerImportDataFiles(unittest.TestCase):
    
    @patch("os.path.exists")
    @patch("polars.read_excel")
    def test_import_data_files_with_meta_file(self, mock_read_excel, mock_exists):
        # Setup mock return values for the url_data and meta_data
        mock_exists.return_value = True  # Simulate that the meta file exists

        # Simulate reading the URL file
        url_data_mock = pl.DataFrame({
            "BRnum": [1, 2, 3],
            "Pdf_URL": ["url1", "url2", "url3"],
            "Report Html Address": ["html1", "html2", "html3"]
        })

        # Simulate reading the meta data file with pdf_downloaded = "yes"
        meta_data_mock = pl.DataFrame({
            "BRnum": [1, 2],
            "pdf_downloaded": ["yes", "yes"]
        })

        # Mock the return values
        mock_read_excel.side_effect = [url_data_mock, meta_data_mock]

        # Create an instance of FileHandler
        handler = FileHandler("url_file.xlsx", "meta_file.xlsx", "destination", 4)

        # Call the method
        file_data, meta_data = handler.import_data_files()

        # Assertions to check filtering results
        self.assertEqual(file_data.shape[0], 1)  # Only one file should be left after join
        self.assertEqual(file_data["BRnum"].to_list(), [3])  # Only the record with BRnum 3 remains

        # Check if meta_data is correctly filtered (pdf_downloaded == "yes")
        self.assertEqual(meta_data.shape[0], 2)  # Both entries should remain since pdf_downloaded is "yes"
    
    @patch("os.path.exists")
    @patch("polars.read_excel")
    def test_import_data_files_no_meta_file(self, mock_read_excel, mock_exists):
        # Simulate that the meta file doesn't exist
        mock_exists.return_value = False

        # Simulate reading the URL file
        url_data_mock = pl.DataFrame({
            "BRnum": [1, 2, 3],
            "Pdf_URL": ["url1", "url2", "url3"],
            "Report Html Address": ["html1", "html2", "html3"]
        })

        # Mock the return value of the URL file only
        mock_read_excel.return_value = url_data_mock

        # Create an instance of FileHandler
        handler = FileHandler("url_file.xlsx", "meta_file.xlsx", "destination", 4)

        # Call the method
        file_data, meta_data = handler.import_data_files()

        # Assertions to check filtering results
        self.assertEqual(file_data.shape[0], 3)  # No filtering should happen, all 3 rows should remain
        self.assertEqual(file_data["BRnum"].to_list(), [1, 2, 3])  # All records should be present
        self.assertEqual(meta_data.shape[0], 0)  # meta_data should be empty since the meta file doesn't exist
    


#class TestFileHandlerThreadDownloader(unittest.TestCase):
#    def setUp(self):
#        try:
#            # Initialize the file handler instance
#            self.file_handler = FileHandler("url_file_name", "report_file_name", "destination", 4)
#            self.file_handler.ID = "mock_id"
#            self.file_handler.destination = "mock_destination"
#            self.file_handler.add_download_status = MagicMock()  # Mock the add_download_status method
#        except Exception as e:
#            print(f"Error in setUp: {e}")
#            raise
 #
 #  @patch("queue.Queue.get")
 #  @patch("Downloader.Downloader.download_handling")
 #  def test_successful_download(self, mock_download_handling, mock_queue_get):
 #      # Mock the queue to return specific values
 #      mock_queue_get.return_value = ("mock_link", "mock_destination", "mock_name", "mock_alt_link")
 #
 #      # Mock download_handling to return True (successful download)
 #      mock_download_handling.return_value = True
 #
 #      # Run thread_downloader method
 #      queue = Queue()
 #      queue.put(("mock_link", "mock_destination", "mock_name", "mock_alt_link"))
 #      self.file_handler.thread_downloader(queue)
 #
 #      # Assertions: Ensure add_download_status was called with "yes" for successful download
 #      self.file_handler.add_download_status.assert_called_once_with(("mock_name", "yes"))
 #
 #   @patch("queue.Queue.get")
 #   @patch("Downloader.Downloader.download_handling")
 #   def test_failed_download(self, mock_download_handling, mock_queue_get):
 #       # Mock the queue to return specific values
 #       mock_queue_get.return_value = ("mock_link", "mock_destination", "mock_name", "mock_alt_link")
 #
 #       # Mock download_handling to return False (failed download)
 #       mock_download_handling.return_value = False
 #
 #       # Run thread_downloader method
 #       queue = Queue()
 #       queue.put(("mock_link", "mock_destination", "mock_name", "mock_alt_link"))
 #       self.file_handler.thread_downloader(queue)
 #
 #       # Assertions: Ensure add_download_status was called with "no" for failed download
 #       self.file_handler.add_download_status.assert_called_once_with(("mock_name", "no"))
 #
 #   @patch("queue.Queue.get")
 #   def test_empty_queue(self, mock_queue_get):
 #       # Mock the queue to return nothing (empty queue)
 #       mock_queue_get.return_value = None  # No items in the queue
 #
 #       # Run thread_downloader method with an empty queue
 #       queue = Queue()
 #       self.file_handler.thread_downloader(queue)
 #
 #       # Assertions: Ensure that add_download_status was never called
 #       self.file_handler.add_download_status.assert_not_called()
 #
 #   @patch("queue.Queue.get")
 #   @patch("Downloader.Downloader.download_handling")
 #   def test_multiple_items_in_queue(self, mock_download_handling, mock_queue_get):
 #       # Mock the queue to return multiple items
 #       mock_queue_get.side_effect = [
 #           ("mock_link1", "mock_destination", "mock_name1", "mock_alt_link1"),
 #           ("mock_link2", "mock_destination", "mock_name2", "mock_alt_link2"),
 #           ("mock_link3", "mock_destination", "mock_name3", "mock_alt_link3"),
 #       ]
 #
 #       # Mock download_handling to return True (successful download) for each item
 #       mock_download_handling.return_value = True
 #
 #       # Run thread_downloader method with multiple items in the queue
 #       queue = Queue()
 #       queue.put(("mock_link1", "mock_destination", "mock_name1", "mock_alt_link1"))
 #       queue.put(("mock_link2", "mock_destination", "mock_name2", "mock_alt_link2"))
 #       queue.put(("mock_link3", "mock_destination", "mock_name3", "mock_alt_link3"))
 #       self.file_handler.thread_downloader(queue)
 #
 #       # Assertions: Ensure add_download_status was called for all items in the queue
 #       self.file_handler.add_download_status.assert_any_call(("mock_name1", "yes"))
 #       self.file_handler.add_download_status.assert_any_call(("mock_name2", "yes"))
 #       self.file_handler.add_download_status.assert_any_call(("mock_name3", "yes"))
 #       self.assertEqual(self.file_handler.add_download_status.call_count, 3)
#


# Run the tests
if __name__ == "__main__":
    unittest.main()
