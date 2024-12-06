import unittest
from unittest.mock import mock_open, patch, MagicMock
import sys
import os

# Add the src directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
from Downloader import Downloader


class TestSaveToFile(unittest.TestCase):
    def test_save_to_file_success(self):
        # Arrange
        # Create a mock response object
        mock_response = MagicMock()
        mock_response.content = b"Mock file content"

        # Mock the open function
        with patch("builtins.open", mock_open()) as mocked_file:
            downloader = Downloader()

            # Act
            sut = downloader.save_to_file("mock_path.txt", mock_response)

            # Assert
            self.assertTrue(sut)

    def test_save_to_file_failure(self):
        # Arrange
        # Create a mock response object
        mock_response = MagicMock()
        mock_response.content = b"Mock file content"

        # Mock the open function and make write raise an exception
        with patch("builtins.open", mock_open()) as mocked_file:
            downloader = Downloader()
            mocked_file().write.side_effect = IOError("Mocked error")  # Simulate write failure

            # Act
            result = downloader.save_to_file("mock_path.txt", mock_response)

            # Assertions
            self.assertFalse(result)


class TestDownloader(unittest.TestCase):
    @patch("Downloader.requests.get")  # Mock requests.get
    def test_download_success_pdf(self, mock_get):
        # Mock response for a successful PDF download
        mock_response = MagicMock()
        mock_response.headers = {"content-type": "application/pdf"}
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        # Create instance and call the method
        downloader = Downloader()
        success, response = downloader.download("http://test.com/test.pdf")

        # Assertions
        self.assertTrue(success)
        self.assertEqual(response, mock_response)

    @patch("Downloader.requests.get")  # Mock requests.get
    def test_download_failure_non_pdf(self, mock_get):
        # Arrange
        mock_response = MagicMock()
        mock_response.headers = {"content-type": "text/html"}
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        downloader = Downloader()

        # Act
        success, response = downloader.download("http://test.com/test.html")

        # Assertions
        self.assertFalse(success)
        self.assertIsNone(response)

    @patch("Downloader.requests.get")  # Mock requests.get
    def test_download_failure_exception(self, mock_get):
        # Arrange
        mock_get.side_effect = Exception("Connection error")
        downloader = Downloader()

        # Act
        success, response = downloader.download("http://test.com/test.pdf")

        # Assertions
        self.assertFalse(success)
        self.assertIsNone(response)


class TestDownloadHandling(unittest.TestCase):

    @patch.object(Downloader, "download")  # Mock the 'download' method of Downloader
    @patch.object(Downloader, "save_to_file")  # Mock the 'save_to_file' method of Downloader
    def test_download_handling_success_main_url(self, mock_save_to_file, mock_download):
        # Arrange
        url = "http://main-url.com/file.pdf"
        alt_url = "http://alt-url.com/file.pdf"
        destination_path = "mock_path.pdf"

        # Mock download to return success for main URL
        mock_response = MagicMock()
        mock_download.return_value = (
            True,
            mock_response,
        )  # Simulate success for main URL
        mock_save_to_file.return_value = True  # Simulate successful save

        # Act
        downloader = Downloader()  # Replace with your actual class
        result = downloader.download_handling(url, destination_path, alt_url)

        # Assert
        self.assertTrue(result)
        mock_download.assert_called_once_with(url)  # Check that download was called with the main URL

    @patch.object(Downloader, "download")  # Mock the 'download' method of Downloader
    @patch.object(Downloader, "save_to_file")  # Mock the 'save_to_file' method of Downloader
    def test_download_handling_success_alt_url(self, mock_save_to_file, mock_download):
        # Arrange
        url = "http://main-url.com/file.pdf"
        alt_url = "http://alt-url.com/file.pdf"
        destination_path = "mock_path.pdf"

        # Mock download to fail for main URL and succeed for alt URL
        mock_response = MagicMock()
        mock_download.side_effect = [
            (False, None),
            (True, mock_response),
        ]  # Main URL fails, alt URL succeeds
        mock_save_to_file.return_value = True  # Simulate successful save

        # Act
        downloader = Downloader()  # Replace with your actual class
        result = downloader.download_handling(url, destination_path, alt_url)

        # Assert
        self.assertTrue(result)
        mock_download.assert_any_call(alt_url)  # Check that download was called with the alt URL

    @patch.object(Downloader, "download")  # Mock the 'download' method of Downloader
    @patch.object(Downloader, "save_to_file")  # Mock the 'save_to_file' method of Downloader
    def test_download_handling_no_url_or_alt_url(self, mock_save_to_file, mock_download):
        # Arrange
        url = None
        alt_url = None
        destination_path = "mock_path.pdf"

        # Act
        downloader = Downloader()  # Replace with your actual class
        result = downloader.download_handling(url, destination_path, alt_url)

        # Assert
        self.assertFalse(result)  # Expecting False since no URL is provided
        mock_download.assert_not_called()  # download should not be called
        mock_save_to_file.assert_not_called()  # save_to_file should not be called

    @patch.object(Downloader, "download")  # Mock the 'download' method of Downloader
    @patch.object(Downloader, "save_to_file")  # Mock the 'save_to_file' method of Downloader
    def test_download_handling_save_failure(self, mock_save_to_file, mock_download):
        # Arrange
        url = "http://main-url.com/file.pdf"
        alt_url = "http://alt-url.com/file.pdf"
        destination_path = "mock_path.pdf"

        # Mock download to return success for the main URL
        mock_response = MagicMock()
        mock_download.return_value = (
            True,
            mock_response,
        )  # Simulate success for main URL
        mock_save_to_file.return_value = False  # Simulate save failure

        # Act
        downloader = Downloader()  # Replace with your actual class
        result = downloader.download_handling(url, destination_path, alt_url)

        # Assert
        self.assertFalse(result)  # Expecting False due to save failure
        mock_download.assert_called_once_with(url)  # Check that download was called with the main URL


# Run the tests
if __name__ == "__main__":
    unittest.main()
