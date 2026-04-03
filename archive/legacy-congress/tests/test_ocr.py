"""Tests for OCR module."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cppi.ocr import (
    OCRResult,
    estimate_ocr_confidence,
    is_paper_filing,
    is_tesseract_available,
    ocr_filing,
    ocr_image,
)


class TestOCRResult:
    """Test OCRResult dataclass."""

    def test_successful_result(self):
        """Test a successful OCR result."""
        result = OCRResult(
            text="This is extracted text with more than fifty characters to be considered successful.",
            confidence=0.8,
            source_format="pdf",
            page_count=3,
            warnings=[],
        )
        assert result.is_successful is True

    def test_unsuccessful_short_text(self):
        """Test unsuccessful result with short text."""
        result = OCRResult(
            text="Too short",
            confidence=0.8,
            source_format="pdf",
            page_count=1,
            warnings=[],
        )
        assert result.is_successful is False

    def test_unsuccessful_low_confidence(self):
        """Test unsuccessful result with low confidence."""
        result = OCRResult(
            text="This text is long enough but the confidence score is too low to be useful.",
            confidence=0.2,
            source_format="pdf",
            page_count=1,
            warnings=[],
        )
        assert result.is_successful is False


class TestIsPaperFiling:
    """Test paper filing detection."""

    def test_paper_filing_id(self):
        """Test detecting paper filing IDs."""
        assert is_paper_filing("82212345") is True
        assert is_paper_filing("822abcde") is True

    def test_electronic_filing_id(self):
        """Test non-paper filing IDs."""
        assert is_paper_filing("20241234") is False
        assert is_paper_filing("2024abcd") is False

    def test_empty_filing_id(self):
        """Test empty filing ID."""
        assert is_paper_filing("") is False
        assert is_paper_filing(None) is False


class TestEstimateOCRConfidence:
    """Test OCR confidence estimation."""

    def test_empty_text(self):
        """Test empty text returns 0."""
        assert estimate_ocr_confidence("") == 0.0
        assert estimate_ocr_confidence("   ") == 0.0

    def test_short_text(self):
        """Test short text returns 0."""
        assert estimate_ocr_confidence("Too short") == 0.0

    def test_normal_text(self):
        """Test normal text gets moderate confidence."""
        text = "This is a sample congressional filing with purchase and sale transactions. "
        text += "The filer reported $10,000 in stock purchases."
        confidence = estimate_ocr_confidence(text)
        assert 0.3 <= confidence <= 1.0

    def test_longer_text_higher_confidence(self):
        """Test longer text gets higher confidence."""
        short_text = "This is a sample text with purchase transaction stock sale. " * 5
        long_text = short_text * 10

        short_conf = estimate_ocr_confidence(short_text)
        long_conf = estimate_ocr_confidence(long_text)

        assert long_conf >= short_conf

    def test_garbage_text_lower_confidence(self):
        """Test garbage characters reduce confidence."""
        clean_text = "This is clean text with purchase and sale information. " * 10
        garbage_text = "This is []{}|\\<>~ garbage text with purchase. " * 10

        clean_conf = estimate_ocr_confidence(clean_text)
        garbage_conf = estimate_ocr_confidence(garbage_text)

        assert clean_conf > garbage_conf

    def test_expected_patterns_boost_confidence(self):
        """Test expected patterns increase confidence."""
        generic_text = "Lorem ipsum dolor sit amet consectetur adipiscing elit. " * 10
        filing_text = "Purchase of stock for spouse, joint account transaction sale. " * 10

        generic_conf = estimate_ocr_confidence(generic_text)
        filing_conf = estimate_ocr_confidence(filing_text)

        assert filing_conf > generic_conf


class TestOCRImage:
    """Test image OCR functionality."""

    @patch("cppi.ocr.HAS_TESSERACT", False)
    def test_no_tesseract(self):
        """Test graceful handling when Tesseract not installed."""
        with tempfile.NamedTemporaryFile(suffix=".png") as f:
            result = ocr_image(Path(f.name))
            assert result.text == ""
            assert result.confidence == 0.0
            assert "not installed" in result.warnings[0]

    @patch("cppi.ocr.HAS_TESSERACT", True)
    @patch("subprocess.run")
    def test_successful_ocr(self, mock_run):
        """Test successful OCR with mocked Tesseract."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Sample OCR text with purchase and sale information for the congressional filing",
            stderr="",
        )

        with tempfile.NamedTemporaryFile(suffix=".png") as f:
            result = ocr_image(Path(f.name))
            assert len(result.text) > 0
            assert result.confidence > 0

    @patch("cppi.ocr.HAS_TESSERACT", True)
    @patch("subprocess.run")
    def test_tesseract_error(self, mock_run):
        """Test handling Tesseract errors."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Tesseract error occurred",
        )

        with tempfile.NamedTemporaryFile(suffix=".png") as f:
            result = ocr_image(Path(f.name))
            assert result.text == ""
            assert "error" in result.warnings[0].lower()

    @patch("cppi.ocr.HAS_TESSERACT", True)
    @patch("subprocess.run")
    def test_tesseract_timeout(self, mock_run):
        """Test handling Tesseract timeout."""
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired("tesseract", 120)

        with tempfile.NamedTemporaryFile(suffix=".png") as f:
            result = ocr_image(Path(f.name))
            assert result.text == ""
            assert "timeout" in result.warnings[0].lower()


class TestOCRFiling:
    """Test filing OCR dispatcher."""

    @patch("cppi.ocr.ocr_pdf")
    def test_pdf_dispatch(self, mock_ocr_pdf):
        """Test PDF files are dispatched to ocr_pdf."""
        mock_ocr_pdf.return_value = OCRResult(
            text="PDF text",
            confidence=0.8,
            source_format="pdf",
            page_count=1,
            warnings=[],
        )

        with tempfile.NamedTemporaryFile(suffix=".pdf") as f:
            result = ocr_filing(Path(f.name))
            mock_ocr_pdf.assert_called_once()

    @patch("cppi.ocr.ocr_image")
    @patch("cppi.ocr.preprocess_image_for_ocr")
    def test_gif_dispatch(self, mock_preprocess, mock_ocr_image):
        """Test GIF files are dispatched to ocr_image."""
        mock_preprocess.return_value = Path("/tmp/processed.png")
        mock_ocr_image.return_value = OCRResult(
            text="GIF text",
            confidence=0.7,
            source_format="gif",
            page_count=1,
            warnings=[],
        )

        with tempfile.NamedTemporaryFile(suffix=".gif") as f:
            result = ocr_filing(Path(f.name))
            mock_preprocess.assert_called_once()
            mock_ocr_image.assert_called_once()

    def test_unsupported_format(self):
        """Test unsupported file format."""
        with tempfile.NamedTemporaryFile(suffix=".xyz") as f:
            result = ocr_filing(Path(f.name))
            assert result.text == ""
            assert "Unsupported" in result.warnings[0]


class TestTesseractAvailability:
    """Test Tesseract availability checking."""

    @patch("cppi.ocr.HAS_TESSERACT", True)
    def test_available(self):
        """Test when Tesseract is available."""
        # Note: This patches the module-level variable, not the actual check
        # The function returns the module variable
        from cppi import ocr
        with patch.object(ocr, "HAS_TESSERACT", True):
            assert is_tesseract_available() is True

    @patch("cppi.ocr.HAS_TESSERACT", False)
    def test_not_available(self):
        """Test when Tesseract is not available."""
        from cppi import ocr
        with patch.object(ocr, "HAS_TESSERACT", False):
            assert is_tesseract_available() is False
