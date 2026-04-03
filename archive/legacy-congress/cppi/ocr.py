"""
OCR support for paper congressional filings.

Handles:
- House paper PDFs (822xxxx IDs)
- Senate GIF filings

Requires Tesseract OCR installed:
  macOS: brew install tesseract
  Ubuntu: sudo apt-get install tesseract-ocr
"""

import logging
import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Check for Tesseract availability
HAS_TESSERACT = False
TESSERACT_PATH = "tesseract"

try:
    result = subprocess.run(
        ["tesseract", "--version"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    if result.returncode == 0:
        HAS_TESSERACT = True
        version_line = result.stdout.split("\n")[0]
        logger.debug(f"Tesseract available: {version_line}")
except (subprocess.SubprocessError, FileNotFoundError):
    logger.debug("Tesseract not found in PATH")


@dataclass
class OCRResult:
    """Result of OCR processing."""

    text: str
    confidence: float  # 0-1, estimated confidence
    source_format: str  # 'pdf', 'gif', 'png', etc.
    page_count: int
    warnings: list[str]

    @property
    def is_successful(self) -> bool:
        """Check if OCR produced usable output."""
        return len(self.text.strip()) > 50 and self.confidence > 0.3


def is_tesseract_available() -> bool:
    """Check if Tesseract is available for OCR."""
    return HAS_TESSERACT


def get_tesseract_version() -> Optional[str]:
    """Get Tesseract version string."""
    if not HAS_TESSERACT:
        return None

    try:
        result = subprocess.run(
            ["tesseract", "--version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.split("\n")[0]
    except subprocess.SubprocessError:
        return None


def is_paper_filing(filing_id: str) -> bool:
    """
    Check if a filing ID indicates a paper filing.

    Paper filings typically have IDs starting with 822xxxxx.
    """
    if not filing_id:
        return False
    return filing_id.startswith("822")


def ocr_image(
    image_path: Path,
    language: str = "eng",
    config: str = "",
) -> OCRResult:
    """
    Run OCR on a single image file.

    Args:
        image_path: Path to image file (GIF, PNG, TIFF, etc.)
        language: Tesseract language code
        config: Additional Tesseract config

    Returns:
        OCRResult with extracted text
    """
    if not HAS_TESSERACT:
        return OCRResult(
            text="",
            confidence=0.0,
            source_format=image_path.suffix.lstrip("."),
            page_count=1,
            warnings=["Tesseract OCR not installed. Run: brew install tesseract"],
        )

    warnings = []

    try:
        # Run Tesseract
        cmd = [
            "tesseract",
            str(image_path),
            "stdout",
            "-l", language,
        ]
        if config:
            cmd.extend(config.split())

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )

        if result.returncode != 0:
            warnings.append(f"Tesseract error: {result.stderr}")
            return OCRResult(
                text="",
                confidence=0.0,
                source_format=image_path.suffix.lstrip("."),
                page_count=1,
                warnings=warnings,
            )

        text = result.stdout

        # Estimate confidence based on output quality
        confidence = estimate_ocr_confidence(text)

        return OCRResult(
            text=text,
            confidence=confidence,
            source_format=image_path.suffix.lstrip("."),
            page_count=1,
            warnings=warnings,
        )

    except subprocess.TimeoutExpired:
        return OCRResult(
            text="",
            confidence=0.0,
            source_format=image_path.suffix.lstrip("."),
            page_count=1,
            warnings=["OCR timeout exceeded (120s)"],
        )
    except Exception as e:
        return OCRResult(
            text="",
            confidence=0.0,
            source_format=image_path.suffix.lstrip("."),
            page_count=1,
            warnings=[f"OCR error: {str(e)}"],
        )


def ocr_pdf(
    pdf_path: Path,
    language: str = "eng",
    dpi: int = 300,
) -> OCRResult:
    """
    Run OCR on a PDF file by converting pages to images.

    Requires pdftoppm (from poppler-utils) for PDF to image conversion.

    Args:
        pdf_path: Path to PDF file
        language: Tesseract language code
        dpi: Resolution for rendering PDF pages

    Returns:
        OCRResult with extracted text from all pages
    """
    if not HAS_TESSERACT:
        return OCRResult(
            text="",
            confidence=0.0,
            source_format="pdf",
            page_count=0,
            warnings=["Tesseract OCR not installed"],
        )

    warnings = []
    all_text = []
    page_count = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        # Convert PDF to images using pdftoppm
        try:
            cmd = [
                "pdftoppm",
                "-png",
                "-r", str(dpi),
                str(pdf_path),
                os.path.join(tmpdir, "page"),
            ]
            result = subprocess.run(cmd, capture_output=True, timeout=120)

            if result.returncode != 0:
                warnings.append(f"pdftoppm error: {result.stderr.decode()}")
                return OCRResult(
                    text="",
                    confidence=0.0,
                    source_format="pdf",
                    page_count=0,
                    warnings=warnings,
                )

        except FileNotFoundError:
            warnings.append("pdftoppm not found. Install poppler-utils.")
            return OCRResult(
                text="",
                confidence=0.0,
                source_format="pdf",
                page_count=0,
                warnings=warnings,
            )
        except subprocess.TimeoutExpired:
            warnings.append("PDF conversion timeout")
            return OCRResult(
                text="",
                confidence=0.0,
                source_format="pdf",
                page_count=0,
                warnings=warnings,
            )

        # OCR each page image
        page_files = sorted(Path(tmpdir).glob("page-*.png"))
        page_count = len(page_files)

        for page_file in page_files:
            page_result = ocr_image(page_file, language)
            if page_result.text:
                all_text.append(page_result.text)
            warnings.extend(page_result.warnings)

    combined_text = "\n\n--- PAGE BREAK ---\n\n".join(all_text)
    confidence = estimate_ocr_confidence(combined_text) if all_text else 0.0

    return OCRResult(
        text=combined_text,
        confidence=confidence,
        source_format="pdf",
        page_count=page_count,
        warnings=warnings,
    )


def estimate_ocr_confidence(text: str) -> float:
    """
    Estimate OCR confidence based on text quality heuristics.

    Factors:
    - Reasonable text length
    - Low ratio of garbage characters
    - Presence of expected patterns (dates, amounts)

    Args:
        text: OCR'd text

    Returns:
        Confidence score from 0 to 1
    """
    if not text or len(text.strip()) < 50:
        return 0.0

    score = 0.5  # Start at moderate confidence

    # Check text length (more text = more reliable)
    if len(text) > 500:
        score += 0.1
    if len(text) > 2000:
        score += 0.1

    # Check for garbage character ratio
    garbage_chars = sum(1 for c in text if c in "[]{}|\\<>~`")
    if len(text) > 0:
        garbage_ratio = garbage_chars / len(text)
        if garbage_ratio < 0.01:
            score += 0.1
        elif garbage_ratio > 0.05:
            score -= 0.2

    # Check for expected patterns in congressional filings
    expected_patterns = [
        "purchase",
        "sale",
        "$",
        "transaction",
        "stock",
        "spouse",
        "joint",
    ]
    pattern_count = sum(1 for p in expected_patterns if p.lower() in text.lower())
    score += min(0.2, pattern_count * 0.03)

    return max(0.0, min(1.0, score))


def preprocess_image_for_ocr(
    image_path: Path,
    output_path: Optional[Path] = None,
) -> Path:
    """
    Preprocess image to improve OCR quality.

    Applies:
    - Grayscale conversion
    - Contrast enhancement
    - Deskewing

    Requires ImageMagick.

    Args:
        image_path: Input image path
        output_path: Output path (default: temp file)

    Returns:
        Path to preprocessed image
    """
    if output_path is None:
        output_path = Path(tempfile.mktemp(suffix=".png"))

    try:
        cmd = [
            "convert",
            str(image_path),
            "-colorspace", "Gray",
            "-contrast",
            "-deskew", "40%",
            str(output_path),
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=60)

        if result.returncode == 0:
            return output_path
        else:
            logger.warning(f"Image preprocessing failed: {result.stderr.decode()}")
            return image_path

    except (FileNotFoundError, subprocess.SubprocessError) as e:
        logger.warning(f"ImageMagick not available for preprocessing: {e}")
        return image_path


def ocr_filing(
    file_path: Path,
    preprocess: bool = True,
) -> OCRResult:
    """
    OCR a congressional filing (PDF or image).

    Automatically detects file type and applies appropriate OCR method.

    Args:
        file_path: Path to filing file
        preprocess: Whether to preprocess images

    Returns:
        OCRResult with extracted text
    """
    suffix = file_path.suffix.lower()

    if suffix == ".pdf":
        return ocr_pdf(file_path)

    elif suffix in (".gif", ".png", ".jpg", ".jpeg", ".tiff", ".tif"):
        if preprocess:
            processed = preprocess_image_for_ocr(file_path)
            result = ocr_image(processed)
            # Clean up temp file if created
            if processed != file_path:
                try:
                    processed.unlink()
                except Exception:
                    pass
            return result
        else:
            return ocr_image(file_path)

    else:
        return OCRResult(
            text="",
            confidence=0.0,
            source_format=suffix.lstrip("."),
            page_count=0,
            warnings=[f"Unsupported file format: {suffix}"],
        )
