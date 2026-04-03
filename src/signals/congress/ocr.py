from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path


@dataclass
class OCRResult:
    text: str
    confidence: float
    source_format: str
    page_count: int
    warnings: list[str]

    @property
    def is_successful(self) -> bool:
        return len(self.text.strip()) > 50 and self.confidence > 0.3


def _has_binary(name: str) -> bool:
    return shutil.which(name) is not None


HAS_TESSERACT = _has_binary("tesseract")
HAS_PDFTOPPM = _has_binary("pdftoppm")


def is_tesseract_available() -> bool:
    return HAS_TESSERACT


def estimate_ocr_confidence(text: str) -> float:
    if not text or len(text.strip()) < 50:
        return 0.0

    score = 0.5
    if len(text) > 500:
        score += 0.1
    if len(text) > 2000:
        score += 0.1

    garbage_chars = sum(1 for c in text if c in "[]{}|\\<>~`")
    if len(text) > 0:
        garbage_ratio = garbage_chars / len(text)
        if garbage_ratio < 0.01:
            score += 0.1
        elif garbage_ratio > 0.05:
            score -= 0.2

    expected_patterns = ["purchase", "sale", "$", "transaction", "stock", "spouse", "joint"]
    pattern_count = sum(1 for p in expected_patterns if p.lower() in text.lower())
    score += min(0.2, pattern_count * 0.03)

    return max(0.0, min(1.0, score))


def ocr_image(image_path: Path, language: str = "eng", config: str = "") -> OCRResult:
    if not HAS_TESSERACT:
        return OCRResult(
            text="",
            confidence=0.0,
            source_format=image_path.suffix.lstrip("."),
            page_count=1,
            warnings=["Tesseract OCR not installed. Run: brew install tesseract"],
        )

    try:
        cmd = ["tesseract", str(image_path), "stdout", "-l", language]
        if config:
            cmd.extend(config.split())
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            return OCRResult(
                text="",
                confidence=0.0,
                source_format=image_path.suffix.lstrip("."),
                page_count=1,
                warnings=[f"Tesseract error: {result.stderr}"],
            )
        text = result.stdout
        return OCRResult(
            text=text,
            confidence=estimate_ocr_confidence(text),
            source_format=image_path.suffix.lstrip("."),
            page_count=1,
            warnings=[],
        )
    except subprocess.TimeoutExpired:
        return OCRResult(
            text="",
            confidence=0.0,
            source_format=image_path.suffix.lstrip("."),
            page_count=1,
            warnings=["OCR timeout exceeded (120s)"],
        )


def _default_ocr_cache_dir(pdf_path: Path) -> Path:
    if pdf_path.parent.name == "house" and pdf_path.parent.parent.name == "pdfs":
        return pdf_path.parent.parent.parent / "ocr_cache" / "house"
    return pdf_path.parent / ".ocr_cache"


def _pdf_content_hash(pdf_path: Path) -> str:
    digest = hashlib.sha256()
    with pdf_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _ocr_cache_path(pdf_path: Path, cache_dir: Path | None = None) -> Path:
    target_dir = cache_dir or _default_ocr_cache_dir(pdf_path)
    return target_dir / f"{_pdf_content_hash(pdf_path)}.json"


def load_cached_ocr_result(pdf_path: Path, cache_dir: Path | None = None) -> OCRResult | None:
    cache_path = _ocr_cache_path(pdf_path, cache_dir)
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        return OCRResult(
            text=payload.get("text", ""),
            confidence=float(payload.get("confidence", 0.0)),
            source_format=payload.get("source_format", "pdf"),
            page_count=int(payload.get("page_count", 0)),
            warnings=list(payload.get("warnings", [])),
        )
    except Exception:
        return None


def save_cached_ocr_result(pdf_path: Path, result: OCRResult, cache_dir: Path | None = None) -> Path:
    cache_path = _ocr_cache_path(pdf_path, cache_dir)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(asdict(result), sort_keys=True), encoding="utf-8")
    return cache_path


def _ocr_pdf_uncached(pdf_path: Path, language: str = "eng", dpi: int = 300) -> OCRResult:
    if not HAS_TESSERACT:
        return OCRResult(text="", confidence=0.0, source_format="pdf", page_count=0, warnings=["Tesseract OCR not installed"])
    if not HAS_PDFTOPPM:
        return OCRResult(text="", confidence=0.0, source_format="pdf", page_count=0, warnings=["pdftoppm not installed. Install poppler."])

    warnings: list[str] = []
    all_text: list[str] = []
    page_count = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            cmd = ["pdftoppm", "-png", "-r", str(dpi), str(pdf_path), os.path.join(tmpdir, "page")]
            result = subprocess.run(cmd, capture_output=True, timeout=120)
            if result.returncode != 0:
                return OCRResult(
                    text="",
                    confidence=0.0,
                    source_format="pdf",
                    page_count=0,
                    warnings=[f"pdftoppm error: {result.stderr.decode()}"],
                )
        except subprocess.TimeoutExpired:
            return OCRResult(text="", confidence=0.0, source_format="pdf", page_count=0, warnings=["PDF conversion timeout"])

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


def ocr_pdf(pdf_path: Path, language: str = "eng", dpi: int = 300, cache_dir: Path | None = None) -> OCRResult:
    cached = load_cached_ocr_result(pdf_path, cache_dir)
    if cached is not None:
        return cached
    result = _ocr_pdf_uncached(pdf_path, language=language, dpi=dpi)
    save_cached_ocr_result(pdf_path, result, cache_dir)
    return result
