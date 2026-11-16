"""File Comparison routes and interactive web UI for semantic RCA analysis."""

from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, File, Form, UploadFile, HTTPException
from fastapi.responses import HTMLResponse
from services.code_comparator import CodeComparator

router = APIRouter(tags=["File Comparison"])
comparator = CodeComparator()


@router.post("/api/v1/compare/files")
async def compare_files_endpoint(
    source_file: UploadFile = File(..., description="Source code file (e.g. .sas, .sql, .cpp)"),
    target_file: UploadFile = File(..., description="Target code file (e.g. .py, .js)"),
    source_language: Optional[str] = Form(None),
    target_language: Optional[str] = Form(None),
):
    """Uploads two files (source and target) and performs automated semantic RCA analysis."""
    try:
        source_bytes = await source_file.read()
        target_bytes = await target_file.read()

        source_code = source_bytes.decode("utf-8", errors="replace")
        target_code = target_bytes.decode("utf-8", errors="replace")

        report = comparator.compare_files(
            source_code=source_code,
            target_code=target_code,
            source_filename=source_file.filename or "source",
            target_filename=target_file.filename or "target",
            source_language=source_language,
            target_language=target_language,
        )
        return report

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Comparison failed: {str(e)}")


@router.get("/compare", response_class=HTMLResponse)
def compare_ui_page() -> str:
    """Interactive browser UI for uploading and comparing 2 files."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Universal Data Knowledge - File Semantic Comparison & RCA</title>
  <style>
    :root {
      --bg: #0f172a;
      --card-bg: #1e293b;
      --border: #334155;
      --accent: #38bdf8;
      --accent-hover: #0284c7;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --success: #22c55e;
      --warning: #f59e0b;
      --danger: #ef4444;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
    body { background-color: var(--bg); color: var(--text); padding: 2rem 1rem; }
    .container { max-width: 1000px; margin: 0 auto; }
    header { text-align: center; margin-bottom: 2.5rem; }
    header h1 { font-size: 2rem; color: var(--accent); margin-bottom: 0.5rem; }
    header p { color: var(--text-muted); font-size: 1rem; }
    .card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; padding: 2rem; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.2); margin-bottom: 2rem; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; }
    @media (max-width: 640px) { .grid { grid-template-columns: 1fr; } }
    .upload-zone { border: 2px dashed var(--border); border-radius: 8px; padding: 1.5rem; text-align: center; cursor: pointer; transition: 0.2s; background: rgba(15,23,42,0.4); }
    .upload-zone:hover { border-color: var(--accent); background: rgba(56,189,248,0.05); }
    .upload-zone input { display: none; }
    .upload-zone label { display: block; font-weight: 600; margin-bottom: 0.5rem; cursor: pointer; color: var(--accent); }
    .filename { font-size: 0.85rem; color: var(--text-muted); margin-top: 0.5rem; word-break: break-all; }
    .btn { display: block; width: 100%; padding: 0.85rem; background: var(--accent); color: #000; border: none; border-radius: 8px; font-size: 1rem; font-weight: 700; cursor: pointer; margin-top: 1.5rem; transition: 0.2s; }
    .btn:hover { background: var(--accent-hover); color: #fff; }
    .badge { display: inline-block; padding: 0.25rem 0.6rem; border-radius: 6px; font-size: 0.75rem; font-weight: 700; text-transform: uppercase; }
    .badge-convertible { background: rgba(56,189,248,0.2); color: var(--accent); }
    .badge-cond { background: rgba(245,158,11,0.2); color: var(--warning); }
    .badge-incompat { background: rgba(239,68,68,0.2); color: var(--danger); }
    .badge-equiv { background: rgba(34,197,94,0.2); color: var(--success); }
    .finding-card { background: rgba(15,23,42,0.6); border: 1px solid var(--border); border-radius: 8px; padding: 1.25rem; margin-bottom: 1rem; }
    .finding-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem; }
    .code-block { background: #000; padding: 0.75rem; border-radius: 6px; font-family: monospace; font-size: 0.9rem; overflow-x: auto; margin: 0.5rem 0; border: 1px solid var(--border); }
    .correction-box { background: rgba(239,68,68,0.1); border-left: 4px solid var(--danger); padding: 0.75rem; margin-top: 0.75rem; border-radius: 0 6px 6px 0; }
    .recom-box { background: rgba(34,197,94,0.1); border-left: 4px solid var(--success); padding: 0.75rem; margin-top: 0.75rem; border-radius: 0 6px 6px 0; }
    #loading { display: none; text-align: center; color: var(--accent); margin: 1rem 0; font-weight: 600; }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <h1>Cross-Language Semantic Code Comparator</h1>
      <p>Upload source and target code files to identify semantic divergence, data type traps, and behavior-preserving corrections.</p>
    </header>

    <div class="card">
      <form id="compareForm">
        <div class="grid">
          <div class="upload-zone" onclick="document.getElementById('sourceFile').click()">
            <label>1. Source Code File</label>
            <p style="font-size:0.85rem; color:var(--text-muted);">e.g. .sas, .sql, .cpp</p>
            <input type="file" id="sourceFile" name="source_file" required onchange="showFilename('sourceFile', 'srcName')">
            <div id="srcName" class="filename">No file selected</div>
          </div>
          <div class="upload-zone" onclick="document.getElementById('targetFile').click()">
            <label>2. Target Code File</label>
            <p style="font-size:0.85rem; color:var(--text-muted);">e.g. .py, .js, .java</p>
            <input type="file" id="targetFile" name="target_file" required onchange="showFilename('targetFile', 'tgtName')">
            <div id="tgtName" class="filename">No file selected</div>
          </div>
        </div>
        <button type="submit" class="btn">Analyze & Compare Semantics</button>
      </form>
      <div id="loading">Analyzing code semantics against knowledge repository...</div>
    </div>

    <div id="resultsCard" class="card" style="display:none;">
      <h2 style="font-size:1.4rem; margin-bottom: 1rem; color: var(--text);">Semantic Difference & RCA Report</h2>
      <div id="summaryStats" style="margin-bottom: 1.5rem; color: var(--text-muted); font-size: 0.95rem;"></div>
      <div id="findingsList"></div>
    </div>
  </div>

  <script>
    function showFilename(inputId, displayId) {
      const input = document.getElementById(inputId);
      const display = document.getElementById(displayId);
      display.textContent = input.files[0] ? input.files[0].name : "No file selected";
    }

    document.getElementById('compareForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      const form = e.target;
      const formData = new FormData(form);
      const loading = document.getElementById('loading');
      const resultsCard = document.getElementById('resultsCard');
      const findingsList = document.getElementById('findingsList');
      const summaryStats = document.getElementById('summaryStats');

      loading.style.display = 'block';
      resultsCard.style.display = 'none';
      findingsList.innerHTML = '';

      try {
        const res = await fetch('/api/v1/compare/files', { method: 'POST', body: formData });
        if (!res.ok) throw new Error(await res.text());
        const data = await res.json();

        summaryStats.innerHTML = `
          <strong>Source:</strong> ${data.source_file} (${data.source_language}) &nbsp;|&nbsp;
          <strong>Target:</strong> ${data.target_file} (${data.target_language}) &nbsp;|&nbsp;
          <strong>Corrections Needed:</strong> <span style="color:${data.corrections_needed_count > 0 ? 'var(--danger)' : 'var(--success)'}; font-weight:bold;">${data.corrections_needed_count}</span>
        `;

        if (data.findings.length === 0) {
          findingsList.innerHTML = '<p style="color:var(--text-muted);">No specific semantic hazards detected in standard expressions.</p>';
        } else {
          data.findings.forEach(f => {
            const badgeClass = f.compatibility === 'CONVERTIBLE' ? 'badge-convertible' :
                               f.compatibility === 'CONDITIONALLY_EQUIVALENT' ? 'badge-cond' :
                               f.compatibility === 'INCOMPATIBLE' ? 'badge-incompat' : 'badge-equiv';

            let itemHtml = `
              <div class="finding-card">
                <div class="finding-header">
                  <strong>${f.operation} (${f.category})</strong>
                  <span class="badge ${badgeClass}">${f.compatibility}</span>
                </div>
                <div style="font-size:0.85rem; color:var(--text-muted);">
                  Source (Line ${f.source.line || '?'}): <div class="code-block">${f.source.expression}</div>
                  Target (Line ${f.target.line || '?'}): <div class="code-block">${f.target.expression || '(missing target match)'}</div>
                </div>
            `;

            if (f.needs_correction) {
              itemHtml += `
                <div class="correction-box">
                  <strong style="color:var(--danger);">Correction Required:</strong> ${f.correction_reason}
                </div>
              `;
            }

            if (f.recommended_code) {
              itemHtml += `
                <div class="recom-box">
                  <strong style="color:var(--success);">Recommended Target Code:</strong>
                  <div class="code-block">${f.recommended_code}</div>
                </div>
              `;
            }

            if (f.preservation_rules && f.preservation_rules.length > 0) {
              itemHtml += `
                <div style="margin-top:0.75rem; font-size:0.85rem;">
                  <strong>Preservation Rules:</strong>
                  <ul style="padding-left:1.25rem; margin-top:0.25rem; color:var(--text-muted);">
                    ${f.preservation_rules.map(r => `<li>${r}</li>`).join('')}
                  </ul>
                </div>
              `;
            }

            itemHtml += `</div>`;
            findingsList.innerHTML += itemHtml;
          });
        }
        resultsCard.style.display = 'block';
      } catch (err) {
        alert("Comparison error: " + err.message);
      } finally {
        loading.style.display = 'none';
      }
    });
  </script>
</body>
</html>
"""
