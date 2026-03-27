import os
import re
import uuid
import sqlite3
from datetime import datetime
from typing import List, Dict, Any, Optional

import streamlit as st
from pypdf import PdfReader
from docx import Document
from dotenv import load_dotenv
from PIL import Image
from google import genai

load_dotenv()

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="ABT DATA ANALYST REPORTS",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =========================================================
# CONSTANTS
# =========================================================
APP_NAME = "ABT DATA ANALYST REPORTS"
LOGO_PATH = "ABT LOGO.jpg"
DB_PATH = "abt_data_analyst_history.db"
DEFAULT_MODEL = "gemini-2.5-flash"

# =========================================================
# CSS
# =========================================================
st.markdown(
    """
<style>
:root {
    --bg: #06080d;
    --panel: #0d1118;
    --panel2: #121827;
    --line: rgba(255,255,255,0.08);
    --text: #f3f6ff;
    --muted: #99a4bf;
    --blue: #2563eb;
    --red: #ef4444;
    --cyan: #06b6d4;
    --purple: #7c3aed;
}

html, body, [class*="css"] {
    font-family: "Inter", sans-serif;
}

.stApp {
    background:
      radial-gradient(circle at top left, rgba(37,99,235,0.18), transparent 24%),
      radial-gradient(circle at top right, rgba(239,68,68,0.15), transparent 20%),
      radial-gradient(circle at bottom center, rgba(124,58,237,0.12), transparent 24%),
      linear-gradient(180deg, #05070b 0%, #090d14 100%);
    color: var(--text);
}

.block-container {
    max-width: 1400px;
    padding-top: 1rem;
    padding-bottom: 2rem;
}

.shell {
    background: linear-gradient(180deg, rgba(255,255,255,0.02), rgba(255,255,255,0.01));
    border: 1px solid var(--line);
    border-radius: 28px;
    padding: 22px;
    box-shadow: 0 24px 80px rgba(0,0,0,0.35);
}

.hero {
    border-radius: 26px;
    padding: 24px;
    background:
      linear-gradient(135deg, rgba(37,99,235,0.17), rgba(239,68,68,0.10), rgba(124,58,237,0.10)),
      linear-gradient(180deg, #0b1018 0%, #0a0f16 100%);
    border: 1px solid var(--line);
    margin-bottom: 18px;
}

.hero-title {
    font-size: 2.15rem;
    font-weight: 900;
    letter-spacing: -0.02em;
    color: white;
    margin-bottom: .2rem;
}

.hero-sub {
    color: #c4cee6;
    font-size: 1.02rem;
}

.badge-row {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    margin-top: 14px;
}

.badge-pill {
    background: rgba(255,255,255,0.05);
    border: 1px solid var(--line);
    color: #dbe3f5;
    padding: 8px 12px;
    border-radius: 999px;
    font-size: .86rem;
}

.section-card {
    background: linear-gradient(180deg, rgba(17,22,33,0.97), rgba(10,14,21,0.98));
    border: 1px solid var(--line);
    border-radius: 22px;
    padding: 18px;
    margin-bottom: 16px;
}

.info-box {
    background: linear-gradient(135deg, rgba(37,99,235,0.14), rgba(6,182,212,0.10));
    border-left: 4px solid #38bdf8;
    border: 1px solid var(--line);
    border-radius: 16px;
    padding: 14px 16px;
    color: #ebf2ff;
    margin: 10px 0;
}

.stat-card {
    background: linear-gradient(180deg, #111723, #0d1320);
    border: 1px solid var(--line);
    border-radius: 18px;
    padding: 14px 16px;
}

.stat-label {
    color: var(--muted);
    font-size: .84rem;
}

.stat-value {
    color: #ffffff;
    font-size: 1.35rem;
    font-weight: 800;
}

.history-card {
    background: linear-gradient(180deg, #101521, #0c1018);
    border: 1px solid var(--line);
    border-radius: 16px;
    padding: 12px 14px;
    margin-bottom: 10px;
}

.stTextArea textarea, .stTextInput input {
    background: #0b1018 !important;
    color: #f8fafc !important;
    border-radius: 14px !important;
}

.stButton > button {
    border-radius: 14px !important;
    border: 1px solid rgba(255,255,255,0.08) !important;
    background: linear-gradient(135deg, #2563eb, #ef4444) !important;
    color: white !important;
    font-weight: 800 !important;
    padding: 0.6rem 1rem !important;
}

.stDownloadButton > button {
    border-radius: 14px !important;
    font-weight: 800 !important;
}

[data-testid="stSidebar"] {
    background: linear-gradient(180deg, rgba(11,14,20,0.98), rgba(8,10,15,0.98));
    border-right: 1px solid var(--line);
}
</style>
""",
    unsafe_allow_html=True,
)

# =========================================================
# DATABASE
# =========================================================
def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS report_history (
            id TEXT PRIMARY KEY,
            created_at TEXT,
            title TEXT,
            content_type TEXT,
            content TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def save_history(title: str, content_type: str, content: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO report_history (id, created_at, title, content_type, content) VALUES (?, ?, ?, ?, ?)",
        (
            str(uuid.uuid4()),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            title,
            content_type,
            content,
        ),
    )
    conn.commit()
    conn.close()


def load_history(limit: int = 100) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, created_at, title, content_type, content
        FROM report_history
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "id": row[0],
            "created_at": row[1],
            "title": row[2],
            "content_type": row[3],
            "content": row[4],
        }
        for row in rows
    ]


def delete_history(item_id: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM report_history WHERE id = ?", (item_id,))
    conn.commit()
    conn.close()


# =========================================================
# SESSION STATE
# =========================================================
defaults = {
    "raw_analysis_text": "",
    "empirical_review_text": "",
    "study_context_text": "",
    "parsed_tables": [],
    "interpreted_tables": [],
    "summary_of_findings": "",
    "discussion_of_findings": "",
    "final_outputs": "",
    "full_report": "",
}
for key, val in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = val


# =========================================================
# API
# =========================================================
def get_api_key() -> str:
    key = os.getenv("GEMINI_API_KEY", "")
    if key:
        return key.strip()

    try:
        key = st.secrets.get("GEMINI_API_KEY", "")
        if key:
            return key.strip()
    except Exception:
        pass

    return ""


def get_client(api_key: str):
    return genai.Client(api_key=api_key)


def gemini_text(api_key: str, prompt: str, model_name: str = DEFAULT_MODEL) -> str:
    try:
        client = get_client(api_key)
        response = client.models.generate_content(model=model_name, contents=prompt)
        return (response.text or "").strip()
    except Exception as e:
        return f"ERROR: {e}"


# =========================================================
# TEXT HELPERS
# =========================================================
def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\x00", " ")
    text = text.replace("\uf0b7", "•")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_pdf_text(uploaded_file) -> str:
    try:
        uploaded_file.seek(0)
        reader = PdfReader(uploaded_file)
        pages = [page.extract_text() or "" for page in reader.pages]
        return clean_text("\n".join(pages))
    except Exception as e:
        return f"ERROR: Could not read PDF. {e}"


def extract_docx_text(uploaded_file) -> str:
    try:
        uploaded_file.seek(0)
        doc = Document(uploaded_file)
        paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
        table_texts = []

        for table in doc.tables:
            rows = []
            for row in table.rows:
                cells = [clean_text(cell.text) for cell in row.cells]
                rows.append("\t".join(cells))
            table_texts.append("\n".join(rows))

        return clean_text("\n\n".join(paragraphs + table_texts))
    except Exception as e:
        return f"ERROR: Could not read DOCX. {e}"


def extract_text_from_upload(uploaded_file) -> str:
    name = uploaded_file.name.lower()
    if name.endswith(".pdf"):
        return extract_pdf_text(uploaded_file)
    if name.endswith(".docx"):
        return extract_docx_text(uploaded_file)
    return "ERROR: Unsupported file type."


# =========================================================
# PARSING HELPERS
# =========================================================
WORD_TO_NUM = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
}


def to_number(token: str) -> Optional[int]:
    token = str(token).strip().lower()
    if token.isdigit():
        return int(token)
    return WORD_TO_NUM.get(token)


def table_number_from_text(text: str) -> int:
    match = re.search(r"table\s+(\d+)", str(text).lower())
    if match:
        return int(match.group(1))
    return 999


def section_sort_number(tag: str) -> int:
    tag = str(tag)
    match = re.search(r"\b(\d+)\b", tag)
    if match:
        return int(match.group(1))

    word_match = re.search(
        r"\b(one|two|three|four|five|six|seven|eight|nine|ten)\b",
        tag.lower(),
    )
    if word_match:
        return WORD_TO_NUM[word_match.group(1)]
    return 999


def split_tables_from_text(raw_text: str) -> List[str]:
    raw_text = clean_text(raw_text)
    if not raw_text:
        return []

    lines = [ln.rstrip() for ln in raw_text.split("\n") if ln.strip()]
    tables: List[str] = []
    pending_headings: List[str] = []
    current: List[str] = []

    def is_table_start(line: str) -> bool:
        return bool(re.search(r"^Table\s+\d+[.:]?", line, flags=re.I))

    def is_section_heading(line: str) -> bool:
        lowered = line.strip().lower()
        heading_patterns = [
            r"^research question\s+(one|two|three|four|five|six|\d+)",
            r"^question\s+(one|two|three|four|five|six|\d+)",
            r"^rq\s+(one|two|three|four|five|six|\d+)",
            r"^hypothesis\s+(one|two|three|four|five|six|\d+)",
            r"^research hypothesis\s+(one|two|three|four|five|six|\d+)",
            r"^h0?\d+",
            r"^demographic",
        ]
        return any(re.search(p, lowered) for p in heading_patterns)

    def looks_like_table_line(line: str) -> bool:
        if re.search(r'\d+.*\d+.*\d+', line) and len(line.split()) >= 3:
            return True
        if '|' in line and line.count('|') >= 2:
            return True
        if '\t' in line and line.count('\t') >= 2:
            return True
        if re.search(r'(\d+\.\d+|\d+%|Mean|SD|Std|F\(|t\(|p\(|r\(|R\s*=|B\s*=|β\s*=)', line):
            return True
        return False

    for line in lines:
        stripped = line.strip()
        if is_table_start(stripped):
            if current:
                tables.append("\n".join(current).strip())
            current = pending_headings + [stripped]
            pending_headings = []
        elif current:
            if is_section_heading(stripped) and len(current) > 1:
                tables.append("\n".join(current).strip())
                current = [stripped]
            elif looks_like_table_line(stripped) or stripped.startswith("Source:"):
                current.append(stripped)
            elif len(stripped) > 10:
                current.append(stripped)
        else:
            if is_section_heading(stripped) or looks_like_table_line(stripped):
                pending_headings.append(stripped)
            elif len(stripped) > 20:
                pending_headings.append(stripped)
            
            if len(pending_headings) > 8:
                pending_headings = pending_headings[-8:]

    if current:
        tables.append("\n".join(current).strip())

    return [tbl for tbl in tables if len(tbl) > 50 and any(keyword in tbl.lower() for keyword in ['table', 'mean', 'sd', 'std', 'f(', 't(', 'p(', 'gender', 'age', 'male', 'female', 'respondent', 'n=', '%', 'frequency'])]


def detect_table_type(title: str, body: str) -> str:
    text = f"{title}\n{body}".lower()

    if any(k in text for k in ["gender", "age range", "qualification", "demographic", "respondent", "stakeholder category"]):
        return "Demographic"
    if any(k in text for k in ["pearson correlation", "ppmc", "relationship between", "correlation"]):
        return "PPMC"
    if any(k in text for k in ["t-test", "t value", "mean ratings of male and female"]):
        return "Independent t-test"
    if any(k in text for k in ["anova", "between groups", "within groups", "f-value", "analysis of variance"]):
        return "ANOVA"
    if any(k in text for k in ["model summary", "coefficient", "regression", "r square", "adjusted r square"]):
        return "Linear Regression"
    if any(k in text for k in ["mediation", "indirect effect", "sobel", "direct effect", "a path", "b path"]):
        return "Mediation"
    if any(k in text for k in ["mean", "std. deviation", "ranking", "decision", "weighted mean", "remarks"]):
        return "Descriptive"
    return "Other"


def infer_section_tag_from_context(text_block: str) -> str:
    lower = text_block.lower()

    rq_patterns = [
        r"research question\s+(one|two|three|four|five|six|\d+)\s*[:\.]?\s*([^\n]+)",
        r"rq\s+(one|two|three|four|five|six|\d+)\s*[:\.]?\s*([^\n]+)",
        r"question\s+(one|two|three|four|five|six|\d+)\s*[:\.]?\s*([^\n]+)",
    ]
    for pattern in rq_patterns:
        match = re.search(pattern, lower)
        if match:
            num = to_number(match.group(1))
            text = match.group(2).replace("?", "").strip()
            if num is not None and len(text) > 5:
                return f"RQ {num}: {text.title()}"

    h_patterns = [
        r"h0?(\d+)\s*[:\.]?\s*([^\n]+)",
        r"hypothesis\s+(one|two|three|four|five|six|\d+)\s*[:\.]?\s*([^\n]+)",
        r"research hypothesis\s+(one|two|three|four|five|six|\d+)\s*[:\.]?\s*([^\n]+)",
    ]
    for pattern in h_patterns:
        match = re.search(pattern, lower)
        if match:
            num = to_number(match.group(1))
            text = match.group(2).replace("?", "").strip()
            if num is not None and len(text) > 5:
                return f"H{str(num).zfill(2)}: {text.title()}"

    rq_match = re.search(r"research question\s+(one|two|three|four|five|six|\d+)", lower)
    if rq_match:
        num = to_number(rq_match.group(1))
        if num is not None:
            return f"RQ {num}"

    h_match = re.search(r"(?:h0?(\d+)|hypothesis\s+(one|two|three|four|five|six|\d+))", lower)
    if h_match:
        raw = h_match.group(1) or h_match.group(2)
        num = to_number(raw)
        if num is not None:
            return f"H{str(num).zfill(2)}"

    if "demographic" in lower or "gender" in lower or "age range" in lower:
        return "DEMOGRAPHIC"

    return "OTHER"


def parse_table_chunk(chunk: str) -> Dict[str, Any]:
    lines = [ln.strip() for ln in chunk.split("\n") if ln.strip()]
    table_line_index = 0
    for idx, line in enumerate(lines):
        if re.search(r"^Table\s+\d+", line, flags=re.I):
            table_line_index = idx
            break

    context_lines = lines[:table_line_index]
    title = lines[table_line_index] if lines else "Untitled Table"
    body = "\n".join(lines[table_line_index + 1:]) if len(lines) > table_line_index + 1 else ""
    context_text = "\n".join(context_lines)

    return {
        "original_title": title,
        "body": body,
        "context_text": context_text,
        "table_type": detect_table_type(title, body),
        "section_tag": infer_section_tag_from_context(chunk),
        "chunk": chunk,
    }


def extract_source_line(chunk: str) -> str:
    match = re.search(r"(Source:\s*.*)", chunk, flags=re.I)
    return match.group(1).strip() if match else "Source: Researcher’s Output (Year) Using SPSS"


def build_apa_title(table: Dict[str, Any], index: int) -> str:
    return f"Table {index}: {table['original_title'].strip()}"


def normalize_rows(rows: List[List[str]]) -> List[List[str]]:
    if not rows:
        return []
    max_len = max(len(r) for r in rows)
    return [r + [""] * (max_len - len(r)) for r in rows]


def split_columns_from_line(line: str) -> List[str]:
    if "\t" in line:
        parts = [p.strip() for p in line.split("\t")]
        return [p for p in parts if p != ""]

    if "|" in line:
        parts = [p.strip() for p in line.split("|")]
        return [p for p in parts if p != ""]

    parts = [p.strip() for p in re.split(r"\s{2,}", line) if p.strip()]
    if len(parts) >= 2:
        return parts

    return [line.strip()] if line.strip() else []


def chunk_to_rows(chunk: str) -> List[List[str]]:
    lines = [ln.rstrip() for ln in chunk.split("\n") if ln.strip()]
    rows: List[List[str]] = []
    for line in lines:
        if re.match(
            r"^(Research Question|Question|RQ\s+\d+|Hypothesis|Research Hypothesis|H0?\d+|Source:)",
            line,
            flags=re.I,
        ):
            continue
        cols = split_columns_from_line(line)
        if cols:
            rows.append(cols)
    return normalize_rows(rows)


def detect_apa_note(table: Dict[str, Any]) -> str:
    chunk = table.get("chunk", "")
    lower = chunk.lower()

    if "correlation is significant" in lower:
        match = re.search(r"significant at the\s+([0-9.]+)\s+level", chunk, flags=re.I)
        if match:
            return f"Note. Correlation was significant at the {match.group(1)} level (2-tailed)."
        return "Note. Correlation was significant at the stated level (2-tailed)."

    if "p<0.05" in lower or "@p<0.05" in lower:
        return "Note. Decision was based on p < .05."

    if "p>0.05" in lower:
        return "Note. Decision was based on p > .05."

    return ""


def rows_to_markdown_table(rows: List[List[str]]) -> str:
    if not rows:
        return ""

    rows = normalize_rows(rows)
    if len(rows) == 1:
        header = [f"Column {i + 1}" for i in range(len(rows[0]))]
        body = rows
    else:
        header = rows[0]
        body = rows[1:]

    header_line = "| " + " | ".join(header) + " |"
    sep_line = "| " + " | ".join(["---"] * len(header)) + " |"
    body_lines = ["| " + " | ".join(r) + " |" for r in body]
    return "\n".join([header_line, sep_line] + body_lines)


def build_apa_table_block(table: Dict[str, Any], index: int) -> str:
    rows = chunk_to_rows(table.get("chunk", ""))
    md_table = rows_to_markdown_table(rows)
    note = detect_apa_note(table)
    title = table.get("apa_title", f"Table {index}")
    source = table.get("source_line", "")

    block = [f"**{title}**", "", md_table or "Table data could not be formatted."]
    if note:
        block.extend(["", f"*{note}*"])
    if source:
        block.extend(["", f"*{source}*"])
    return "\n".join(block)


def sort_for_final_output(tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def key_func(item: Dict[str, Any]):
        tag = str(item.get("section_tag", "OTHER"))
        if tag.startswith("RQ"):
            return (0, section_sort_number(tag), table_number_from_text(item.get("apa_title", "")))
        if tag.startswith("H"):
            return (1, section_sort_number(tag), table_number_from_text(item.get("apa_title", "")))
        if tag == "DEMOGRAPHIC":
            return (2, 0, table_number_from_text(item.get("apa_title", "")))
        return (3, 999, table_number_from_text(item.get("apa_title", "")))

    return sorted(tables, key=key_func)
# PROMPTS
# =========================================================
def build_all_interpretations_prompt(tables: List[Dict[str, Any]]) -> str:
    blocks = []
    for i, table in enumerate(tables, start=1):
        apa_table = build_apa_table_block(table, i)
        table_type = table['table_type'].upper()
        
        # Add specific format examples based on table type
        format_examples = ""
        if table_type == "DEMOGRAPHIC":
            format_examples = """
EXAMPLE FORMAT FOR DEMOGRAPHIC:
Table 1 above described the categories of the respondents based on gender. It revealed that male respondents are 97 (48.85%) and female respondents are 103 (51.5%). This implies that there are more females than male in the selected schools.
"""
        elif table_type == "DESCRIPTIVE":
            format_examples = """
EXAMPLE FORMAT FOR DESCRIPTIVE:
Table 2 disclosed how different stakeholders contributed to school development based on students' opinions. The highest-rated contribution was parents supporting the school financially, with a mean score of 4.78, ranked 1st, suggesting it was perceived as the most significant input.
"""
        elif table_type == "PPMC":
            format_examples = """
EXAMPLE FORMAT FOR PPMC:
Table 7 presented the result of the analysis examining the relationship between principals' management practices and teacher retention rate in secondary schools in Ilorin South. The result showed a moderate positive correlation (r = 0.38) between management practices and teacher retention, with a p-value of 0.01, which is less than 0.05.
"""
        elif table_type == "INDEPENDENT T-TEST":
            format_examples = """
EXAMPLE FORMAT FOR INDEPENDENT T-TEST:
Table 5 showed that a t-value of 1.51 was obtained with a p-value of 0.13, which was greater than the 0.05 level of significance. Consequently, the null hypothesis was retained. This indicated that there was no statistically significant difference in the mean ratings of male and female respondents.
"""
        elif table_type == "ANOVA":
            format_examples = """
EXAMPLE FORMAT FOR ANOVA:
As shown in Table 4, the F-value of 1.283 with a p-value of 0.23 computed at 0.05 alpha level. Since the p-value of 0.23 obtained is greater than 0.05 level of significance, the null hypothesis two is retained.
"""
        elif table_type == "LINEAR REGRESSION":
            format_examples = """
EXAMPLE FORMAT FOR LINEAR REGRESSION:
The model summary tests the null hypothesis that there is no significant impact of age on teacher attrition rates in private secondary schools. The results indicate that the correlation coefficient is nearly zero, signifying a negligible linear relationship between age and teacher attrition rates.
"""
        elif table_type == "MEDIATION":
            format_examples = """
EXAMPLE FORMAT FOR MEDIATION:
The mediation analysis revealed that Health Impact significantly predicted Social Support (R² = .085, F (1, 148) = 13.68, p < .001), indicating that 8.5% of the variance in Social Support was explained by Health Impact.
"""
        
        blocks.append(
            f"""
###TABLE_{i}_START###
TABLE TYPE: {table_type}
APA TITLE: {table['apa_title']}
SECTION TAG: {table['section_tag']}
SOURCE LINE: {table['source_line']}
CONTEXT TEXT:
{table.get('context_text', '')}
RAW TABLE BODY:
{table['body']}
{format_examples}
###TABLE_{i}_END###
"""
        )

    return f"""
STRICT EXECUTION MODE – FOLLOW MY INTERPRETATION GUIDE EXACTLY.

For EACH table, provide interpretation using the specified format for its table type.

CRITICAL RULES:
- Use the exact format examples provided for each table type
- Write in past tense throughout
- Include statistical values (means, SD, p-values, correlations, etc.)
- Explain what the findings mean in practical terms
- Use phrases like "revealed that", "showed that", "indicated that", "implied that"
- For demographic tables, describe respondent characteristics and distributions
- For descriptive tables, explain mean scores, rankings, and interpretations
- For inferential tables, state hypotheses, statistical results, and conclusions
- Keep each interpretation to one comprehensive paragraph
- Do NOT generate APA tables - only provide interpretations
- Wrap each interpretation in these markers:
  ###ANALYSIS_1### ... ###END_ANALYSIS_1###
  ###ANALYSIS_2### ... ###END_ANALYSIS_2###

TABLES TO INTERPRET:
{''.join(blocks)}
"""


def parse_bulk_interpretations(result: str, tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output = []
    for i, table in enumerate(tables, start=1):
        pattern = rf"###ANALYSIS_{i}###(.*?)###END_ANALYSIS_{i}###"
        match = re.search(pattern, result, flags=re.S)
        interpretation = match.group(1).strip() if match else "Analysis could not be parsed."
        item = dict(table)
        item["interpretation"] = interpretation
        output.append(item)
    return output


def build_summary_prompt(all_tables: List[Dict[str, Any]]) -> str:
    joined = "\n\n".join(
        [
            f"SECTION TAG: {t['section_tag']}\nAPA TITLE: {t['apa_title']}\nINTERPRETATION: {t['interpretation']}"
            for t in all_tables
        ]
    )

    return f"""
Generate a summary of findings based only on the interpreted tables below.

Rules:
- Start with exactly: The summary of answers to research questions is presented below:
- Use numbered bullet points (1., 2., 3., etc.)
- Keep the language simple and direct
- Arrange findings by research questions first, then hypotheses
- Include demographic findings separately if present
- Each point should be a complete sentence that captures a key finding
- Do not add statistical details in the summary - focus on the main conclusions

INTERPRETED FINDINGS:
{joined}
"""


def build_discussion_prompt(non_demo_tables: List[Dict[str, Any]], empirical_review: str) -> str:
    sorted_tables = sorted(non_demo_tables, key=lambda x: table_number_from_text(x.get("apa_title", "")))
    findings_text = ""
    for i, table in enumerate(sorted_tables, start=1):
        findings_text += f"Table {i} ({table['apa_title']}): {table['interpretation']}\n\n"

    return f"""
You are an expert academic writing assistant.
Generate DISCUSSION OF FINDINGS.

Rules:
1. Use “Findings from Table One”, “Findings from Table Two”, etc.
2. For each table, write exactly one paragraph.
3. Compare findings with previous studies from the empirical review only.
4. Use exactly one citation per paragraph from the empirical review only.
5. Add a REFERENCES section at the end.
6. Do not use chapter one, chapter three, methodology, or other non-empirical sections.

FINDINGS TO DISCUSS:
{findings_text}

EMPIRICAL REVIEW ONLY:
{empirical_review[:15000]}
"""


def build_final_outputs_prompt(non_demo_tables: List[Dict[str, Any]], study_context_text: str = "") -> str:
    sorted_tables = sort_for_final_output(non_demo_tables)
    findings_text = ""
    for i, table in enumerate(sorted_tables, start=1):
        findings_text += f"Table {i} ({table['apa_title']}): {table['interpretation']}\n\n"

    return f"""
Generate the following sections in this exact order:
1. CONCLUSION (exactly 200 words)
2. IMPLICATION OF THE STUDY
3. RECOMMENDATIONS
4. LIMITATION OF THE STUDY
5. SUGGESTIONS FOR FUTURE RESEARCH

Rules:
- Base everything primarily on the findings below.
- Use the study context only to infer the setting, variables, likely respondents, scope, and realistic study limitations.
- Do not invent data values.
- Give at least 5 recommendations.
- Give at least 5 future research suggestions.
- Make the implication of the study practical and specific to the study findings.
- Make the limitation of the study realistic and aligned with the study context.
- Use clear headings.

FINDINGS TO USE:
{findings_text}

STUDY CONTEXT:
{study_context_text[:12000]}
"""


# =========================================================
# EMPIRICAL REVIEW EXTRACTION
# =========================================================
def extract_empirical_review_section(full_text: str) -> str:
    text = clean_text(full_text)
    if not text:
        return ""

    start_patterns = [
        r"\bempirical review\b",
        r"\breview of empirical studies\b",
        r"\bempirical studies\b",
    ]
    end_patterns = [
        r"\bsummary of reviewed literature\b",
        r"\bappraisal of reviewed literature\b",
        r"\bchapter three\b",
        r"\bresearch methodology\b",
        r"\bmethodology\b",
    ]

    start_idx = -1
    for pattern in start_patterns:
        match = re.search(pattern, text, flags=re.I)
        if match:
            start_idx = match.start()
            break

    if start_idx == -1:
        return text

    sliced = text[start_idx:]
    end_idx = len(sliced)
    for pattern in end_patterns:
        match = re.search(pattern, sliced[1:], flags=re.I)
        if match:
            end_idx = min(end_idx, match.start() + 1)
    return sliced[:end_idx].strip()


# =========================================================
# REPORT EXPORT
# =========================================================
def export_full_report(
    all_tables: List[Dict[str, Any]],
    summary_text: str,
    discussion_text: str,
    final_output_text: str,
) -> str:
    lines = [f"# {APP_NAME}\n"]

    lines.append("## TABLES AND INTERPRETATIONS\n")
    for i, table in enumerate(all_tables, start=1):
        lines.append(f"Table {i}: {table['original_title']}")
        lines.append("")
        lines.append(table["chunk"])
        lines.append("")
        lines.append("**Interpretation:**")
        lines.append(table.get("interpretation", ""))
        lines.append("")

    if summary_text.strip():
        lines.append("## SUMMARY OF FINDINGS\n")
        lines.append(summary_text)
        lines.append("")

    if discussion_text.strip():
        lines.append("## DISCUSSION OF FINDINGS\n")
        lines.append(discussion_text)
        lines.append("")

    if final_output_text.strip():
        lines.append("## CONCLUSION, IMPLICATION, RECOMMENDATIONS, LIMITATION AND FUTURE RESEARCH\n")
        lines.append(final_output_text)

    return "\n".join(lines)


# =========================================================
# INIT
# =========================================================
init_db()


# =========================================================
# SIDEBAR
# =========================================================
st.sidebar.markdown("## ⚙️ Control Room")

api_key = get_api_key()
manual_key = st.sidebar.text_input("Gemini API Key", type="password")
if manual_key.strip():
    api_key = manual_key.strip()

model_name = st.sidebar.selectbox(
    "Gemini Model",
    [DEFAULT_MODEL, "gemini-2.5-pro", "gemini-2.5-flash-lite"],
    index=0,
)

if st.sidebar.button("Clear Current Session"):
    for key, val in defaults.items():
        st.session_state[key] = val
    st.rerun()


# =========================================================
# HEADER
# =========================================================
st.markdown("<div class='shell'>", unsafe_allow_html=True)

logo_col, title_col = st.columns([1, 3])
with logo_col:
    if os.path.exists(LOGO_PATH):
        try:
            img = Image.open(LOGO_PATH)
            st.image(img, use_container_width=True)
        except Exception:
            st.warning("Logo file could not be displayed.")
    else:
        st.info("Put 'ABT LOGO.jpg' in the same folder as this app.")

with title_col:
    st.markdown(
        f"""
    <div class="hero">
        <div class="hero-title">{APP_NAME}</div>
        <div class="hero-sub">
            Upload all your tables at once, generate APA-style tables, interpret everything at once,
            then produce summary of findings, discussion of findings, conclusion, implication,
            recommendations, limitation, and future research in one workflow.
        </div>
        <div class="badge-row">
            <div class="badge-pill">DOCX + PDF Upload</div>
            <div class="badge-pill">Interpret All At Once</div>
            <div class="badge-pill">APA-Style Tables</div>
            <div class="badge-pill">Summary First</div>
            <div class="badge-pill">Full Report Option</div>
            <div class="badge-pill">Saved Histories</div>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

c1, c2, c3 = st.columns(3)
with c1:
    st.markdown(
        f"""
    <div class="stat-card">
        <div class="stat-label">Detected Tables</div>
        <div class="stat-value">{len(st.session_state.parsed_tables)}</div>
    </div>
    """,
        unsafe_allow_html=True,
    )
with c2:
    st.markdown(
        f"""
    <div class="stat-card">
        <div class="stat-label">Interpreted Tables</div>
        <div class="stat-value">{len(st.session_state.interpreted_tables)}</div>
    </div>
    """,
        unsafe_allow_html=True,
    )
with c3:
    st.markdown(
        f"""
    <div class="stat-card">
        <div class="stat-label">Saved Histories</div>
        <div class="stat-value">{len(load_history(500))}</div>
    </div>
    """,
        unsafe_allow_html=True,
    )

if not api_key:
    st.warning("Add your GEMINI_API_KEY in Streamlit secrets or paste it in the sidebar.")

tab1, tab2, tab3, tab4 = st.tabs([
    "📥 Load Data",
    "📊 Tables & Reports",
    "🧠 Empirical Review",
    "🗂 Saved Histories",
])


# =========================================================
# TAB 1 - LOAD DATA
# =========================================================
with tab1:
    st.markdown("<div class='section-card'>", unsafe_allow_html=True)
    st.subheader("Load All Analysis Tables")

    upload_file = st.file_uploader(
        "Upload DOCX or PDF containing your analysis tables",
        type=["docx", "pdf"],
    )

    pasted_text = st.text_area(
        "Or paste all your tables here",
        height=320,
        placeholder="Paste all your demographic, descriptive, and inferential tables here...",
    )

    if st.button("Load and Detect All Tables"):
        combined = ""

        if upload_file is not None:
            extracted = extract_text_from_upload(upload_file)
            if extracted.startswith("ERROR:"):
                st.error(extracted)
            else:
                combined += extracted + "\n\n"

        if pasted_text.strip():
            combined += pasted_text.strip()

        combined = clean_text(combined)

        if not combined:
            st.warning("Upload a file or paste your tables first.")
        else:
            chunks = split_tables_from_text(combined)
            parsed = [parse_table_chunk(c) for c in chunks]

            for i, table in enumerate(parsed, start=1):
                table["apa_title"] = build_apa_title(table, i)
                table["source_line"] = extract_source_line(table["chunk"])

            st.session_state.raw_analysis_text = combined
            st.session_state.parsed_tables = parsed
            st.session_state.interpreted_tables = []
            st.session_state.summary_of_findings = ""
            st.session_state.discussion_of_findings = ""
            st.session_state.final_outputs = ""
            st.session_state.full_report = ""
            st.success(f"{len(parsed)} tables were detected and prepared.")

            if api_key and parsed:
                with st.spinner("Auto-interpreting all tables..."):
                    prompt = build_all_interpretations_prompt(parsed)
                    raw_interpretations = gemini_text(api_key, prompt, model_name)
                    if raw_interpretations.startswith("ERROR:"):
                        st.error(raw_interpretations)
                    else:
                        interpreted = parse_bulk_interpretations(raw_interpretations, parsed)
                        st.session_state.interpreted_tables = interpreted
                        save_history("Table Interpretations", "tables", raw_interpretations[:20000])
                        st.success("All tables interpreted successfully!")

    if st.session_state.raw_analysis_text:
        with st.expander("Preview Loaded Content"):
            st.text_area("Loaded Text", st.session_state.raw_analysis_text[:20000], height=320)

    st.markdown("</div>", unsafe_allow_html=True)


# =========================================================
# TAB 2 - TABLES AND REPORTS
# =========================================================
with tab2:
    st.markdown("<div class='section-card'>", unsafe_allow_html=True)
    st.subheader("Generate All Interpretations and Final Outputs")

    if not st.session_state.parsed_tables:
        st.info("Load your tables first in the Load Data tab.")
    else:
        st.markdown(
            """
        <div class='info-box'>
        This section displays interpreted tables, summary of findings, discussion of findings,
        conclusion, implication of the study, recommendations, limitation of the study,
        future research suggestions, and a downloadable report.
        </div>
        """,
            unsafe_allow_html=True,
        )

        if st.session_state.interpreted_tables:
            st.markdown("### TABLE INTERPRETATIONS")
            for item in st.session_state.interpreted_tables:
                with st.expander(item["apa_title"]):
                    st.markdown(item["interpretation"])

            if st.button("📋 Generate Summary of Findings", use_container_width=True):
                if not api_key:
                    st.error("Gemini API key is missing.")
                else:
                    with st.spinner("Generating summary of findings..."):
                        prompt = build_summary_prompt(st.session_state.interpreted_tables)
                        summary_text = gemini_text(api_key, prompt, model_name)
                        if summary_text.startswith("ERROR:"):
                            st.error(summary_text)
                        else:
                            st.session_state.summary_of_findings = summary_text
                            save_history("Summary of Findings", "summary", summary_text[:20000])
                            st.success("Summary of findings generated successfully!")

        if st.session_state.summary_of_findings:
            st.markdown("### SUMMARY OF FINDINGS")
            st.markdown(st.session_state.summary_of_findings)

        if st.session_state.interpreted_tables and st.session_state.summary_of_findings:
            st.markdown("### GENERATE COMPLETE REPORT SECTIONS")
            col1, col2, col3 = st.columns(3)

            with col1:
                if st.button("📚 Generate Discussion of Findings", use_container_width=True):
                    if not api_key:
                        st.error("Gemini API key is missing.")
                    elif not st.session_state.empirical_review_text:
                        st.error("Please load empirical review first in the Empirical Review tab.")
                    else:
                        non_demo = [
                            t for t in st.session_state.interpreted_tables
                            if t["section_tag"] != "DEMOGRAPHIC"
                        ]
                        if non_demo:
                            with st.spinner("Generating discussion of findings..."):
                                prompt = build_discussion_prompt(non_demo, st.session_state.empirical_review_text)
                                text = gemini_text(api_key, prompt, model_name)
                                if text.startswith("ERROR:"):
                                    st.error(text)
                                else:
                                    st.session_state.discussion_of_findings = text
                                    save_history("Discussion of Findings", "discussion", text[:20000])
                                    st.success("Discussion of findings generated successfully!")
                        else:
                            st.error("No non-demographic tables found for discussion generation.")

            with col2:
                if st.button("📝 Generate Conclusion Pack", use_container_width=True):
                    if not api_key:
                        st.error("Gemini API key is missing.")
                    else:
                        non_demo = [
                            t for t in st.session_state.interpreted_tables
                            if t["section_tag"] != "DEMOGRAPHIC"
                        ]
                        if non_demo:
                            with st.spinner("Generating conclusion, implication, recommendations, limitation and future research..."):
                                prompt = build_final_outputs_prompt(non_demo, st.session_state.study_context_text)
                                text = gemini_text(api_key, prompt, model_name)
                                if text.startswith("ERROR:"):
                                    st.error(text)
                                else:
                                    st.session_state.final_outputs = text
                                    save_history("Final Outputs", "final_outputs", text[:20000])
                                    st.success("Conclusion pack generated successfully!")
                        else:
                            st.error("No non-demographic tables found for final outputs generation.")

            with col3:
                if st.button("🚀 Generate Full Report Now", use_container_width=True):
                    if not api_key:
                        st.error("Gemini API key is missing.")
                    elif not st.session_state.empirical_review_text:
                        st.error("Please load empirical review first in the Empirical Review tab.")
                    else:
                        non_demo = [
                            t for t in st.session_state.interpreted_tables
                            if t["section_tag"] != "DEMOGRAPHIC"
                        ]
                        if non_demo:
                            with st.spinner("Generating discussion, conclusion, implication, recommendations, limitation and future research..."):
                                discussion_prompt = build_discussion_prompt(non_demo, st.session_state.empirical_review_text)
                                discussion_text = gemini_text(api_key, discussion_prompt, model_name)
                                if discussion_text.startswith("ERROR:"):
                                    st.error(discussion_text)
                                else:
                                    final_prompt = build_final_outputs_prompt(non_demo, st.session_state.study_context_text)
                                    final_text = gemini_text(api_key, final_prompt, model_name)
                                    if final_text.startswith("ERROR:"):
                                        st.error(final_text)
                                    else:
                                        st.session_state.discussion_of_findings = discussion_text
                                        st.session_state.final_outputs = final_text
                                        save_history("Discussion of Findings", "discussion", discussion_text[:20000])
                                        save_history("Final Outputs", "final_outputs", final_text[:20000])
                                        st.success("Full report sections generated successfully!")
                        else:
                            st.error("No non-demographic tables found for full report generation.")

        if st.session_state.discussion_of_findings:
            st.markdown("### DISCUSSION OF FINDINGS")
            st.markdown(st.session_state.discussion_of_findings)

        if st.session_state.final_outputs:
            st.markdown("### CONCLUSION, IMPLICATION, RECOMMENDATIONS, LIMITATION & FUTURE RESEARCH")
            st.markdown(st.session_state.final_outputs)

        if st.session_state.interpreted_tables:
            st.session_state.full_report = export_full_report(
                all_tables=st.session_state.interpreted_tables,
                summary_text=st.session_state.summary_of_findings,
                discussion_text=st.session_state.discussion_of_findings,
                final_output_text=st.session_state.final_outputs,
            )

        if st.session_state.full_report:
            st.download_button(
                "Download Full Report (.md)",
                data=st.session_state.full_report,
                file_name="abt_data_analyst_reporter_full_report.md",
                mime="text/markdown",
            )
            if st.button("💾 Save Full Report to History", use_container_width=True):
                save_history("Full Report", "full_report", st.session_state.full_report[:50000])
                st.success("Full report saved to history.")

    st.markdown("</div>", unsafe_allow_html=True)


# =========================================================
# TAB 3 - EMPIRICAL REVIEW
# =========================================================
with tab3:
    st.markdown("<div class='section-card'>", unsafe_allow_html=True)
    st.subheader("Load Empirical Review / Chapter 1–3")

    st.markdown(
        """
    <div class='info-box'>
    This upload will be read in full. The app will automatically extract the empirical review part
    and use only that extracted empirical review for discussion of findings. It will also keep the
    full uploaded text as study context for generating implication of the study and limitation of the study.
    </div>
    """,
        unsafe_allow_html=True,
    )

    chapter_file = st.file_uploader(
        "Upload empirical review / Chapter 1–3 (DOCX or PDF)",
        type=["docx", "pdf"],
        key="empirical_upload",
    )

    empirical_text = st.text_area(
        "Or paste empirical review text here",
        height=280,
        placeholder="Paste your empirical review here...",
    )

    if st.button("Load Empirical Review"):
        combined = ""

        if chapter_file is not None:
            extracted = extract_text_from_upload(chapter_file)
            if extracted.startswith("ERROR:"):
                st.error(extracted)
            else:
                combined += extracted + "\n\n"

        if empirical_text.strip():
            combined += empirical_text.strip()

        combined = clean_text(combined)

        if not combined:
            st.warning("Upload or paste the empirical review first.")
        else:
            empirical_only = extract_empirical_review_section(combined)
            st.session_state.study_context_text = combined
            st.session_state.empirical_review_text = empirical_only
            st.success("Document loaded successfully. Empirical review section was extracted for discussion of findings.")
            save_history("Empirical Review", "empirical_review", empirical_only[:20000])

    if st.session_state.empirical_review_text:
        with st.expander("Preview Extracted Empirical Review"):
            st.text_area(
                "Empirical Review Preview",
                st.session_state.empirical_review_text[:20000],
                height=320,
            )
        with st.expander("Preview Full Study Context"):
            st.text_area(
                "Full Uploaded Document Preview",
                st.session_state.study_context_text[:20000],
                height=320,
            )

    st.markdown("</div>", unsafe_allow_html=True)


# =========================================================
# TAB 4 - HISTORY
# =========================================================
with tab4:
    st.markdown("<div class='section-card'>", unsafe_allow_html=True)
    st.subheader("Saved Histories")

    histories = load_history(200)

    if not histories:
        st.info("No saved history yet.")
    else:
        for item in histories:
            st.markdown("<div class='history-card'>", unsafe_allow_html=True)
            st.markdown(f"**{item['title']}**")
            st.caption(f"{item['created_at']} • {item['content_type']}")
            with st.expander("View Content"):
                st.text_area(f"history_{item['id']}", item["content"], height=220)

            c1, c2 = st.columns(2)
            with c1:
                st.download_button(
                    f"Download {item['title']}",
                    data=item["content"],
                    file_name=f"{item['title'].replace(' ', '_').lower()}.txt",
                    mime="text/plain",
                    key=f"download_{item['id']}",
                )
            with c2:
                if st.button("Delete", key=f"delete_{item['id']}"):
                    delete_history(item["id"])
                    st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("</div>", unsafe_allow_html=True)