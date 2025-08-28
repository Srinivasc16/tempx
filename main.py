from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os
import logging
import re

# Setup logging
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Student Results API", version="5.0")

# Allow frontend calls
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

EXCEL_FILE = "students.xlsx"


def load_excel():
    """Load Excel safely and detect multi-level headers."""
    if not os.path.exists(EXCEL_FILE):
        raise FileNotFoundError("Excel file not found!")

    try:
        df = pd.read_excel(EXCEL_FILE, header=[0, 1])
        if isinstance(df.columns, pd.MultiIndex):
            logging.info("✅ Multi-level header detected.")
            return df, True
    except Exception as e:
        logging.warning(f"Multi-level header read failed, falling back to single: {e}")

    df = pd.read_excel(EXCEL_FILE)
    logging.info("✅ Single-level header detected.")
    return df, False


def normalize_key(key: str) -> str:
    """Make clean CamelCase-like keys."""
    key = str(key).strip()
    key = re.sub(r"[^a-zA-Z0-9]+", "", key)
    return key[0].upper() + key[1:] if key else key


def convert_to_flat_json(df: pd.DataFrame, multi: bool):
    """Convert DataFrame into clean JSON with keys like College_Test1."""
    students = []
    for _, row in df.iterrows():
        student = {}
        for col in df.columns:
            if isinstance(col, tuple):
                top, sub = col
                if "Unnamed" in str(sub) or sub.strip() == "":
                    key = normalize_key(top)
                else:
                    key = f"{normalize_key(top)}_{normalize_key(sub)}"
            else:
                key = normalize_key(col)
            student[key] = row[col]
        students.append(student)
    return students


def find_roll_col(df):
    """Find the correct Roll No column, even in multi-level headers."""
    for col in df.columns:
        if isinstance(col, tuple):
            if "roll" in str(col[0]).lower() or "roll" in str(col[1]).lower():
                return col
        else:
            if "roll" in str(col).lower():
                return col
    raise HTTPException(status_code=500, detail="Roll No column not found in Excel!")


@app.get("/")
def home():
    return {"message": "Welcome to the Student Results API 🚀"}


@app.get("/students")
def get_all_students():
    """Fetch all students in clean JSON format."""
    try:
        df, multi = load_excel()
        return convert_to_flat_json(df, multi)
    except Exception as e:
        logging.exception("Error fetching students")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/student/{roll_no}")
def get_student_by_roll(roll_no: str):
    """Fetch single student details by Roll No."""
    try:
        df, multi = load_excel()
        roll_col = find_roll_col(df)

        # Case-insensitive search
        student_df = df[df[roll_col].astype(str).str.lower() == roll_no.lower()]

        if student_df.empty:
            raise HTTPException(status_code=404, detail="Student not found!")

        return convert_to_flat_json(student_df, multi)[0]
    except Exception as e:
        logging.exception("Error fetching student")
        raise HTTPException(status_code=500, detail=str(e))
