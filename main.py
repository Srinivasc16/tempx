from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os
import logging
import re

# Logging setup
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Student Results API", version="6.0")

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
    """Make clean readable keys like College_Test1."""
    key = str(key).strip()
    key = re.sub(r"[^a-zA-Z0-9]+", "", key)
    return key[0].upper() + key[1:] if key else key


def convert_to_flat_json(df: pd.DataFrame, multi: bool):
    """Convert DataFrame into clean JSON with proper keys."""
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
    """Find the correct Roll No column dynamically."""
    for col in df.columns:
        if isinstance(col, tuple):
            if "roll" in str(col[0]).lower() or "roll" in str(col[1]).lower():
                return col
        else:
            if "roll" in str(col).lower():
                return col
    raise HTTPException(status_code=500, detail="Roll No column not found in Excel!")


# ---------------- ENDPOINTS ---------------- #
BASE_URL = "https://tempx.vercel.app"
@app.get("/")
def home():
    endpoints = [
        {
            "Endpoint": "/students",
            "Method": "GET",
            "Description": "Fetch all students' data",
            "Example URL": f"{BASE_URL}/students"
        },
        {
            "Endpoint": "/student/{roll_no}",
            "Method": "GET",
            "Description": "Get details of a student by roll number",
            "Example URL": f"{BASE_URL}/student/23CSE001"
        },
        {
            "Endpoint": "/students/department/{dept}",
            "Method": "GET",
            "Description": "Get all students in a department",
            "Example URL": f"{BASE_URL}/students/department/CSE"
        },
        {
            "Endpoint": "/students/crt/{batch}",
            "Method": "GET",
            "Description": "Get all students in a CRT batch",
            "Example URL": f"{BASE_URL}/students/crt/Batch1"
        },
        {
            "Endpoint": "/average/student/{roll_no}",
            "Method": "GET",
            "Description": "Get a student's average score",
            "Example URL": f"{BASE_URL}/average/student/23CSE001"
        },
        {
            "Endpoint": "/average/department/{dept}",
            "Method": "GET",
            "Description": "Get average score by department",
            "Example URL": f"{BASE_URL}/average/department/CSE"
        },
        {
            "Endpoint": "/average/platform/{platform}",
            "Method": "GET",
            "Description": "Get average score by platform",
            "Example URL": f"{BASE_URL}/average/platform/SuperSet"
        },
        {
            "Endpoint": "/average/overall",
            "Method": "GET",
            "Description": "Get overall average of all students",
            "Example URL": f"{BASE_URL}/average/overall"
        }
    ]
    return {"API Endpoints": endpoints}


@app.get("/students")
def get_all_students():
    """Fetch all students."""
    try:
        df, multi = load_excel()
        return convert_to_flat_json(df, multi)
    except Exception as e:
        logging.exception("Error fetching students")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/student/{roll_no}")
def get_student_by_roll(roll_no: str):
    """Fetch a single student by roll number."""
    try:
        df, multi = load_excel()
        roll_col = find_roll_col(df)
        student_df = df[df[roll_col].astype(str).str.lower() == roll_no.lower()]
        if student_df.empty:
            raise HTTPException(status_code=404, detail="Student not found!")
        return convert_to_flat_json(student_df, multi)[0]
    except Exception as e:
        logging.exception("Error fetching student")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/students/department/{dept}")
def get_students_by_department(dept: str):
    """Fetch students of a specific department."""
    try:
        df, multi = load_excel()
        dept_col = next(col for col in df.columns if "dept" in str(col).lower())
        filtered_df = df[df[dept_col].astype(str).str.lower() == dept.lower()]
        return convert_to_flat_json(filtered_df, multi)
    except StopIteration:
        raise HTTPException(status_code=500, detail="Department column not found!")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/students/crt/{crt_batch}")
def get_students_by_crt(crt_batch: str):
    """Fetch students of a specific CRT batch."""
    try:
        df, multi = load_excel()
        crt_col = next(col for col in df.columns if "crt" in str(col).lower())
        filtered_df = df[df[crt_col].astype(str).str.lower() == crt_batch.lower()]
        return convert_to_flat_json(filtered_df, multi)
    except StopIteration:
        raise HTTPException(status_code=500, detail="CRT column not found!")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/average/platform/{platform}")
def get_platform_average(platform: str):
    """
    Get average score for each test in a given platform.
    Example: /average/platform/College → gives Test1, Test2, etc. averages.
    """
    try:
        df, multi = load_excel()
        platform_cols = [col for col in df.columns if isinstance(col, tuple) and platform.lower() in str(col[0]).lower()]
        if not platform_cols:
            raise HTTPException(status_code=404, detail=f"Platform '{platform}' not found!")
        averages = {}
        for col in platform_cols:
            averages[f"{platform}_{col[1]}"] = round(df[col].mean(), 2)
        return averages
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/average/student/{roll_no}")
def get_student_average(roll_no: str):
    """Get average score per platform for a student."""
    try:
        df, multi = load_excel()
        roll_col = find_roll_col(df)
        student_df = df[df[roll_col].astype(str).str.lower() == roll_no.lower()]
        if student_df.empty:
            raise HTTPException(status_code=404, detail="Student not found!")

        student_row = student_df.iloc[0]
        averages = {}
        for col in df.columns:
            if isinstance(col, tuple) and not "Unnamed" in str(col[1]):
                platform = normalize_key(col[0])
                averages.setdefault(platform, [])
                averages[platform].append(student_row[col])
        return {k: round(sum(v)/len(v), 2) for k, v in averages.items()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/average/department/{dept}")
def get_department_average(dept: str):
    """Get department-wise average score per platform."""
    try:
        df, multi = load_excel()
        dept_col = next(col for col in df.columns if "dept" in str(col).lower())
        dept_df = df[df[dept_col].astype(str).str.lower() == dept.lower()]
        if dept_df.empty:
            raise HTTPException(status_code=404, detail="No students found for this department!")
        averages = {}
        for col in dept_df.columns:
            if isinstance(col, tuple) and not "Unnamed" in str(col[1]):
                platform = normalize_key(col[0])
                averages.setdefault(platform, [])
                averages[platform].append(dept_df[col].mean())
        return {k: round(sum(v)/len(v), 2) for k, v in averages.items()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/topper/{platform}")
def get_platform_topper(platform: str):
    """Get the topper for a given platform based on total marks."""
    try:
        df, multi = load_excel()
        roll_col = find_roll_col(df)
        platform_cols = [col for col in df.columns if isinstance(col, tuple) and platform.lower() in str(col[0]).lower()]
        df["Total"] = df[platform_cols].sum(axis=1)
        topper_row = df.loc[df["Total"].idxmax()]
        return {"RollNo": topper_row[roll_col], "TotalMarks": topper_row["Total"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
