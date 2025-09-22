from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import motor.motor_asyncio
import pandas as pd
from io import BytesIO
import re

app = FastAPI()

# Allow CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB Connection
MONGO_URI = "mongodb://localhost:27017"
client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
db = client["studentDB"]
students_collection = db["students"]

# ✅ Helper: Flatten multi-level headers
def flatten_columns(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join([str(level) for level in col if str(level) != "nan"])
            for col in df.columns.values
        ]
    else:
        df.columns = [str(col) for col in df.columns]
    return df

# ✅ Upload Excel (MERGE instead of REPLACE)
@app.post("/upload-excel")
async def upload_excel(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents), header=[0, 1])  # read multi-level headers
        df = flatten_columns(df)

        if "RollNo" not in df.columns:
            return {"message": "Excel must contain RollNo column!"}

        # Convert each row into dict and upsert into MongoDB
        for _, row in df.iterrows():
            roll_no = row["RollNo"]
            student_data = row.to_dict()

            # Upsert (insert if new, update if existing)
            await students_collection.update_one(
                {"_id": roll_no},
                {"$set": student_data},
                upsert=True,
            )

        return {"message": "Excel uploaded and merged successfully!"}

    except Exception as e:
        return {"message": f"Upload failed: {str(e)}"}

# ✅ Fetch all students
@app.get("/students")
async def get_students():
    students = await students_collection.find().to_list(1000)
    return students

# ✅ Fetch one student
@app.get("/student/{roll_no}")
async def get_student(roll_no: str):
    student = await students_collection.find_one({"_id": roll_no})
    return student

# ✅ Fetch students by department
@app.get("/students/department/{dept}")
async def get_students_by_department(dept: str):
    students = await students_collection.find({"Department": dept}).to_list(1000)
    return students

# ✅ Fetch students by CRT batch
@app.get("/students/crt/{crt_batch}")
async def get_students_by_crt(crt_batch: str):
    students = await students_collection.find({"CRT_Batch": crt_batch}).to_list(1000)
    return students

# ✅ Average marks per platform
@app.get("/average/platform/{platform}")
async def average_platform(platform: str):
    pipeline = [
        {"$group": {
            "_id": None,
            "avg_score": {"$avg": f"${platform}"}
        }}
    ]
    result = await students_collection.aggregate(pipeline).to_list(1)
    return result[0] if result else {"avg_score": None}

# ✅ Topper per platform
@app.get("/topper/{platform}")
async def topper(platform: str):
    topper = await students_collection.find().sort(platform, -1).limit(1).to_list(1)
    return topper[0] if topper else {}

# ✅ Average per student
@app.get("/average/student/{roll_no}")
async def average_student(roll_no: str):
    student = await students_collection.find_one({"_id": roll_no})
    if not student:
        return {"error": "Student not found"}
    scores = [v for k, v in student.items() if re.search(r"Test\d+", k) and isinstance(v, (int, float))]
    avg = sum(scores) / len(scores) if scores else None
    return {"roll_no": roll_no, "average": avg}

# ✅ Average per department
@app.get("/average/department/{dept}")
async def average_department(dept: str):
    students = await students_collection.find({"Department": dept}).to_list(1000)
    scores = []
    for s in students:
        scores += [v for k, v in s.items() if re.search(r"Test\d+", k) and isinstance(v, (int, float))]
    avg = sum(scores) / len(scores) if scores else None
    return {"department": dept, "average": avg}

# ✅ Topper per department
@app.get("/topper/department/{dept}")
async def topper_department(dept: str):
    students = await students_collection.find({"Department": dept}).to_list(1000)
    if not students:
        return {}
    toppers = max(students, key=lambda s: sum([v for k, v in s.items() if re.search(r"Test\d+", k) and isinstance(v, (int, float))]))
    return toppers

# ✅ Overall average
@app.get("/average/overall")
async def average_overall():
    students = await students_collection.find().to_list(1000)
    scores = []
    for s in students:
        scores += [v for k, v in s.items() if re.search(r"Test\d+", k) and isinstance(v, (int, float))]
    avg = sum(scores) / len(scores) if scores else None
    return {"overall_average": avg}

# ✅ Overall topper
@app.get("/overall/topper")
async def overall_topper():
    students = await students_collection.find().to_list(1000)
    if not students:
        return {}
    topper = max(students, key=lambda s: sum([v for k, v in s.items() if re.search(r"Test\d+", k) and isinstance(v, (int, float))]))
    return topper
