from fastapi import FastAPI, UploadFile, BackgroundTasks
from fastapi.responses import FileResponse
from.processor import run_meteorite_analysis
import shutil, os

app = FastAPI(title="NexusSpark Advanced Service")
UPLOAD_DIR = "uploads"

@app.post("/upload")
async def upload_and_process(file: UploadFile, background_tasks: BackgroundTasks):
    # Save uploaded file
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Advanced: Trigger processing in background so user doesn't wait
    background_tasks.add_task(run_meteorite_analysis, file_path)
    
    return {
        "status": "Processing Started",
        "filename": file.filename,
        "info": "The ML model is running in the background. Check /results later."
    }

@app.get("/download/{filename}")
async def download_result(filename: str):
    return FileResponse(path=f"results/{filename}", filename=filename)