import os
import pandas as pd
from fastapi import FastAPI, Depends, Request, UploadFile, File
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from database import engine, get_db, DATABASE_URL
from models import Base, KPIData
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)

# Konfigurasi LLM via Dinoiki
llm = ChatOpenAI(
    model="gpt-4o",
    openai_api_key=os.getenv("DINOIKI_API_KEY"),
    base_url="https://ai.dinoiki.com/v1",
    temperature=0.7
)

db_engine = SQLDatabase.from_uri(DATABASE_URL)
# Menggunakan invoke() sebagai pengganti run() yang sudah deprecated
db_chain = SQLDatabaseChain.from_llm(llm, db_engine, verbose=True)

@app.get("/", response_class=HTMLResponse)
async def chat_ui(request: Request):
    # PERBAIKAN: Menggunakan parameter request secara eksplisit
    return templates.TemplateResponse(
        request=request, 
        name="chatbot.html"
    )

@app.post("/upload-sync")
async def upload_sync(file: UploadFile = File(...), db: Session = Depends(get_db)):
    file_location = f"temp_{file.filename}"
    with open(file_location, "wb") as f:
        f.write(await file.read())
    
    try:
        df = pd.read_excel(file_location)
        # Hapus data lama agar tidak duplikat
        db.query(KPIData).delete()
        
        for _, row in df.iterrows():
            item = KPIData(
                id_kpi=str(row['ID_KPI']),
                ru=str(row['RU (Refinery Unit)']),
                area=str(row['Area']),
                kategori=str(row['Kategori_Maintenance']),
                nama_kpi=str(row['Nama_KPI']),
                deskripsi=str(row['Deskripsi_Indikator']),
                satuan=str(row['Satuan']),
                target=float(row['Target_Tahunan']),
                realisasi=float(row['Realisasi_YTD']),
                pencapaian=str(row['Pencapaian_Persen']),
                status=str(row['Status']),
                analisis=str(row['Analisis_Penyebab']),
                rekomendasi=str(row['Rekomendasi_Tindakan'])
            )
            db.add(item)
        
        db.commit()
        if os.path.exists(file_location):
            os.remove(file_location)
            
        return {"message": "Data Berhasil Diupdate ke PostgreSQL!"}
    except Exception as e:
        db.rollback() # Batalkan transaksi jika error
        if os.path.exists(file_location):
            os.remove(file_location)
        return {"error": str(e)}

@app.get("/ask")
async def ask_ai(question: str):
    try:
        # Menambahkan context agar LLM paham table schema
        response = db_chain.invoke({"query": question})
        return {"answer": response["result"]}
    except Exception as e:
        return {"error": f"AI Error: {str(e)}"}