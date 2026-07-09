import os, json, time, random, string, shutil
from typing import Optional
from fastapi import FastAPI, UploadFile, File, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import OpenAI
from dotenv import load_dotenv
from pdf_processor import extract_text_from_pdf, chunk_text

load_dotenv()
app = FastAPI(title="Symposia AI Platform", version="2.5.0")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

UPLOAD_DIR = "./uploaded_docs"
DB_FILE = "./symposia_store.json"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def load_permanent_db():
    default = {"rooms": {}}
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                return data if isinstance(data, dict) and "rooms" in data else default
            except: return default
    return default

def save_permanent_db(data):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

try:
    ai_client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=os.getenv("OPENAI_API_KEY"))
except: ai_client = None

# --- MODELS ---
class CreateRoomRequest(BaseModel): password: Optional[str] = None
class JoinRoomRequest(BaseModel): password: Optional[str] = None
class QueryRequest(BaseModel): room_key: str; filename: Optional[str] = None; question: str; mode: Optional[str] = "normal" 
class PomodoroControl(BaseModel): action: str; duration_minutes: int = 25
class DebateArgument(BaseModel): room_key: str; username: str; argument: str; context_file: Optional[str] = None

class ConnectionManager:
    def __init__(self): self.active_connections = {}
    async def connect(self, ws, room):
        await ws.accept()
        if room not in self.active_connections: self.active_connections[room] = []
        self.active_connections[room].append(ws)
    def disconnect(self, ws, room):
        if room in self.active_connections: 
            self.active_connections[room].remove(ws)
            if not self.active_connections[room]: del self.active_connections[room]
    async def broadcast(self, msg, room):
        if room in self.active_connections:
            for conn in self.active_connections[room]: await conn.send_text(msg)
    def get_active_users(self, room):
        return len(self.active_connections.get(room, []))

manager = ConnectionManager()

# --- HELPER: SCORE BROADCASTER ---
async def broadcast_scores(room_key: str):
    db = load_permanent_db()
    scores = db["rooms"][room_key].get("members", {})
    await manager.broadcast(f"SYS_SCORE|{json.dumps(scores)}", room_key)

@app.websocket("/ws/chat/{room_key}/{username}")
async def chat_endpoint(ws: WebSocket, room_key: str, username: str):
    room = room_key.upper().strip()
    db = load_permanent_db()
    
    # Initialize user in tracker if new
    if "members" not in db["rooms"][room]: db["rooms"][room]["members"] = {}
    if username not in db["rooms"][room]["members"]:
        db["rooms"][room]["members"][username] = {"chat": 0, "quiz": 0, "debate": 0, "total": 0}
        save_permanent_db(db)

    await manager.connect(ws, room)
    await manager.broadcast(f"🟢|{username} joined the workspace.", room)
    await broadcast_scores(room) # Sync scores on join
    
    try:
        while True:
            data = await ws.receive_text()
            
            # Intercept system commands so they don't count as chat XP
            if not data.startswith("SYS_"):
                # Award 5 XP for active chat participation
                db = load_permanent_db()
                db["rooms"][room]["members"][username]["chat"] += 5
                db["rooms"][room]["members"][username]["total"] += 5
                save_permanent_db(db)
                await broadcast_scores(room)
                
            await manager.broadcast(f"💬|{username}|{data}", room)
    except WebSocketDisconnect:
        manager.disconnect(ws, room)
        await manager.broadcast(f"🔴|{username} left the workspace.", room)

# --- AUTH ENDPOINTS ---
@app.post("/api/v1/rooms/create")
async def create_room(payload: CreateRoomRequest = None):
    db = load_permanent_db()
    while True:
        room_key = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
        if room_key not in db["rooms"]: break
    room_password = payload.password if payload and payload.password else None
    db["rooms"][room_key] = {
        "created_at": time.time(), "password": room_password, "documents": {}, "members": {},
        "pomodoro": {"status": "focus", "is_active": False, "remaining_seconds": 1500, "end_timestamp": 0.0}
    }
    save_permanent_db(db)
    return {"room_key": room_key}

@app.post("/api/v1/rooms/join/{room_key}")
async def join_room(room_key: str, payload: JoinRoomRequest = None):
    db = load_permanent_db()
    room_key = room_key.upper().strip()
    if room_key not in db["rooms"]: raise HTTPException(status_code=404, detail="Invalid Room Key.")
    room_data = db["rooms"][room_key]
    if room_data.get("password") and (not payload or payload.password != room_data["password"]):
        raise HTTPException(status_code=403, detail="Incorrect Room Password.")
    return {"room_key": room_key}

# --- POMODORO ---
@app.get("/api/v1/rooms/{room_key}/pomodoro/status")
async def get_pomodoro_status(room_key: str):
    db = load_permanent_db()
    room_key = room_key.upper().strip()
    pomo = db["rooms"][room_key]["pomodoro"]
    if pomo["is_active"]:
        time_left = int(pomo["end_timestamp"] - time.time())
        if time_left <= 0:
            pomo["is_active"] = False; pomo["remaining_seconds"] = 0
            save_permanent_db(db)
            return {"status": pomo["status"], "is_active": False, "remaining_seconds": 0}
        return {"status": pomo["status"], "is_active": True, "remaining_seconds": time_left}
    return {"status": pomo["status"], "is_active": False, "remaining_seconds": pomo["remaining_seconds"]}

@app.post("/api/v1/rooms/{room_key}/pomodoro/control")
async def control_pomodoro(room_key: str, payload: PomodoroControl):
    db = load_permanent_db()
    room_key = room_key.upper().strip()
    pomo = db["rooms"][room_key]["pomodoro"]
    action = payload.action.lower().strip()
    if action == "start" and not pomo["is_active"]:
        pomo["is_active"] = True; pomo["end_timestamp"] = time.time() + pomo["remaining_seconds"]
    elif action == "pause" and pomo["is_active"]:
        pomo["is_active"] = False; pomo["remaining_seconds"] = max(0, int(pomo["end_timestamp"] - time.time()))
    elif action == "reset":
        pomo["is_active"] = False; pomo["remaining_seconds"] = payload.duration_minutes * 60; pomo["status"] = "focus" if payload.duration_minutes >= 20 else "break"
    save_permanent_db(db)
    return {"pomodoro": pomo}

# --- AI & DOCUMENTS ---
@app.post("/api/v1/rooms/{room_key}/upload")
async def upload_document(room_key: str, file: UploadFile = File(...)):
    db = load_permanent_db()
    room_key = room_key.upper().strip()
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as buffer: shutil.copyfileobj(file.file, buffer)
    try:
        raw_text = extract_text_from_pdf(file_path) if file.filename.endswith(".pdf") else f"Content of {file.filename} saved."
        db["rooms"][room_key]["documents"][file.filename] = chunk_text(raw_text, 1000, 200) if raw_text else ["No readable text."]
        save_permanent_db(db)
        return {"filename": file.filename}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/query")
async def query_document(payload: QueryRequest):
    db = load_permanent_db()
    room_key = payload.room_key.upper().strip()
    sys_prompt = "You are an elite academic assistant. Provide elaborate, scholarly answers."
    if payload.mode == "challenge": sys_prompt = "You are a ruthless academic interrogator. Critique logic, point out flaws, and ask challenging follow-up questions."
    context = "\n---\n".join(db["rooms"][room_key]["documents"][payload.filename]) if payload.filename and payload.filename in db["rooms"][room_key]["documents"] else ""
    user_prompt = f"[Context]:\n{context}\n\n[User Input]:\n{payload.question}" if context else payload.question
    res = ai_client.chat.completions.create(model="openai/gpt-oss-120b", messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": user_prompt}])
    return {"answer": res.choices[0].message.content}

@app.post("/api/v1/rooms/{room_key}/quiz")
async def generate_quiz(room_key: str, filename: str):
    db = load_permanent_db()
    room_key = room_key.upper().strip()
    context = "\n---\n".join(db["rooms"][room_key]["documents"][filename])
    res = ai_client.chat.completions.create(model="openai/gpt-oss-120b", messages=[
        {"role": "system", "content": 'Generate a 3-question multiple choice quiz based strictly on the text. Respond ONLY with raw JSON: [{"question": "...", "options": ["A", "B", "C", "D"], "correct_answer": "A"}]'},
        {"role": "user", "content": f"[Context]:\n{context}"}
    ])
    return {"quiz": json.loads(res.choices[0].message.content.strip("` \n"))}

# --- NEW: AI DEBATE MODERATOR ---
@app.post("/api/v1/rooms/debate/moderate")
async def moderate_debate(payload: DebateArgument):
    db = load_permanent_db()
    room_key = payload.room_key.upper().strip()
    
    sys_prompt = (
        "You are an elite academic Debate Moderator. Evaluate the user's argument based on logical coherence and evidence. "
        "Award points (up to +50) for strong, well-reasoned points. Deduct points (down to -20) for logical fallacies or ad hominem attacks. "
        "Respond ONLY with a JSON object: {\"verdict\": \"Your brief critique here...\", \"score_change\": 25}"
    )
    
    context = "\n---\n".join(db["rooms"][room_key]["documents"][payload.context_file]) if payload.context_file and payload.context_file in db["rooms"][room_key]["documents"] else "No specific document context provided."
    user_prompt = f"[Context Material]:\n{context}\n\n[Student Argument]:\n{payload.argument}"
    
    res = ai_client.chat.completions.create(model="openai/gpt-oss-120b", messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": user_prompt}])
    
    try:
        verdict_data = json.loads(res.choices[0].message.content.strip("` \n"))
        score = verdict_data.get("score_change", 0)
        
        # Update user's score in DB
        db["rooms"][room_key]["members"][payload.username]["debate"] += score
        db["rooms"][room_key]["members"][payload.username]["total"] += score
        save_permanent_db(db)
        
        # Broadcast new scores
        await broadcast_scores(room_key)
        return verdict_data
    except Exception as e:
        raise HTTPException(status_code=500, detail="Moderation failed to parse.")