"""
Cerybro CEO Engine v0.3.0
Merkezi ajan ve operasyon yönetim motoru.

v0.2.0 -> v0.3.0 düzeltmeleri:
- Çift tanımlı görev sistemi tek sisteme indirildi (eski API adresleri korundu)
- Tüm tablolar açılışta oluşturulur (ilk sohbette tablo yok hatası giderildi)
- JSONB alanları artık gerçek liste/obje döner (eskiden metin dönüyordu)
- Ollama sohbet/görev çağrılarında doğru adrese bağlanır
- CEO model ayarı artık ajanlara da uygulanır (ajanın kendi modeli yoksa)
- Sohbet geçmişi varsayılan adresi sohbetle aynı yapıldı
- Girişte deneme sınırı (Redis) ve şifre kontrolü ana akışı bloklamaz
- API anahtarındaki boşluk/satır sonu otomatik temizlenir
"""
import asyncio
import base64
import hashlib
import hmac
import json
import os
import re
import time
from contextlib import asynccontextmanager
from typing import Optional, Union

import asyncpg
import redis.asyncio as redis
from cryptography.fernet import Fernet
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


# ---------------------------------------------------------------- AYARLAR
DATABASE_URL = os.environ["DATABASE_URL"]
REDIS_URL = os.environ["REDIS_URL"]
GOOGLE_CLIENT_ID = os.environ.get(
    "CERYBRO_GOOGLE_CLIENT_ID",
    "184405280042-djfna9m6gt4ban51o0lhrsjoil7e6m15.apps.googleusercontent.com",
).strip()
ADMIN_EMAIL = "ikanto81@gmail.com"
AUTH_SECRET = os.environ["CERYBRO_SECRET_KEY"]
OLLAMA_API_BASE = os.getenv("OLLAMA_API_BASE", "http://cerybro-ollama:11434")
TOKEN_TTL = 43200
VERSION = "0.3.0"

db_pool = None
redis_client = None

DEFAULT_AGENTS = [
    ("Vivo", "Vivaqua", "Dijital vivaryum projeleri", "#08b5a5", "rounded", "◆"),
    ("Gyro", "GallopX", "Yazılım ve operasyon", "#f36b08", "circle", "●"),
    ("Aveo", "Aetheria", "Strateji ve dijital projeler", "#08b5a5", "hex", "◆"),
    ("Pyro", "SEO & Ads", "SEO ve reklam yönetimi", "#f39808", "triangle", "▲"),
    ("Tixo", "Timotee", "Baskı pazaryeri yönetimi", "#1685ed", "circle", "●"),
    ("Sleepo", "SleepyBaby", "Bebek ve uyku projesi", "#f32649", "circle", "●"),
    ("Campo", "Campora App", "Uygulama geliştirme", "#f39808", "rounded", "●"),
    ("Affro", "AffilieightX", "Affiliate pazarlama", "#1685ed", "circle", "●"),
    ("Picasso", "Designer", "Tasarım ve görsel üretim", "#9255ed", "hex", "⬢"),
    ("Spyro", "Lead & Sherlock", "Araştırma ve müşteri bulma", "#f32649", "hex", "⬢"),
    ("Caira", "Store", "E-ticaret ve mağaza yönetimi", "#f39808", "circle", "●"),
    ("Timo", "Timox", "Full-stack geliştirme", "#1685ed", "circle", "●"),
]

MODEL_PROVIDERS = {
    "gemini": "gemini",
    "claude": "anthropic",
    "openai": "openai",
    "grok": "xai",
    "openrouter": "openrouter",
    "ollama": "ollama",
}

TASK_STATUSES = {"queued", "assigned", "running", "completed", "failed", "cancelled"}
PRIORITY_NAMES = {"low": 1, "normal": 3, "high": 4, "urgent": 5}


# ------------------------------------------------------- VERİTABANI KURULUMU
async def _init_connection(conn):
    """JSONB alanları Python liste/sözlük olarak okunsun ve yazılsın."""
    def enc(value):
        return json.dumps(value, ensure_ascii=False)

    for typ in ("jsonb", "json"):
        await conn.set_type_codec(
            typ, encoder=enc, decoder=json.loads, schema="pg_catalog"
        )


async def _migrate_tasks(conn):
    """Eski (çift şemalı) görev tablosunu tek şemaya taşır. Tekrar çalışması zararsız."""
    cols = {
        r["column_name"]: (r["data_type"], r["character_maximum_length"])
        for r in await conn.fetch("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name='cerybro_tasks' AND table_schema=current_schema()
        """)
    }

    async with conn.transaction():
        if cols.get("priority", ("", None))[0] == "character varying":
            await conn.execute("ALTER TABLE cerybro_tasks ALTER COLUMN priority DROP DEFAULT")
            await conn.execute("""
                ALTER TABLE cerybro_tasks ALTER COLUMN priority TYPE SMALLINT
                USING (CASE priority WHEN 'low' THEN 1 WHEN 'high' THEN 4
                                     WHEN 'urgent' THEN 5 ELSE 3 END)
            """)
            await conn.execute("ALTER TABLE cerybro_tasks ALTER COLUMN priority SET DEFAULT 3")

        if cols.get("result", ("", None))[0] == "text":
            await conn.execute("ALTER TABLE cerybro_tasks ALTER COLUMN result DROP NOT NULL")
            await conn.execute("ALTER TABLE cerybro_tasks ALTER COLUMN result DROP DEFAULT")
            await conn.execute("""
                ALTER TABLE cerybro_tasks ALTER COLUMN result TYPE JSONB
                USING (CASE WHEN result IS NULL OR result='' THEN NULL
                            ELSE jsonb_build_object('output', result) END)
            """)

        length = cols.get("title", ("", 180))[1]
        if length is not None and length < 180:
            await conn.execute("ALTER TABLE cerybro_tasks ALTER COLUMN title TYPE VARCHAR(180)")

        await conn.execute("ALTER TABLE cerybro_tasks ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb")
        await conn.execute("ALTER TABLE cerybro_tasks ADD COLUMN IF NOT EXISTS error TEXT")
        await conn.execute("ALTER TABLE cerybro_tasks ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ")
        await conn.execute("ALTER TABLE cerybro_tasks ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ")


async def _create_schema(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS agents (
            id BIGSERIAL PRIMARY KEY,
            name VARCHAR(80) NOT NULL UNIQUE,
            role VARCHAR(120) NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            color VARCHAR(7) NOT NULL DEFAULT '#1685ed',
            shape VARCHAR(20) NOT NULL DEFAULT 'rounded',
            symbol VARCHAR(12) NOT NULL DEFAULT '✦',
            model_provider VARCHAR(40),
            model_name VARCHAR(100),
            status VARCHAR(20) NOT NULL DEFAULT 'active',
            memory_enabled BOOLEAN NOT NULL DEFAULT TRUE,
            permissions JSONB NOT NULL DEFAULT '[]'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS cerybro_tasks (
            id BIGSERIAL PRIMARY KEY,
            title VARCHAR(180) NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            status VARCHAR(20) NOT NULL DEFAULT 'queued'
                CHECK (status IN ('queued','assigned','running','completed','failed','cancelled')),
            priority SMALLINT NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 5),
            assigned_agent_id BIGINT REFERENCES agents(id) ON DELETE SET NULL,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            result JSONB,
            error TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ
        )
    """)
    await _migrate_tasks(conn)
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_cerybro_tasks_status_priority ON cerybro_tasks(status, priority DESC, created_at)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_cerybro_tasks_agent ON cerybro_tasks(assigned_agent_id, status)")

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS cerybro_chat_messages (
            id BIGSERIAL PRIMARY KEY,
            conversation_id VARCHAR(100) NOT NULL,
            role VARCHAR(20) NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_cerybro_chat_conv ON cerybro_chat_messages(conversation_id, id)")

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS model_credentials (
            provider VARCHAR(30) PRIMARY KEY,
            encrypted_key TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS cerybro_ceo_settings (
            id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id=1),
            provider VARCHAR(30) NOT NULL,
            model_name VARCHAR(120) NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    # v0.3.0: idempotent proje/ajan adi guncellemeleri
    await conn.execute("""
        UPDATE agents SET name='Tixo', role='Timotee', description='Baskı pazaryeri yönetimi'
        WHERE name='Dsigno' AND NOT EXISTS (SELECT 1 FROM agents WHERE name='Tixo')
    """)
    await conn.execute("UPDATE agents SET role='Timotee' WHERE role='Design Market'")
    await conn.execute("UPDATE agents SET role='AffilieightX' WHERE role='AffiliateX'")

    for name, role, description, color, shape, symbol in DEFAULT_AGENTS:
        await conn.execute("""
            INSERT INTO agents (name, role, description, color, shape, symbol)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (name) DO NOTHING
        """, name, role, description, color, shape, symbol)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, redis_client
    db_pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=1, max_size=5, init=_init_connection
    )
    redis_client = redis.from_url(
        REDIS_URL, socket_connect_timeout=5, socket_timeout=5,
        decode_responses=True
    )
    async with db_pool.acquire() as conn:
        await _create_schema(conn)

    yield

    try:
        await redis_client.aclose()
    except Exception:
        pass
    if db_pool:
        await db_pool.close()


app = FastAPI(
    title="Cerybro CEO Engine",
    version=VERSION,
    description="Cerybro merkezi ajan ve operasyon yönetim motoru",
    lifespan=lifespan,
)


# ------------------------------------------------------------ GİRİŞ / YETKİ
def _b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _new_token():
    header = _b64(b'{"alg":"HS256","typ":"JWT"}')
    payload = _b64(json.dumps({
        "sub": "admin", "iat": int(time.time()),
        "exp": int(time.time()) + TOKEN_TTL
    }, separators=(",", ":")).encode())
    body = header + "." + payload
    sig = hmac.new(AUTH_SECRET.encode(), body.encode(), hashlib.sha256).digest()
    return body + "." + _b64(sig)


def _valid_token(token):
    try:
        a, b, sig = token.split(".")
        body = a + "." + b
        expected = _b64(hmac.new(
            AUTH_SECRET.encode(), body.encode(), hashlib.sha256
        ).digest())
        if not hmac.compare_digest(sig, expected):
            return False
        payload = json.loads(base64.urlsafe_b64decode(
            b + "=" * ((4 - len(b) % 4) % 4)
        ))
        return payload.get("sub") == "admin" and int(payload["exp"]) > int(time.time())
    except Exception:
        return False


LOGIN_WINDOW = 900  # 15 dakika
LOGIN_LIMIT_IP = 5
LOGIN_LIMIT_ALL = 30


def _client_ip(request: Request):
    forwarded = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
    if forwarded:
        return forwarded
    return request.client.host if request.client else "unknown"


async def _login_keys_blocked(keys):
    """Redis çalışmıyorsa giriş engellenmez (kendini dışarıda bırakma)."""
    try:
        for key, limit in zip(keys, (LOGIN_LIMIT_IP, LOGIN_LIMIT_ALL)):
            value = await redis_client.get(key)
            if value and int(value) >= limit:
                return True
    except Exception:
        return False
    return False


async def _login_record_failure(keys):
    try:
        for key in keys:
            count = await redis_client.incr(key)
            if count == 1:
                await redis_client.expire(key, LOGIN_WINDOW)
    except Exception:
        pass


class GoogleLoginInput(BaseModel):
    credential: str = Field(min_length=20, max_length=8192)


def _verify_google_id_token(credential):
    import urllib.parse
    import urllib.request
    url = "https://oauth2.googleapis.com/tokeninfo?id_token=" + urllib.parse.quote(credential, safe="")
    try:
        with urllib.request.urlopen(url, timeout=8) as r:
            info = json.loads(r.read())
    except Exception:
        return None
    if info.get("aud") != GOOGLE_CLIENT_ID:
        return None
    if info.get("iss") not in ("accounts.google.com", "https://accounts.google.com"):
        return None
    try:
        if int(info.get("exp", 0)) < int(time.time()):
            return None
    except Exception:
        return None
    return info


@app.get("/api/auth/config")
async def auth_config():
    return {"google_client_id": GOOGLE_CLIENT_ID}


@app.post("/api/auth/google")
async def google_login(data: GoogleLoginInput, request: Request):
    keys = ["cerybro:login_fail:ip:" + _client_ip(request), "cerybro:login_fail:all"]
    if await _login_keys_blocked(keys):
        raise HTTPException(429, "Çok fazla hatalı deneme. 15 dakika sonra tekrar dene.")
    info = await asyncio.to_thread(_verify_google_id_token, data.credential)
    email = str((info or {}).get("email", "")).strip().lower()
    verified = str((info or {}).get("email_verified", "")).lower() == "true"
    if not info or email != ADMIN_EMAIL or not verified:
        await _login_record_failure(keys)
        raise HTTPException(status_code=403, detail="Bu hesapla giriş yetkisi yok")
    try:
        await redis_client.delete(keys[0])
    except Exception:
        pass
    return {"access_token": _new_token(), "token_type": "bearer", "expires_in": TOKEN_TTL}


@app.post("/api/login")
async def admin_login_removed():
    raise HTTPException(status_code=410, detail="Şifreli giriş kaldırıldı. Google ile giriş yapın.")


@app.middleware("http")
async def cerybro_auth_guard(request: Request, call_next):
    path = request.url.path
    if path.startswith("/api/") and path not in ("/api/login", "/api/auth/google", "/api/auth/config"):
        header = request.headers.get("authorization", "")
        token = header[7:].strip() if header.lower().startswith("bearer ") else ""
        if not _valid_token(token):
            return JSONResponse(status_code=401, content={"detail": "Yönetici girişi gerekli"})
    return await call_next(request)


# ---------------------------------------------------------- SAĞLIK KONTROLÜ
@app.get("/")
async def root():
    return {"name": "Cerybro CEO Engine", "version": VERSION, "status": "running"}


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "cerybro-api"}


@app.get("/ready")
async def ready():
    results = {}
    try:
        async with db_pool.acquire() as conn:
            results["postgres"] = await conn.fetchval("SELECT 1")
    except Exception:
        results["postgres"] = "unavailable"

    try:
        results["redis"] = await redis_client.ping()
    except Exception:
        results["redis"] = "unavailable"

    if results.get("postgres") != 1 or results.get("redis") is not True:
        raise HTTPException(status_code=503, detail=results)

    return {"status": "ready", "postgres": "connected", "redis": "connected"}


# ------------------------------------------------------------------ AJANLAR
class AgentCreate(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    role: str = Field(min_length=2, max_length=120)
    description: str = ""
    color: str = "#1685ed"
    shape: str = "rounded"
    symbol: str = Field(default="✦", max_length=12)
    model_provider: Optional[str] = Field(default=None, max_length=40)
    model_name: Optional[str] = Field(default=None, max_length=100)
    memory_enabled: bool = True
    permissions: list[str] = []


class AgentUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=2, max_length=80)
    role: Optional[str] = Field(default=None, min_length=2, max_length=120)
    description: Optional[str] = None
    color: Optional[str] = None
    shape: Optional[str] = None
    symbol: Optional[str] = Field(default=None, max_length=12)
    model_provider: Optional[str] = Field(default=None, max_length=40)
    model_name: Optional[str] = Field(default=None, max_length=100)
    memory_enabled: Optional[bool] = None
    permissions: Optional[list[str]] = None
    status: Optional[str] = None


AGENT_COLUMNS = {
    "name", "role", "description", "color", "shape", "symbol",
    "model_provider", "model_name", "memory_enabled", "permissions", "status"
}
AGENT_NOT_NULL = AGENT_COLUMNS - {"model_provider", "model_name"}


def validate_agent(data):
    if data.get("color") is not None:
        if not re.fullmatch(r"#[0-9a-fA-F]{6}", data["color"]):
            raise HTTPException(400, "Geçersiz renk kodu")

    if data.get("shape") is not None:
        if data["shape"] not in ["circle", "rounded", "hex", "diamond", "triangle"]:
            raise HTTPException(400, "Geçersiz avatar şekli")

    if "status" in data and data["status"] not in ["active", "paused"]:
        raise HTTPException(400, "Geçersiz ajan durumu")


def agent_dict(row):
    return dict(row)


@app.get("/api/agents")
async def list_agents(include_paused: bool = False):
    async with db_pool.acquire() as conn:
        if include_paused:
            rows = await conn.fetch("SELECT * FROM agents ORDER BY created_at, id")
        else:
            rows = await conn.fetch(
                "SELECT * FROM agents WHERE status='active' ORDER BY created_at, id"
            )
    return [agent_dict(row) for row in rows]


@app.get("/api/agents/{agent_id}")
async def get_agent(agent_id: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM agents WHERE id=$1", agent_id)
    if not row:
        raise HTTPException(404, "Ajan bulunamadı")
    return agent_dict(row)


@app.post("/api/agents", status_code=201)
async def create_agent(agent: AgentCreate):
    data = agent.model_dump()
    validate_agent(data)

    async with db_pool.acquire() as conn:
        try:
            row = await conn.fetchrow("""
                INSERT INTO agents
                (name, role, description, color, shape, symbol,
                 model_provider, model_name, memory_enabled, permissions)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
                RETURNING *
            """,
                data["name"], data["role"], data["description"],
                data["color"], data["shape"], data["symbol"],
                data["model_provider"], data["model_name"],
                data["memory_enabled"], data["permissions"]
            )
        except asyncpg.UniqueViolationError:
            raise HTTPException(409, "Bu ajan adı zaten kullanılıyor")

    return agent_dict(row)


@app.patch("/api/agents/{agent_id}")
async def update_agent(agent_id: int, update: AgentUpdate):
    data = {k: v for k, v in update.model_dump(exclude_unset=True).items()
            if k in AGENT_COLUMNS}
    if not data:
        raise HTTPException(400, "Güncellenecek alan yok")
    validate_agent(data)

    for key in data:
        if key in AGENT_NOT_NULL and data[key] is None:
            raise HTTPException(400, key + " alanı boş olamaz")

    values, assignments = [], []
    for key, value in data.items():
        values.append(value)
        cast = "::jsonb" if key == "permissions" else ""
        assignments.append(f"{key}=${len(values)}{cast}")

    values.append(agent_id)
    sql = (
        "UPDATE agents SET " + ", ".join(assignments) +
        f", updated_at=NOW() WHERE id=${len(values)} RETURNING *"
    )

    async with db_pool.acquire() as conn:
        try:
            row = await conn.fetchrow(sql, *values)
        except asyncpg.UniqueViolationError:
            raise HTTPException(409, "Bu ajan adı zaten kullanılıyor")

    if not row:
        raise HTTPException(404, "Ajan bulunamadı")
    return agent_dict(row)


@app.delete("/api/agents/{agent_id}")
async def pause_agent(agent_id: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE agents SET status='paused', updated_at=NOW()
            WHERE id=$1 RETURNING *
        """, agent_id)
    if not row:
        raise HTTPException(404, "Ajan bulunamadı")
    return {"message": "Ajan pasife alındı", "agent": agent_dict(row)}


# ------------------------------------------------------------------ MODELLER
class ModelKeyInput(BaseModel):
    api_key: str = Field(min_length=8, max_length=500)


class ModelTestInput(BaseModel):
    provider: str
    model_name: str = Field(min_length=2, max_length=120)
    prompt: str = Field(min_length=2, max_length=4000)


class CeoSettingsInput(BaseModel):
    provider: str = ""
    model_name: str = ""


class MissingKeyError(ValueError):
    pass


def _model_cipher():
    digest = hashlib.sha256(
        AUTH_SECRET.encode() + b":cerybro-model-credentials-v1"
    ).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def _cerybro_default_model(provider):
    defaults = {
        "gemini": "gemini-2.5-flash",
        "claude": "claude-sonnet-5-5",
        "openai": "gpt-4.1-mini",
        "grok": "grok-3-mini",
        "openrouter": "openrouter/auto",
        "ollama": "qwen3:1.7b",
    }
    env_name = "CERYBRO_" + provider.upper() + "_MODEL"
    return os.getenv(env_name, defaults.get(provider, ""))


async def _ceo_choice():
    """Kayıtlı CEO model ayarı; yoksa ortam değişkeni / varsayılan."""
    row = await db_pool.fetchrow(
        "SELECT provider, model_name FROM cerybro_ceo_settings WHERE id=1"
    )
    if row:
        return row["provider"], row["model_name"]
    provider = os.getenv("CERYBRO_CEO_PROVIDER", "gemini")
    return provider, _cerybro_default_model(provider)


async def _get_api_key(provider):
    if provider not in MODEL_PROVIDERS:
        raise ValueError("Desteklenmeyen model sağlayıcısı: " + str(provider))
    if provider == "ollama":
        return None
    row = await db_pool.fetchrow(
        "SELECT encrypted_key FROM model_credentials WHERE provider=$1", provider
    )
    if not row:
        raise MissingKeyError(
            provider + " API anahtarı Model Merkezi'nde kayıtlı değil."
        )
    return _model_cipher().decrypt(row["encrypted_key"].encode()).decode()


async def _cerybro_completion(messages, provider=None, model_name=None,
                              max_tokens=1200):
    import litellm

    provider = provider or os.getenv("CERYBRO_CEO_PROVIDER", "gemini")
    api_key = await _get_api_key(provider)
    model_name = model_name or _cerybro_default_model(provider)
    if not model_name:
        raise ValueError("Model adı tanımlanmamış: " + provider)

    kwargs = dict(
        messages=messages,
        max_tokens=max_tokens,
        temperature=0.3,
        timeout=120,
        drop_params=True,  # modelin desteklemediği ayarları sessizce atlar
    )
    if provider == "ollama":
        model = "ollama_chat/" + model_name
        kwargs["api_base"] = OLLAMA_API_BASE
    else:
        model = MODEL_PROVIDERS[provider] + "/" + model_name
        kwargs["api_key"] = api_key

    response = await litellm.acompletion(model=model, **kwargs)
    answer = response.choices[0].message.content
    if not answer:
        raise ValueError("Model boş yanıt döndürdü.")
    return str(answer).strip(), provider, model


cerybro_completion = _cerybro_completion  # diğer modüller bu adı kullanıyor olabilir


@app.get("/api/models/providers")
async def model_provider_status():
    rows = await db_pool.fetch(
        "SELECT provider, updated_at FROM model_credentials ORDER BY provider"
    )
    configured = {r["provider"]: r["updated_at"].isoformat() for r in rows}
    configured["ollama"] = "local (API anahtarı gerekmez)"
    return {
        "supported": list(MODEL_PROVIDERS.keys()),
        "configured": configured,
        "router": "LiteLLM",
    }


@app.put("/api/models/keys/{provider}")
async def save_model_key(provider: str, data: ModelKeyInput):
    if provider not in MODEL_PROVIDERS:
        raise HTTPException(400, "Desteklenmeyen model sağlayıcısı")
    if provider == "ollama":
        return {"status": "saved", "provider": "ollama", "key": "not_required"}

    api_key = data.api_key.strip()  # telefondan yapıştırınca gelen boşluk/satır sonu
    if len(api_key) < 8:
        raise HTTPException(400, "API anahtarı çok kısa")

    encrypted = _model_cipher().encrypt(api_key.encode()).decode()
    await db_pool.execute("""
        INSERT INTO model_credentials(provider, encrypted_key)
        VALUES($1,$2)
        ON CONFLICT(provider) DO UPDATE
        SET encrypted_key=EXCLUDED.encrypted_key, updated_at=NOW()
    """, provider, encrypted)
    return {"status": "saved", "provider": provider, "key": "encrypted"}


@app.delete("/api/models/keys/{provider}")
async def delete_model_key(provider: str):
    if provider not in MODEL_PROVIDERS:
        raise HTTPException(400, "Desteklenmeyen sağlayıcı")
    result = await db_pool.execute(
        "DELETE FROM model_credentials WHERE provider=$1", provider
    )
    return {"status": "deleted", "provider": provider, "result": result}


@app.post("/api/models/test")
async def test_model_connection(data: ModelTestInput):
    if data.provider not in MODEL_PROVIDERS:
        raise HTTPException(400, "Desteklenmeyen sağlayıcı")
    try:
        answer, _, _ = await _cerybro_completion(
            [{"role": "user", "content": data.prompt}],
            provider=data.provider, model_name=data.model_name, max_tokens=120
        )
    except MissingKeyError:
        raise HTTPException(404, "Bu sağlayıcının API anahtarı henüz kaydedilmedi")
    except Exception as exc:
        raise HTTPException(502, "Model bağlantısı başarısız: " + str(exc)[:300])
    return {
        "status": "connected",
        "provider": data.provider,
        "model": data.model_name,
        "response": answer,
    }


@app.get("/api/ceo/settings")
async def get_ceo_settings():
    keys = await db_pool.fetch("SELECT provider FROM model_credentials")
    configured = [r["provider"] for r in keys]
    if "ollama" not in configured:
        configured.append("ollama")

    provider, model_name = await _ceo_choice()
    return {
        "provider": provider,
        "model_name": model_name,
        "configured": configured,
        "supported": list(MODEL_PROVIDERS.keys()),
    }


@app.put("/api/ceo/settings")
async def save_ceo_settings(data: CeoSettingsInput):
    provider = data.provider.strip().lower()
    model_name = data.model_name.strip()

    if provider not in MODEL_PROVIDERS:
        raise HTTPException(400, "Desteklenmeyen model sağlayıcısı.")
    if not model_name or len(model_name) > 120:
        raise HTTPException(400, "Geçerli bir model adı girin.")

    if provider != "ollama":
        key_row = await db_pool.fetchrow(
            "SELECT provider FROM model_credentials WHERE provider=$1", provider
        )
        if not key_row:
            raise HTTPException(
                400, provider + " API anahtarı Model Merkezi'nde kayıtlı değil."
            )

    await db_pool.execute("""
        INSERT INTO cerybro_ceo_settings (id, provider, model_name, updated_at)
        VALUES (1, $1, $2, NOW())
        ON CONFLICT (id) DO UPDATE SET
            provider=EXCLUDED.provider,
            model_name=EXCLUDED.model_name,
            updated_at=NOW()
    """, provider, model_name)

    return {"status": "saved", "provider": provider, "model_name": model_name}


# ------------------------------------------------------------------ PROFİL (İK)
class ProfileInput(BaseModel):
    full_name: str = Field(default="", max_length=120)
    title: str = Field(default="", max_length=120)
    email: str = Field(default="", max_length=160)
    phone: str = Field(default="", max_length=40)
    language: str = Field(default="tr", max_length=8)
    notes: str = Field(default="", max_length=2000)


async def _ensure_profile_table():
    await db_pool.execute(
        "CREATE TABLE IF NOT EXISTS cerybro_profile (id INT PRIMARY KEY, data JSONB NOT NULL DEFAULT '{}'::jsonb, updated_at TIMESTAMPTZ DEFAULT NOW())"
    )


@app.get("/api/profile")
async def get_profile():
    await _ensure_profile_table()
    row = await db_pool.fetchrow("SELECT data FROM cerybro_profile WHERE id=1")
    data = row["data"] if row else {}
    if isinstance(data, str):
        data = json.loads(data or "{}")
    return {**ProfileInput().model_dump(), **(data or {})}


@app.put("/api/profile")
async def save_profile(data: ProfileInput):
    await _ensure_profile_table()
    payload = {k: (v.strip() if isinstance(v, str) else v) for k, v in data.model_dump().items()}
    await db_pool.execute(
        "INSERT INTO cerybro_profile (id, data, updated_at) VALUES (1, $1::jsonb, NOW()) "
        "ON CONFLICT (id) DO UPDATE SET data=EXCLUDED.data, updated_at=NOW()",
        json.dumps(payload),
    )
    return {"status": "saved", **payload}


# --------------------------------------------------------------- CEO SOHBETİ
class ChatInput(BaseModel):
    message: str = ""
    conversation_id: str = "cerybro-ceo"


async def _agent_roster():
    rows = await db_pool.fetch(
        "SELECT name, role, description FROM agents WHERE status='active' ORDER BY id"
    )
    if not rows:
        return "Aktif ajan yok."
    return "\n".join(
        "- " + r["name"] + " (" + r["role"] + "): " + r["description"] for r in rows
    )


@app.post("/api/chat")
async def cerybro_chat(data: ChatInput):
    message = data.message.strip()[:6000]
    conversation_id = (data.conversation_id or "cerybro-ceo")[:100]

    if not message:
        raise HTTPException(400, "Mesaj boş olamaz.")

    try:
        provider, model_name = await _ceo_choice()
        rows = await db_pool.fetch("""
            SELECT role, content FROM (
                SELECT id, role, content FROM cerybro_chat_messages
                WHERE conversation_id=$1 ORDER BY id DESC LIMIT 30
            ) recent ORDER BY id ASC
        """, conversation_id)

        history = [{"role": r["role"], "content": r["content"]} for r in rows]
        history.append({"role": "user", "content": message})

        system_prompt = {
            "role": "system",
            "content": (
                "Sen Cerybro, kullanıcının kişisel AI CEO'susun. "
                "Türkçe iletişim kur. Hedefleri analiz et, işleri planla ve "
                "görevleri uygun ajanlara vermeyi öner. "
                "Bu sohbette görev oluşturma, ajan çalıştırma veya dış "
                "sistemlere erişme yetkin yok; yapmadığın bir işi yapılmış "
                "gibi gösterme. Önerilerini 'şu görevi şu ajana ver' gibi "
                "açık adımlarla sun. Harcama, dış sistemlerde geri alınamaz "
                "işlem ve yayınlama için onay iste.\n\n"
                "Aktif ajanlar:\n" + await _agent_roster()
            ),
        }

        answer, used_provider, used_model = await _cerybro_completion(
            [system_prompt] + history,
            provider=provider, model_name=model_name, max_tokens=1200
        )

        # İki mesaj birlikte kaydedilir; model hata verirse geçmiş kirlenmez
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("""
                    INSERT INTO cerybro_chat_messages (conversation_id, role, content)
                    VALUES ($1, 'user', $2)
                """, conversation_id, message)
                await conn.execute("""
                    INSERT INTO cerybro_chat_messages (conversation_id, role, content)
                    VALUES ($1, 'assistant', $2)
                """, conversation_id, answer)

        return {
            "reply": answer,
            "response": answer,
            "provider": used_provider,
            "model": used_model,
        }

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, "CEO model yanıtı alınamadı: " + str(exc)[:300])


@app.get("/api/chat/history")
async def cerybro_chat_history(conversation_id: str = "cerybro-ceo"):
    rows = await db_pool.fetch("""
        SELECT role, content, created_at FROM cerybro_chat_messages
        WHERE conversation_id=$1 ORDER BY id DESC LIMIT 50
    """, conversation_id[:100])
    return {"messages": [
        {"role": r["role"], "content": r["content"],
         "created_at": r["created_at"].isoformat()}
        for r in reversed(rows)
    ]}


# ------------------------------------------------------------------ GÖREVLER
def _priority_value(value):
    if isinstance(value, str):
        value = value.strip().lower()
        value = int(value) if value.isdigit() else PRIORITY_NAMES.get(value)
    if not isinstance(value, int) or not 1 <= value <= 5:
        raise HTTPException(400, "Geçersiz öncelik (1-5 veya low/normal/high/urgent)")
    return value


class TaskCreateInput(BaseModel):
    title: str = Field(min_length=2, max_length=180)
    description: str = Field(default="", max_length=12000)
    priority: Union[int, str] = 3
    assigned_agent_id: Optional[int] = None
    agent_id: Optional[int] = None  # eski kısa ad da kabul edilir
    payload: dict = Field(default_factory=dict)


class TaskAssignInput(BaseModel):
    agent_id: int


class TaskCompleteInput(BaseModel):
    result: Union[str, dict] = ""


class TaskUpdateInput(BaseModel):
    status: str
    result: Optional[dict] = None
    error: Optional[str] = Field(default=None, max_length=12000)


async def _require_active_agent(conn, agent_id):
    agent = await conn.fetchrow(
        "SELECT id FROM agents WHERE id=$1 AND status='active'", agent_id
    )
    if not agent:
        raise HTTPException(404, "Aktif ajan bulunamadı")


@app.post("/api/tasks", status_code=201)
async def create_task(data: TaskCreateInput):
    priority = _priority_value(data.priority)
    agent_id = data.assigned_agent_id if data.assigned_agent_id is not None else data.agent_id
    async with db_pool.acquire() as conn:
        if agent_id is not None:
            await _require_active_agent(conn, agent_id)
        row = await conn.fetchrow("""
            INSERT INTO cerybro_tasks
            (title, description, priority, assigned_agent_id, payload, status)
            VALUES ($1,$2,$3,$4,$5::jsonb,$6) RETURNING *
        """, data.title, data.description, priority, agent_id, data.payload,
             "assigned" if agent_id is not None else "queued")
    return dict(row)


@app.get("/api/tasks")
async def list_tasks(status: Optional[str] = None,
                     agent_id: Optional[int] = None,
                     limit: int = Query(default=50, ge=1, le=200)):
    conditions, values = [], []
    if status:
        values.append(status)
        conditions.append(f"status=${len(values)}")
    if agent_id is not None:
        values.append(agent_id)
        conditions.append(f"assigned_agent_id=${len(values)}")
    values.append(limit)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    rows = await db_pool.fetch(
        f"SELECT * FROM cerybro_tasks {where} "
        f"ORDER BY priority DESC, created_at ASC LIMIT ${len(values)}",
        *values
    )
    return [dict(row) for row in rows]


@app.get("/api/tasks/{task_id}")
async def get_task(task_id: int):
    row = await db_pool.fetchrow("SELECT * FROM cerybro_tasks WHERE id=$1", task_id)
    if not row:
        raise HTTPException(404, "Görev bulunamadı")
    return dict(row)


@app.post("/api/tasks/{task_id}/assign")
async def assign_task(task_id: int, agent_id: Optional[int] = None,
                      body: Optional[TaskAssignInput] = None):
    """agent_id hem adres sonunda (?agent_id=3) hem de JSON gövdesinde kabul edilir."""
    if agent_id is None and body is not None:
        agent_id = body.agent_id
    if agent_id is None:
        raise HTTPException(400, "agent_id gerekli")

    async with db_pool.acquire() as conn:
        await _require_active_agent(conn, agent_id)
        row = await conn.fetchrow("""
            UPDATE cerybro_tasks
            SET assigned_agent_id=$2, status='assigned', error=NULL, updated_at=NOW()
            WHERE id=$1 AND status IN ('queued','assigned','failed')
            RETURNING *
        """, task_id, agent_id)
    if not row:
        raise HTTPException(409, "Görev bulunamadı veya atanabilir durumda değil")
    return dict(row)


@app.post("/api/tasks/{task_id}/claim")
async def claim_task(task_id: int):
    row = await db_pool.fetchrow("""
        UPDATE cerybro_tasks
        SET status='running', started_at=COALESCE(started_at, NOW()), updated_at=NOW()
        WHERE id=$1 AND status='assigned'
        RETURNING *
    """, task_id)
    if not row:
        raise HTTPException(409, "Görev atanmış değil veya başlatılamıyor")
    return dict(row)


@app.post("/api/tasks/{task_id}/complete")
async def complete_task(task_id: int, data: TaskCompleteInput):
    result = data.result if isinstance(data.result, dict) else {"output": data.result}
    row = await db_pool.fetchrow("""
        UPDATE cerybro_tasks
        SET status='completed', result=$2::jsonb, error=NULL,
            completed_at=NOW(), updated_at=NOW()
        WHERE id=$1 AND status='running'
        RETURNING *
    """, task_id, result)
    if not row:
        raise HTTPException(409, "Çalışan görev bulunamadı")
    return dict(row)


@app.patch("/api/tasks/{task_id}")
async def update_task(task_id: int, data: TaskUpdateInput):
    if data.status not in TASK_STATUSES:
        raise HTTPException(400, "Geçersiz görev durumu")
    row = await db_pool.fetchrow("""
        UPDATE cerybro_tasks SET
            status=$2::varchar,
            result=COALESCE($3::jsonb, result),
            error=$4,
            started_at=CASE WHEN $2::varchar='running' AND started_at IS NULL
                            THEN NOW() ELSE started_at END,
            completed_at=CASE WHEN $2::varchar IN ('completed','failed','cancelled')
                              THEN NOW() ELSE NULL END,
            updated_at=NOW()
        WHERE id=$1 RETURNING *
    """, task_id, data.status, data.result, data.error)
    if not row:
        raise HTTPException(404, "Görev bulunamadı")
    return dict(row)


@app.post("/api/tasks/{task_id}/execute")
async def cerybro_execute_task(task_id: int):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            task = await conn.fetchrow(
                "SELECT * FROM cerybro_tasks WHERE id=$1 FOR UPDATE", task_id
            )
            if not task:
                raise HTTPException(404, "Görev bulunamadı")
            if task["status"] not in ("queued", "assigned", "failed"):
                raise HTTPException(409, "Görev çalıştırılabilir durumda değil")

            agent_id = task["assigned_agent_id"]
            if not agent_id:
                raise HTTPException(409, "Göreve önce bir aktif ajan atanmalı")

            agent = await conn.fetchrow(
                "SELECT * FROM agents WHERE id=$1 AND status='active'", agent_id
            )
            if not agent:
                raise HTTPException(404, "Aktif ajan bulunamadı")

            await conn.execute("""
                UPDATE cerybro_tasks
                SET status='running', started_at=NOW(), completed_at=NULL,
                    updated_at=NOW(), error=NULL
                WHERE id=$1
            """, task_id)

    try:
        # Ajanın kendi modeli varsa o, yoksa CEO için seçilen model kullanılır
        if agent["model_provider"]:
            provider = agent["model_provider"]
            model_name = agent["model_name"] or None
        else:
            provider, model_name = await _ceo_choice()

        payload = task["payload"]
        if not isinstance(payload, str):
            payload = json.dumps(payload, ensure_ascii=False)

        prompt = (
            "Görevin: " + str(task["title"]) + "\n\n"
            "Açıklama:\n" + str(task["description"]) + "\n\n"
            "Ek veriler:\n" + payload + "\n\n"
            "Görevi analiz et ve uygulanabilir, açık bir sonuç üret. "
            "Gerçekleştirmediğin harici işlemleri gerçekleştirmiş gibi "
            "gösterme. Araç erişimin yoksa bunu açıkça belirt."
        )

        answer, used_provider, used_model = await _cerybro_completion(
            [
                {
                    "role": "system",
                    "content": (
                        "Sen Cerybro bünyesinde çalışan uzman bir ajansın. "
                        "Türkçe, somut ve sonuç odaklı çalış."
                        "\nAjan: " + str(agent["name"]) +
                        "\nRol: " + str(agent["role"]) +
                        "\nUzmanlık: " + str(agent["description"])
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            provider=provider,
            model_name=model_name,
            max_tokens=1800,
        )

        result = {
            "output": answer,
            "provider": used_provider,
            "model": used_model,
            "agent_id": agent_id,
        }

        await db_pool.execute("""
            UPDATE cerybro_tasks
            SET status='completed', result=$2::jsonb, error=NULL,
                completed_at=NOW(), updated_at=NOW()
            WHERE id=$1 AND status='running'
        """, task_id, result)

        return {"status": "completed", "task_id": task_id, "result": result}

    except Exception as exc:
        await db_pool.execute("""
            UPDATE cerybro_tasks
            SET status='failed', error=$2, completed_at=NOW(), updated_at=NOW()
            WHERE id=$1 AND status='running'
        """, task_id, str(exc)[:12000])
        raise HTTPException(502, "Görev çalıştırma hatası: " + str(exc)[:300])
