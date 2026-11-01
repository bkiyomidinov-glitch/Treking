from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ValidationError


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "supply_chain_tj.sqlite3"
STATIC_DIR = BASE_DIR / "static"
SCHEMA_VERSION = 1
STAGE_ORDER = {"factory": 0, "workshop": 1, "warehouse": 2, "transport": 3}
STAGE_TITLES = {
    "factory": "Корхона",
    "workshop": "Сех ва назорати дохилӣ",
    "warehouse": "Анбор ва феҳрист",
    "transport": "Нақлиёт",
}


@contextmanager
def connect() -> Generator[sqlite3.Connection, None, None]:
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def insert_event(
    db: sqlite3.Connection,
    product_id: int,
    stage: str,
    location_id: int,
    responsible_id: int,
    operator_id: int,
    metadata: dict[str, Any],
) -> sqlite3.Row:
    previous = db.execute(
        "SELECT event_hash FROM trace_events WHERE product_id=? ORDER BY sequence DESC LIMIT 1",
        (product_id,),
    ).fetchone()
    sequence = db.execute(
        "SELECT COALESCE(MAX(sequence), 0) + 1 FROM trace_events WHERE product_id=?",
        (product_id,),
    ).fetchone()[0]
    timestamp = now()
    previous_hash = previous["event_hash"] if previous else "0" * 64
    entry = {
        "product_id": product_id,
        "sequence": sequence,
        "stage": stage,
        "location_id": location_id,
        "responsible_id": responsible_id,
        "operator_id": operator_id,
        "timestamp": timestamp,
        "metadata": metadata,
        "previous_hash": previous_hash,
    }
    event_hash = hashlib.sha256(canonical_json(entry).encode("utf-8")).hexdigest()
    cursor = db.execute(
        "INSERT INTO trace_events(product_id,sequence,stage,location_id,responsible_id,operator_id,timestamp,metadata,previous_hash,event_hash,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (
            product_id,
            sequence,
            stage,
            location_id,
            responsible_id,
            operator_id,
            timestamp,
            canonical_json(metadata),
            previous_hash,
            event_hash,
            SCHEMA_VERSION,
        ),
    )
    return db.execute("SELECT * FROM trace_events WHERE id=?", (cursor.lastrowid,)).fetchone()


def add_revision(
    db: sqlite3.Connection,
    product_id: int,
    config: dict[str, Any],
    changed_by: int,
    note: str,
) -> int:
    version = db.execute(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM product_revisions WHERE product_id=?",
        (product_id,),
    ).fetchone()[0]
    db.execute(
        "INSERT INTO product_revisions(product_id,version,config,changed_by,note,created_at,schema_version) VALUES(?,?,?,?,?,?,?)",
        (product_id, version, canonical_json(config), changed_by, note, now(), SCHEMA_VERSION),
    )
    return version


def seed_database(db: sqlite3.Connection) -> None:
    locations = [
        ("Корхонаи Душанбе", "factory", "Душанбе", "хиёбони Рӯдакӣ, 118", "Душанбе"),
        ("Корхонаи Суғд", "factory", "Суғд", "кӯчаи Саноат, 24", "Хуҷанд"),
        ("Корхонаи Хатлон", "factory", "Хатлон", "хиёбони Исмоили Сомонӣ, 37", "Бохтар"),
        ("Анбори марказии Душанбе", "warehouse", "Душанбе", "ноҳияи Сино, кӯчаи Борбад, 42", "Душанбе"),
        ("Маркази тақсимоти Суғд", "warehouse", "Суғд", "шаҳраки Ғафуров, роҳи саноатӣ, 9", "Бӯстон"),
        ("Анбори Хатлон", "warehouse", "Хатлон", "кӯчаи Ҷалолиддини Балхӣ, 16", "Бохтар"),
        ("Анбори Помир", "warehouse", "ВМКБ", "хиёбони Шоҳтемур, 5", "Хоруғ"),
    ]
    db.executemany(
        "INSERT INTO locations(name,kind,region,address,city) VALUES(?,?,?,?,?)",
        locations,
    )

    staff = [
        ("Фарҳод Каримов", "Роҳбари корхона", "TJ-001", "+992 93 700 10 01"),
        ("Меҳрона Саидова", "Устои сех", "TJ-002", "+992 93 700 10 02"),
        ("Комрон Назаров", "Анбордор", "TJ-003", "+992 93 700 10 03"),
        ("Шаҳноза Раҳмонӣ", "Қабулкунандаи бор", "TJ-004", "+992 93 700 10 04"),
        ("Ҷамшед Раҳимов", "Ронанда", "TJ-005", "+992 93 700 10 05"),
        ("Манижа Ҳакимова", "Ронанда", "TJ-006", "+992 93 700 10 06"),
        ("Сомон Давлатов", "Логист", "TJ-007", "+992 93 700 10 07"),
        ("Зебо Нурова", "Оператори низом", "TJ-008", "+992 93 700 10 08"),
    ]
    db.executemany(
        "INSERT INTO staff(name,role,employee_no,phone) VALUES(?,?,?,?)",
        staff,
    )

    vehicles = [
        ("01TJ101AA", "Isuzu NPR", "Ҷамшед Раҳимов", "+992 93 700 10 05", 8, 1),
        ("02TJ202BB", "Hyundai Mighty", "Манижа Ҳакимова", "+992 93 700 10 06", 5, 1),
        ("03TJ303CC", "Mercedes-Benz Atego", "Саидҷон Холиқов", "+992 93 700 10 09", 12, 0),
    ]
    db.executemany(
        "INSERT INTO vehicles(plate,brand,driver,driver_phone,capacity,refrigerated) VALUES(?,?,?,?,?,?)",
        vehicles,
    )

    routes = [
        ("Душанбе — Хуҷанд", "Душанбе", "Хуҷанд", 305, 6),
        ("Душанбе — Бохтар", "Душанбе", "Бохтар", 100, 2),
        ("Душанбе — Хоруғ", "Душанбе", "Хоруғ", 525, 13),
    ]
    db.executemany(
        "INSERT INTO routes(name,origin,destination,distance_km,duration_hours) VALUES(?,?,?,?,?)",
        routes,
    )

    demo_products = [
        {
            "barcode": "4850000001001", "batch_id": "TJ-26-101", "name": "Маркер «Помир»",
            "config": {"type": "marker", "color": "#729c82", "capColor": "#d2a35e", "length": 12, "diameter": 1.8, "shape": "faceted"},
            "weight": 0.18, "qc_status": "Санҷида шуд", "note": "Нӯги борик, бастаи навбати якум", "stage": "factory",
        },
        {
            "barcode": "4850000001002", "batch_id": "TJ-26-102", "name": "Маркер «Суғд»",
            "config": {"type": "marker", "color": "#6e9eb5", "capColor": "#d66e62", "length": 13, "diameter": 2.0, "shape": "round"},
            "weight": 0.22, "qc_status": "Санҷида шуд", "note": "Сиёҳии кабуд, навбати дуюм", "stage": "workshop",
        },
        {
            "barcode": "4850000001003", "batch_id": "TJ-26-103", "name": "Маркер «Вахш»",
            "config": {"type": "marker", "color": "#d66e62", "capColor": "#e4c66d", "length": 11, "diameter": 1.6, "shape": "faceted"},
            "weight": 0.2, "qc_status": "Санҷида шуд", "note": "Маркер барои хаткашӣ", "stage": "warehouse",
        },
        {
            "barcode": "4850000001004", "batch_id": "TJ-26-104", "name": "Маркер «Хоруғ»",
            "config": {"type": "marker", "color": "#d5a74e", "capColor": "#769b83", "length": 14, "diameter": 2.2, "shape": "round"},
            "weight": 0.25, "qc_status": "Санҷида шуд", "note": "Бастаи махсус барои маркази Помир", "stage": "transport",
        },
    ]
    for index, item in enumerate(demo_products):
        cursor = db.execute(
            "INSERT INTO products(barcode,batch_id,name,config,config_version,weight,qc_status,note,current_stage,revision,created_at,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                item["barcode"], item["batch_id"], item["name"], canonical_json(item["config"]), 1,
                item["weight"], item["qc_status"], item["note"], "factory", 1, now(), SCHEMA_VERSION,
            ),
        )
        product_id = cursor.lastrowid
        factory_id = index % 3 + 1
        add_revision(db, product_id, item["config"], 1, "Танзимоти ибтидоии корхона")
        insert_event(db, product_id, "factory", factory_id, 1, 8, {"batch_weight_kg": item["weight"], "quality": item["qc_status"], "note": item["note"]})

        if STAGE_ORDER[item["stage"]] >= STAGE_ORDER["workshop"]:
            insert_event(db, product_id, "workshop", factory_id, 2, 8, {"inspection": "Мувофиқи талабот", "line": f"Хати истеҳсолии {index + 1}", "adjustment": "Сифати сарпӯш санҷида шуд"})
        if STAGE_ORDER[item["stage"]] >= STAGE_ORDER["warehouse"]:
            warehouse_id = 4 + index % 4
            insert_event(db, product_id, "warehouse", warehouse_id, 3, 4, {"shelf": f"A-{10 + index}", "sku": f"TJ-MRK-{index + 101}", "price_tjs": 18 + index * 2, "temperature_c": 21, "package": "Солим"})
        if item["stage"] == "transport":
            insert_event(db, product_id, "transport", 7, 7, 8, {"vehicle": "01TJ101AA", "driver": "Ҷамшед Раҳимов", "driver_phone": "+992 93 700 10 05", "route": "Душанбе — Хоруғ", "distance_km": 525, "eta_hours": 13, "temperature_c": 17, "seal": "TJ-PL-104", "cargo_state": "Бе осеб"})
        db.execute("UPDATE products SET current_stage=? WHERE id=?", (item["stage"], product_id))


def initialize_database() -> None:
    with connect() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
                kind TEXT NOT NULL, region TEXT NOT NULL, address TEXT NOT NULL,
                city TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS staff (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
                role TEXT NOT NULL, employee_no TEXT NOT NULL, phone TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS vehicles (
                id INTEGER PRIMARY KEY AUTOINCREMENT, plate TEXT NOT NULL,
                brand TEXT NOT NULL, driver TEXT NOT NULL, driver_phone TEXT NOT NULL,
                capacity REAL NOT NULL, refrigerated INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS routes (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
                origin TEXT NOT NULL, destination TEXT NOT NULL,
                distance_km REAL NOT NULL, duration_hours REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT, barcode TEXT UNIQUE NOT NULL,
                batch_id TEXT NOT NULL, name TEXT NOT NULL, config TEXT NOT NULL,
                config_version INTEGER NOT NULL, weight REAL NOT NULL,
                qc_status TEXT NOT NULL, note TEXT NOT NULL,
                current_stage TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL, schema_version INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS product_revisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, product_id INTEGER NOT NULL,
                version INTEGER NOT NULL, config TEXT NOT NULL, changed_by INTEGER NOT NULL,
                note TEXT NOT NULL, created_at TEXT NOT NULL, schema_version INTEGER NOT NULL,
                UNIQUE(product_id, version),
                FOREIGN KEY(product_id) REFERENCES products(id),
                FOREIGN KEY(changed_by) REFERENCES staff(id)
            );
            CREATE TABLE IF NOT EXISTS trace_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, product_id INTEGER NOT NULL,
                sequence INTEGER NOT NULL, stage TEXT NOT NULL, location_id INTEGER NOT NULL,
                responsible_id INTEGER NOT NULL, operator_id INTEGER NOT NULL,
                timestamp TEXT NOT NULL, metadata TEXT NOT NULL,
                previous_hash TEXT NOT NULL, event_hash TEXT NOT NULL,
                schema_version INTEGER NOT NULL, UNIQUE(product_id, sequence),
                FOREIGN KEY(product_id) REFERENCES products(id),
                FOREIGN KEY(location_id) REFERENCES locations(id),
                FOREIGN KEY(responsible_id) REFERENCES staff(id),
                FOREIGN KEY(operator_id) REFERENCES staff(id)
            );
            CREATE TRIGGER IF NOT EXISTS trace_events_no_update
            BEFORE UPDATE ON trace_events BEGIN
                SELECT RAISE(ABORT, 'Рӯйдоди аудитро тағйир додан мумкин нест');
            END;
            CREATE TRIGGER IF NOT EXISTS trace_events_no_delete
            BEFORE DELETE ON trace_events BEGIN
                SELECT RAISE(ABORT, 'Рӯйдоди аудитро нест кардан мумкин нест');
            END;
            CREATE TRIGGER IF NOT EXISTS product_revisions_no_update
            BEFORE UPDATE ON product_revisions BEGIN
                SELECT RAISE(ABORT, 'Версияи маҳсулотро тағйир додан мумкин нест');
            END;
            CREATE TRIGGER IF NOT EXISTS product_revisions_no_delete
            BEFORE DELETE ON product_revisions BEGIN
                SELECT RAISE(ABORT, 'Версияи маҳсулотро нест кардан мумкин нест');
            END;
            """
        )
        if not db.execute("SELECT 1 FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)).fetchone():
            if not db.execute("SELECT COUNT(*) FROM locations").fetchone()[0]:
                seed_database(db)
            db.execute("INSERT INTO schema_migrations(version,applied_at) VALUES(?,?)", (SCHEMA_VERSION, now()))


@asynccontextmanager
async def lifespan(_: FastAPI):
    initialize_database()
    yield


app = FastAPI(title="Supply Tail Tajik", version="1.0.0", lifespan=lifespan)


class LocationInput(BaseModel):
    name: str = Field(min_length=2)
    kind: str
    region: str
    address: str
    city: str


class StaffInput(BaseModel):
    name: str = Field(min_length=2)
    role: str
    employee_no: str
    phone: str


class VehicleInput(BaseModel):
    plate: str
    brand: str
    driver: str
    driver_phone: str
    capacity: float = Field(gt=0)
    refrigerated: bool = False


class RouteInput(BaseModel):
    name: str
    origin: str
    destination: str
    distance_km: float = Field(gt=0)
    duration_hours: float = Field(gt=0)


class ProductInput(BaseModel):
    barcode: str = Field(min_length=8, max_length=32)
    batch_id: str = Field(min_length=2)
    name: str = Field(min_length=2)
    config: dict[str, Any]
    weight: float = Field(gt=0)
    qc_status: str
    note: str = ""
    factory_id: int
    responsible_id: int
    operator_id: int


class EventInput(BaseModel):
    stage: str
    location_id: int
    responsible_id: int
    operator_id: int
    metadata: dict[str, Any] = Field(default_factory=dict)
    config_revision: dict[str, Any] | None = None
    revision_note: str = ""


RESOURCE_COLUMNS = {
    "locations": "id,name,kind,region,address,city",
    "staff": "id,name,role,employee_no,phone",
    "vehicles": "id,plate,brand,driver,driver_phone,capacity,refrigerated",
    "routes": "id,name,origin,destination,distance_km,duration_hours",
}
RESOURCE_INPUTS = {"locations": LocationInput, "staff": StaffInput, "vehicles": VehicleInput, "routes": RouteInput}


def parse_product(row: sqlite3.Row) -> dict[str, Any]:
    product = dict(row)
    product["config"] = json.loads(product["config"])
    return product


def validate_resource(resource: str) -> None:
    if resource not in RESOURCE_COLUMNS:
        raise HTTPException(404, "Маълумотнома ёфт нашуд")


@app.get("/api/bootstrap")
def bootstrap() -> dict[str, Any]:
    with connect() as db:
        result = {key: [dict(row) for row in db.execute(f"SELECT {columns} FROM {key} ORDER BY id")] for key, columns in RESOURCE_COLUMNS.items()}
        result["products"] = [parse_product(row) for row in db.execute("SELECT * FROM products ORDER BY id DESC")]
        result["schema_version"] = SCHEMA_VERSION
        result["schema_history"] = [dict(row) for row in db.execute("SELECT * FROM schema_migrations ORDER BY version")]
        return result


@app.get("/api/mdm/{resource}")
def list_resource(resource: str) -> list[dict[str, Any]]:
    validate_resource(resource)
    with connect() as db:
        return [dict(row) for row in db.execute(f"SELECT {RESOURCE_COLUMNS[resource]} FROM {resource} ORDER BY id")]


@app.post("/api/mdm/{resource}", status_code=201)
def create_resource(resource: str, payload: dict[str, Any]) -> dict[str, Any]:
    validate_resource(resource)
    try:
        values = RESOURCE_INPUTS[resource].model_validate(payload).model_dump()
    except ValidationError as error:
        raise HTTPException(422, "Маълумоти воридшуда нодуруст аст") from error
    if resource == "vehicles":
        values["refrigerated"] = int(values["refrigerated"])
    columns = list(values)
    with connect() as db:
        cursor = db.execute(
            f"INSERT INTO {resource}({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
            [values[column] for column in columns],
        )
        row = db.execute(f"SELECT {RESOURCE_COLUMNS[resource]} FROM {resource} WHERE id=?", (cursor.lastrowid,)).fetchone()
        return dict(row)


@app.get("/api/products")
def list_products() -> list[dict[str, Any]]:
    with connect() as db:
        return [parse_product(row) for row in db.execute("SELECT * FROM products ORDER BY id DESC")]


@app.post("/api/products", status_code=201)
def create_product(payload: ProductInput) -> dict[str, Any]:
    with connect() as db:
        if db.execute("SELECT 1 FROM products WHERE barcode=?", (payload.barcode,)).fetchone():
            raise HTTPException(409, "Маҳсулот бо ин рақами штрихкод аллакай сабт шудааст")
        cursor = db.execute(
            "INSERT INTO products(barcode,batch_id,name,config,config_version,weight,qc_status,note,current_stage,revision,created_at,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (payload.barcode, payload.batch_id, payload.name, canonical_json(payload.config), 1, payload.weight, payload.qc_status, payload.note, "factory", 1, now(), SCHEMA_VERSION),
        )
        product_id = cursor.lastrowid
        add_revision(db, product_id, payload.config, payload.operator_id, "Танзимоти ибтидоии корхона")
        insert_event(db, product_id, "factory", payload.factory_id, payload.responsible_id, payload.operator_id, {"weight_kg": payload.weight, "quality": payload.qc_status, "note": payload.note})
        return parse_product(db.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone())


@app.get("/api/products/{barcode}")
def get_product(barcode: str) -> dict[str, Any]:
    with connect() as db:
        row = db.execute("SELECT * FROM products WHERE barcode=?", (barcode,)).fetchone()
        if not row:
            raise HTTPException(404, "Маҳсулот ёфт нашуд")
        product = parse_product(row)
        events = db.execute(
            "SELECT e.*,l.name AS location_name,l.region,l.city,s.name AS responsible_name,s.role AS responsible_role,o.name AS operator_name FROM trace_events e JOIN locations l ON l.id=e.location_id JOIN staff s ON s.id=e.responsible_id JOIN staff o ON o.id=e.operator_id WHERE e.product_id=? ORDER BY e.sequence",
            (row["id"],),
        )
        product["events"] = [{**dict(event), "metadata": json.loads(event["metadata"])} for event in events]
        revisions = db.execute(
            "SELECT r.version,r.config,r.note,r.created_at,s.name AS changed_by FROM product_revisions r JOIN staff s ON s.id=r.changed_by WHERE r.product_id=? ORDER BY r.version",
            (row["id"],),
        )
        product["revisions"] = [{**dict(revision), "config": json.loads(revision["config"])} for revision in revisions]
        return product


@app.post("/api/products/{barcode}/events", status_code=201)
def add_event(barcode: str, payload: EventInput) -> dict[str, Any]:
    if payload.stage not in STAGE_ORDER or payload.stage == "factory":
        raise HTTPException(422, "Марҳилаи интихобшуда нодуруст аст")
    with connect() as db:
        product = db.execute("SELECT * FROM products WHERE barcode=?", (barcode,)).fetchone()
        if not product:
            raise HTTPException(404, "Маҳсулот ёфт нашуд")
        expected = STAGE_ORDER[product["current_stage"]] + 1
        if STAGE_ORDER[payload.stage] != expected:
            raise HTTPException(409, f"Гузариш ба марҳилаи «{STAGE_TITLES[payload.stage]}» иҷозат нест. Марҳилаи ҷорӣ: «{STAGE_TITLES[product['current_stage']]}».")
        config_version = product["config_version"]
        if payload.config_revision:
            if payload.stage != "workshop":
                raise HTTPException(422, "Танҳо дар сех версияи нави танзимот сабт мешавад")
            config_version = add_revision(db, product["id"], payload.config_revision, payload.responsible_id, payload.revision_note or "Тағйироти марҳилаи сех")
            db.execute("UPDATE products SET config_version=?,revision=revision+1 WHERE id=?", (config_version, product["id"]))
        event = insert_event(db, product["id"], payload.stage, payload.location_id, payload.responsible_id, payload.operator_id, {**payload.metadata, "config_version": config_version})
        db.execute("UPDATE products SET current_stage=?,revision=revision+1 WHERE id=?", (payload.stage, product["id"]))
        return dict(event)


@app.get("/api/health")
def health() -> dict[str, Any]:
    with connect() as db:
        products = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        events = db.execute("SELECT COUNT(*) FROM trace_events").fetchone()[0]
    return {"status": "ok", "database": DB_PATH.name, "products": products, "events": events, "schema_version": SCHEMA_VERSION}


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")