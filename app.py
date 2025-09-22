import os
import re
import sqlite3
import logging
import shutil
from datetime import datetime
from flask import Flask, request, jsonify, send_file, abort, render_template, g
from werkzeug.utils import secure_filename
from flask_cors import CORS
import requests


UPLOAD_FOLDER = "uploads"
DB_FILE = "data.db"
ALLOWED_EXTENSIONS = {"prn"}
ALLOWED_STAGES = {"Raw", "SFG", "FG"}
MAX_CONTENT_LENGTH = 5 * 1024 * 1024


LABELARY_PRINTER = "8dpmm"
LABELARY_LABEL = "4x6"
LABELARY_ROTATION = "0"
LABELARY_URL = f"http://api.labelary.com/v1/printers/{LABELARY_PRINTER}/labels/{LABELARY_LABEL}/{LABELARY_ROTATION}/"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app = Flask(__name__, static_folder="static", template_folder="templates")
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH
CORS(app)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("prn-manager")


def jsonify_error(msg, code=400, details=None):
    payload = {"error": msg}
    if details is not None:
        payload["details"] = str(details)
    response = jsonify(payload)
    response.status_code = code
    return response


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


# def change(product_name: str, stage: str, filename: str):
#     safe_product = normalize_product_folder_name(product_name)
#     safe_stage = stage_folder_name(stage)
#     if not safe_stage:
#         return None
#     safe_filename = secure_filename(filename)
#     return os.path.join(app.config["UPLOAD_FOLDER"], safe_product, safe_stage, safe_filename)

def stage_folder_name(stage_raw: str):
    if not stage_raw:
        return None
    s = stage_raw.strip().lower()
    if s in ("raw", "rawmaterial", "raw_material", "raw material"):
        return "Raw"
    if s in ("sfg", "semi", "semi_finished", "semi_finished_good", "semi finished", "semi-finished", "semifinished"):
        return "SFG"
    if s in ("fg", "finished", "finished_good", "finished good", "finished-good"):
        return "FG"
    # allow canonical forms as well
    if stage_raw in ALLOWED_STAGES:
        return stage_raw
    return None


def extract_prn_columns(file_path: str):
    """Return sorted unique {PLACEHOLDER} fields found in a PRN file."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()
    except Exception:
        raise
    matches = re.findall(r"\{[A-Za-z0-9_]+\}", text)
    unique_fields = sorted(set(m.strip("{}") for m in matches))
    return unique_fields


def normalize_product_folder_name(product_name: str) -> str:
    if not product_name:
        return ""
    safe = secure_filename(product_name)
    if safe:
        return safe
    return re.sub(r"[^\w\-\_\. ]", "_", product_name).strip()


def generate_preview(prn_path: str, out_path: str) -> bool:
    """
    Generate PNG preview using Labelary API. Returns True on success.
    This is best-effort (won't raise) and will return False on failure.
    """
    try:
        with open(prn_path, "rb") as f:
            files = {"file": f}
            resp = requests.post(LABELARY_URL, files=files, stream=True, timeout=30)
            if resp.status_code == 200:
                resp.raw.decode_content = True
                with open(out_path, "wb") as out_file:
                    shutil.copyfileobj(resp.raw, out_file)
                return True
            else:
                logger.warning("Labelary preview failed: %s %s", resp.status_code, resp.text[:200])
                return False
    except Exception as e:
        logger.exception("Exception while generating label preview: %s", e)
        return False


def _get_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    """Create / migrate schema if not exists."""
    conn = _get_connection()
    c = conn.cursor()

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT UNIQUE NOT NULL
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS variables (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            stage TEXT NOT NULL DEFAULT '',
            field_name TEXT NOT NULL,
            field_value TEXT,
            UNIQUE(product_id, stage, field_name),
            FOREIGN KEY (product_id) REFERENCES products (id) ON DELETE CASCADE
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS product_prns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            stage TEXT NOT NULL CHECK(stage IN ('Raw','SFG','FG')),
            prn_filename TEXT NOT NULL,
            prn_path TEXT NOT NULL,
            preview_path TEXT,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(product_id, stage, prn_filename),
            FOREIGN KEY (product_id) REFERENCES products (id) ON DELETE CASCADE
        )
        """
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_prns_product_stage ON product_prns(product_id, stage)")

    conn.commit()
    conn.close()


def migrate_legacy_variables():
    """
    If an older DB had a variables table without 'stage', try to migrate gracefully.
    """
    conn = _get_connection()
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='variables'")
    if not c.fetchone():
        conn.close()
        return

    c.execute("PRAGMA table_info(variables)")
    cols = [r[1] for r in c.fetchall()]
    if "stage" not in cols:
        logger.info("Migrating legacy variables table: adding 'stage' column (default empty string)")
        try:
            c.execute("ALTER TABLE variables ADD COLUMN stage TEXT NOT NULL DEFAULT ''")
            try:
                c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_variables_product_stage_field ON variables(product_id, stage, field_name)")
            except Exception:
                pass
            conn.commit()
            logger.info("Migration completed. Existing rows have empty stage; review them if you want to reassign stages.")
        except Exception as e:
            logger.exception("Failed to migrate variables table: %s", e)
    conn.close()


def get_db():
    """Return a sqlite3 connection stored on flask.g, one per request."""
    if "db" not in g:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(exception=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


init_db()
migrate_legacy_variables()


def require_stage_from_request(req_data=None):
    """
    Look up stage in JSON body, form-data or query params.
    Returns canonical stage string or None if invalid.
    """
    stage_raw = ""
    if req_data and isinstance(req_data, dict):
        stage_raw = req_data.get("stage", "") or ""
    if not stage_raw:
        stage_raw = (request.args.get("stage") or request.form.get("stage") or "") or ""
    stage = stage_folder_name(stage_raw)
    if not stage:
        return None
    return stage


@app.route("/", methods=["GET"])
def index():
    try:
        return render_template("index.html")
    except Exception:
        return "<h1>PRN Manager</h1><p>Frontend not found. Place your index.html in templates/</p>"


@app.route("/create-product", methods=["POST"])
def create_product():
    """Create or idempotently ensure a product exists."""
    data = request.get_json(force=True, silent=True)
    if not data or "product_name" not in data:
        return jsonify_error("product_name required", 400)
    pname = str(data["product_name"]).strip()
    if not pname:
        return jsonify_error("product_name cannot be empty", 400)

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("INSERT OR IGNORE INTO products (product_name) VALUES (?)", (pname,))
        conn.commit()
        cur.execute("SELECT id FROM products WHERE product_name = ?", (pname,))
        row = cur.fetchone()
        product_id = row["id"]
    except Exception as e:
        logger.exception("Error creating product")
        return jsonify_error("failed to create product", 500, e)
    return jsonify({"message": "product created or exists", "product_name": pname, "product_id": product_id})


def _fill_prn_content(prn_content: str, variables: dict) -> str:
    """Replace placeholders {FIELD} in prn_content with provided variable values.
    Only replaces exact placeholders matching the field name (case-sensitive).
    """
    # Replace longer variable names first to avoid partial replacements
    for k in sorted(variables.keys(), key=lambda x: -len(x)):
        v = str(variables[k]) if variables[k] is not None else ""
        prn_content = re.sub(r"\{" + re.escape(k) + r"\}", v, prn_content)
    return prn_content


@app.route("/upload", methods=["POST"])
def upload_file():
    """
    Upload .prn for a product + stage.
    multipart/form-data:
      - file: .prn
      - product_name: string
      - stage: Raw|SFG|FG (or accepted variants)
    """
    if "file" not in request.files:
        return jsonify_error("no file part", 400)
    file = request.files["file"]

    product_name = (
        (request.form.get("product_name") or request.args.get("product_name") or request.form.get("product") or request.args.get("product"))
        or ""
    ).strip()
    stage_raw = (request.form.get("stage") or request.args.get("stage") or "").strip()
    stage = stage_folder_name(stage_raw)

    if not product_name:
        return jsonify_error("product_name required", 400)
    
    if not stage:
        return jsonify_error("invalid or missing stage; allowed: Raw, SFG, FG", 400)

    if file.filename == "":
        return jsonify_error("no selected file", 400)
    
    if not allowed_file(file.filename):
        return jsonify_error("only .prn files allowed", 400)

    filename = secure_filename(file.filename)
    safe_product = normalize_product_folder_name(product_name)
    product_dir = os.path.join(app.config["UPLOAD_FOLDER"], safe_product, stage)

    try:
        os.makedirs(product_dir, exist_ok=True)
    except Exception as e:
        logger.exception("Failed to create folder for product uploads")
        return jsonify_error("failed to create product upload folder", 500, e)

    save_path = os.path.join(product_dir, filename)
    if os.path.exists(save_path):
        base, ext = os.path.splitext(filename)
        filename = f"{base}_{int(datetime.utcnow().timestamp())}{ext}"
        save_path = os.path.join(product_dir, filename)
    try:
        file.save(save_path)
    except Exception as e:
        logger.exception("Error saving uploaded file")
        return jsonify_error("failed to save file", 500, e)

    try:
        fields = extract_prn_columns(save_path)
    except Exception as e:
        logger.exception("Error parsing PRN file")
        return jsonify_error("failed to parse file", 500, e)

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("INSERT OR IGNORE INTO products (product_name) VALUES (?)", (product_name,))
        conn.commit()
        cur.execute("SELECT id FROM products WHERE product_name = ?", (product_name,))
        row = cur.fetchone()
        if not row:
            return jsonify_error("failed to create or fetch product", 500)
        product_id = row["id"]

        cur.execute(
            "INSERT INTO product_prns (product_id, stage, prn_filename, prn_path) VALUES (?, ?, ?, ?)",
            (product_id, stage, filename, save_path),
        )
        conn.commit()

        preview_fname = os.path.splitext(filename)[0] + ".png"
        preview_full = os.path.join(product_dir, preview_fname)
        preview_ok = generate_preview(save_path, preview_full)
        if preview_ok:
            try:
                cur.execute(
                    "UPDATE product_prns SET preview_path = ? WHERE product_id = ? AND stage = ? AND prn_filename = ?",
                    (preview_full, product_id, stage, filename),
                )
                conn.commit()
            except Exception:
                logger.exception("Failed to update preview_path in DB after generating preview")

    except sqlite3.IntegrityError as e:
        logger.warning("PRN registration duplicate: %s", e)
        return jsonify_error("PRN registration failed (possible duplicate)", 409, e)
    except Exception as e:
        logger.exception("Failed to register PRN in DB")
        return jsonify_error("failed to register PRN", 500, e)

    preview_url = None
    if os.path.exists(os.path.join(product_dir, os.path.splitext(filename)[0] + ".png")):
        preview_url = f"/preview/{product_name}/{stage}/{filename}"

    return jsonify({"message": "uploaded", "filename": filename, "stage": stage, "saved_path": save_path, "fields": fields, "preview_url": preview_url})


@app.route("/save-fields", methods=["POST"])
def save_fields():
    """
    Save / merge variables for a product. Requires stage (context).
    JSON body:
    {
      "product_name": "Product A",
      "stage": "Raw",
      "variables": { "FIELD1": "val1", "FIELD2": "val2" },
      // optional:
      "generate_filled": true,
      "source_prn_filename": "original.prn"
    }

    If generate_filled is true and source_prn_filename is provided and the file exists, a new PRN will be created by replacing placeholders with the provided values
    and saved as a new PRN record (with preview generated). The response will include preview_url and filled_prn_filename when created.
    """
    data = request.get_json(force=True, silent=True)
    if not data or "product_name" not in data or "variables" not in data:
        return jsonify_error("provide product_name and variables", 400)
    stage = require_stage_from_request(data)
    if not stage:
        return jsonify_error("stage required and must be one of: Raw, SFG, FG", 400)
    product_name = str(data["product_name"]).strip()
    variables = data["variables"]
    if product_name == "":
        return jsonify_error("product_name cannot be empty", 400)
    if not isinstance(variables, dict):
        return jsonify_error("variables must be a JSON object mapping field_name -> value", 400)

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("INSERT OR IGNORE INTO products (product_name) VALUES (?)", (product_name,))
        conn.commit()
        cur.execute("SELECT id FROM products WHERE product_name = ?", (product_name,))
        row = cur.fetchone()
        if not row:
            return jsonify_error("failed to create or fetch product", 500)
        product_id = row["id"]

        for field_name, field_value in variables.items():
            cur.execute(
                """
                INSERT INTO variables (product_id, stage, field_name, field_value)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(product_id, stage, field_name) DO UPDATE SET field_value = excluded.field_value
                """,
                (product_id, stage, field_name, field_value),
            )
        conn.commit()

        # If requested, generate a filled PRN by replacing placeholders in a source PRN
        generate_filled = bool(data.get("generate_filled", False))
        filled_info = None
        if generate_filled and data.get("source_prn_filename"):
            safe_product = normalize_product_folder_name(product_name)
            product_dir = os.path.join(app.config["UPLOAD_FOLDER"], safe_product, stage)
            src_fname = secure_filename(data.get("source_prn_filename"))
            src_path = os.path.join(product_dir, src_fname)
            if os.path.exists(src_path):
                try:
                    with open(src_path, "r", encoding="utf-8", errors="ignore") as f:
                        src_content = f.read()
                    filled_content = _fill_prn_content(src_content, variables)
                    base, ext = os.path.splitext(src_fname)
                    filled_fname = f"{base}_filled_{int(datetime.utcnow().timestamp())}{ext}"
                    filled_path = os.path.join(product_dir, filled_fname)
                    with open(filled_path, "w", encoding="utf-8", errors="ignore") as out_f:
                        out_f.write(filled_content)

                    # register filled prn in DB
                    try:
                        cur.execute(
                            "INSERT INTO product_prns (product_id, stage, prn_filename, prn_path) VALUES (?, ?, ?, ?)",
                            (product_id, stage, filled_fname, filled_path),
                        )
                        conn.commit()
                    except sqlite3.IntegrityError:
                        # fallback: if uniqueness somehow conflicts, append timestamp and retry
                        filled_fname = f"{base}_filled_{int(datetime.utcnow().timestamp())}_{int(datetime.utcnow().timestamp()*1000)}{ext}"
                        filled_path = os.path.join(product_dir, filled_fname)
                        with open(filled_path, "w", encoding="utf-8", errors="ignore") as out_f:
                            out_f.write(filled_content)
                        cur.execute(
                            "INSERT INTO product_prns (product_id, stage, prn_filename, prn_path) VALUES (?, ?, ?, ?)",
                            (product_id, stage, filled_fname, filled_path),
                        )
                        conn.commit()

                    # generate preview for filled PRN
                    preview_fname = os.path.splitext(filled_fname)[0] + ".png"
                    preview_full = os.path.join(product_dir, preview_fname)
                    preview_ok = generate_preview(filled_path, preview_full)
                    if preview_ok:
                        try:
                            cur.execute(
                                "UPDATE product_prns SET preview_path = ? WHERE product_id = ? AND stage = ? AND prn_filename = ?",
                                (preview_full, product_id, stage, filled_fname),
                            )
                            conn.commit()
                        except Exception:
                            logger.exception("Failed to update preview_path for filled PRN in DB")

                    filled_info = {"filled_prn_filename": filled_fname, "preview_url": None}
                    if os.path.exists(preview_full):
                        filled_info["preview_url"] = f"/preview/{product_name}/{stage}/{filled_fname}"
                except Exception as e:
                    logger.exception("Failed to generate filled PRN: %s", e)

    except Exception as e:
        logger.exception("Failed to save fields")
        return jsonify_error("failed to save variable(s)", 500, e)

    resp = {"message": "saved/merged", "product_name": product_name, "stage": stage}
    if filled_info:
        resp.update(filled_info)
    return jsonify(resp)


@app.route("/list-products", methods=["GET"])
def list_products():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, product_name FROM products ORDER BY product_name COLLATE NOCASE")
    products = [{"id": r["id"], "product_name": r["product_name"]} for r in cur.fetchall()]
    return jsonify(products)


@app.route("/get-product/<path:product_name>", methods=["GET"])
def get_product(product_name):
    """
    Get product variables and PRNs for a specific stage.
    Requires query param: ?stage=Raw|SFG|FG (or normalized variants)
    """
    stage_raw = request.args.get("stage", "")
    stage = stage_folder_name(stage_raw)
    if not stage:
        return jsonify_error("stage query param required and must be one of: Raw, SFG, FG", 400)

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT id FROM products WHERE product_name = ?", (product_name,))
    prod = cur.fetchone()
    if not prod:
        cur.execute("INSERT INTO products (product_name) VALUES (?)", (product_name,))
        conn.commit()
        cur.execute("SELECT id FROM products WHERE product_name = ?", (product_name,))
        prod = cur.fetchone()
    product_id = prod["id"]

    cur.execute("SELECT id, field_name, field_value FROM variables WHERE product_id = ? AND stage = ?", (product_id, stage))
    variables = [
        {"id": r["id"], "field_name": r["field_name"], "field_value": r["field_value"]}
        for r in cur.fetchall()
    ]

    prn_counts = {}
    for st in ALLOWED_STAGES:
        cur.execute("SELECT COUNT(1) as cnt FROM product_prns WHERE product_id = ? AND stage = ?", (product_id, st))
        prn_counts[st] = cur.fetchone()["cnt"]

    cur.execute(
        "SELECT id, prn_filename, prn_path, preview_path, uploaded_at FROM product_prns WHERE product_id = ? AND stage = ? ORDER BY uploaded_at DESC",
        (product_id, stage),
    )
    prns = []
    for r in cur.fetchall():
        preview_url = None
        if r["preview_path"] and os.path.exists(r["preview_path"]):
            preview_url = f"/preview/{product_name}/{stage}/{r['prn_filename']}"
        prns.append(
            {
                "id": r["id"],
                "prn_filename": r["prn_filename"],
                "uploaded_at": r["uploaded_at"],
                "preview_url": preview_url,
            }
        )

    return jsonify(
        {
            "product_name": product_name,
            "stage": stage,
            "variables": variables,
            "prn_counts": prn_counts,
            "prns_for_stage": prns,
        }
    )


@app.route("/add-variable", methods=["POST"])
def add_variable():
    """
    Add a single variable for a product.
    JSON: { "product_name": "Product A", "stage":"Raw", "field_name":"QTY", "field_value":"10" }
    """
    data = request.get_json(force=True, silent=True)
    if not data or "product_name" not in data or "field_name" not in data:
        return jsonify_error("product_name and field_name required", 400)

    stage = require_stage_from_request(data)
    if not stage:
        return jsonify_error("stage required and must be one of: Raw, SFG, FG", 400)

    product_name = str(data["product_name"]).strip()
    field_name = str(data["field_name"]).strip()
    field_value = data.get("field_value", "")

    if not product_name or not field_name:
        return jsonify_error("product_name and field_name cannot be empty", 400)

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM products WHERE product_name = ?", (product_name,))
    row = cur.fetchone()
    if not row:
        return jsonify_error("product not found", 404)
    product_id = row["id"]

    try:
        cur.execute(
            "INSERT INTO variables (product_id, stage, field_name, field_value) VALUES (?, ?, ?, ?)",
            (product_id, stage, field_name, field_value),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        return jsonify_error("variable already exists for this product+stage", 409)
    except Exception as e:
        logger.exception("Error inserting variable")
        return jsonify_error("failed to add variable", 500, e)
    return jsonify({"message": "variable added", "field_name": field_name, "product_name": product_name, "stage": stage})


@app.route("/add_variable", methods=["POST"])
def match_variable_from_db_with_prn():
    """
    Given { product_name, stage (required), prn_content }, check which variables are present in the PRN content.
    Returns mapping field_name -> boolean
    """
    data = request.get_json(force=True, silent=True)
    if not data or "product_name" not in data or "prn_content" not in data:
        return jsonify_error("product_name and prn_content required", 400)

    stage = require_stage_from_request(data)
    if not stage:
        return jsonify_error("stage required and must be one of: Raw, SFG, FG", 400)

    product_name = str(data["product_name"]).strip()
    prn_content = str(data["prn_content"])

    if product_name == "" or prn_content == "":
        return jsonify_error("product_name and prn_content cannot be empty", 400)

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM products WHERE product_name = ?", (product_name,))
    row = cur.fetchone()
    if not row:
        return jsonify_error("product not found", 404)
    product_id = row["id"]

    cur.execute("SELECT field_name FROM variables WHERE product_id = ? AND stage = ?", (product_id, stage))
    variables = [r["field_name"] for r in cur.fetchall()]

    matched = {}
    for var in variables:
        pattern = r"\{" + re.escape(var) + r"\}"
        matched[var] = bool(re.search(pattern, prn_content))

    return jsonify({"matched_variables": matched, "product_name": product_name, "stage": stage})


@app.route("/update-variable", methods=["PUT"])
def update_variable():
    """
    Update variable name/value.
    JSON: { "product_name": "...", "stage":"Raw", "old_field_name":"OLD", "new_field_name":"NEW", "new_field_value":"...", "generate_filled": true, "source_prn_filename": "optional.prn" }
    If generate_filled is true the server will attempt to create a filled PRN using the provided source_prn_filename (or the most recent template for the product+stage) and return filled_prn_filename and preview_url in the response.
    """
    data = request.get_json(force=True, silent=True)
    required = ("product_name" in (data or {}) and "old_field_name" in (data or {}) and ("new_field_name" in (data or {}) or "new_field_value" in (data or {})))
    if not data or not required:
        return jsonify_error("product_name, old_field_name and (new_field_name or new_field_value) required", 400)

    stage = require_stage_from_request(data)
    if not stage:
        return jsonify_error("stage required and must be one of: Raw, SFG, FG", 400)

    product_name = str(data["product_name"]).strip()
    old = str(data["old_field_name"]).strip()
    new_name = str(data.get("new_field_name", old)).strip()
    new_value = data.get("new_field_value", None)

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM products WHERE product_name = ?", (product_name,))
    row = cur.fetchone()
    if not row:
        return jsonify_error("product not found", 404)
    product_id = row["id"]

    cur.execute("SELECT id FROM variables WHERE product_id = ? AND stage = ? AND field_name = ?", (product_id, stage, old))
    varrow = cur.fetchone()
    if not varrow:
        return jsonify_error("variable not found", 404)
    var_id = varrow["id"]

    if new_name != old:
        cur.execute("SELECT id FROM variables WHERE product_id = ? AND stage = ? AND field_name = ?", (product_id, stage, new_name))
        if cur.fetchone():
            return jsonify_error("target variable name already exists for this product+stage", 409)

    try:
        if new_name != old:
            cur.execute("UPDATE variables SET field_name = ? WHERE id = ?", (new_name, var_id))

        if new_value is not None:
            cur.execute("UPDATE variables SET field_value = ? WHERE id = ?", (new_value, var_id))
        conn.commit()

    except Exception as e:
        logger.exception("Error updating variable")
        return jsonify_error("failed to update variable", 500, e)

    response = {"message": "variable updated", "field_name": new_name, "product_name": product_name, "stage": stage}

    # optionally generate a filled PRN after update
    try:
        generate_filled = bool(data.get("generate_filled", False))
        if generate_filled:
            # build variables map for this product+stage from DB
            cur.execute("SELECT field_name, field_value FROM variables WHERE product_id = ? AND stage = ?", (product_id, stage))
            variables = {r["field_name"]: r["field_value"] for r in cur.fetchall()}
            # ensure our updated field is reflected
            variables[new_name] = new_value if new_value is not None else variables.get(new_name, "")

            # determine source template PRN
            src_fname = None
            if data.get("source_prn_filename"):
                src_fname = secure_filename(data.get("source_prn_filename"))
            else:
                # pick most recent uploaded PRN for product+stage
                cur.execute("SELECT prn_filename, prn_path FROM product_prns WHERE product_id = ? AND stage = ? ORDER BY uploaded_at DESC LIMIT 1", (product_id, stage))
                r = cur.fetchone()
                if r:
                    src_fname = r["prn_filename"]

            if src_fname:
                safe_product = normalize_product_folder_name(product_name)
                product_dir = os.path.join(app.config["UPLOAD_FOLDER"], safe_product, stage)
                src_path = os.path.join(product_dir, src_fname)
                if os.path.exists(src_path):
                    try:
                        with open(src_path, "r", encoding="utf-8", errors="ignore") as f:
                            src_content = f.read()
                        filled_content = _fill_prn_content(src_content, variables)
                        base, ext = os.path.splitext(src_fname)
                        filled_fname = f"{base}_filled_{int(datetime.utcnow().timestamp())}{ext}"
                        filled_path = os.path.join(product_dir, filled_fname)
                        with open(filled_path, "w", encoding="utf-8", errors="ignore") as out_f:
                            out_f.write(filled_content)

                        # register filled prn in DB
                        try:
                            cur.execute(
                                "INSERT INTO product_prns (product_id, stage, prn_filename, prn_path) VALUES (?, ?, ?, ?)",
                                (product_id, stage, filled_fname, filled_path),
                            )
                            conn.commit()
                        except sqlite3.IntegrityError:
                            filled_fname = f"{base}_filled_{int(datetime.utcnow().timestamp())}_{int(datetime.utcnow().timestamp()*1000)}{ext}"
                            filled_path = os.path.join(product_dir, filled_fname)
                            with open(filled_path, "w", encoding="utf-8", errors="ignore") as out_f:
                                out_f.write(filled_content)
                            cur.execute(
                                "INSERT INTO product_prns (product_id, stage, prn_filename, prn_path) VALUES (?, ?, ?, ?)",
                                (product_id, stage, filled_fname, filled_path),
                            )
                            conn.commit()

                        # generate preview for filled PRN
                        preview_fname = os.path.splitext(filled_fname)[0] + ".png"
                        preview_full = os.path.join(product_dir, preview_fname)
                        preview_ok = generate_preview(filled_path, preview_full)
                        if preview_ok:
                            try:
                                cur.execute(
                                    "UPDATE product_prns SET preview_path = ? WHERE product_id = ? AND stage = ? AND prn_filename = ?",
                                    (preview_full, product_id, stage, filled_fname),
                                )
                                conn.commit()
                            except Exception:
                                logger.exception("Failed to update preview_path for filled PRN in DB after update-variable generation")

                        response.update({"filled_prn_filename": filled_fname, "preview_url": None})
                        if os.path.exists(preview_full):
                            response["preview_url"] = f"/preview/{product_name}/{stage}/{filled_fname}"
                            response["created_at"] = datetime.utcnow().isoformat()
                    except Exception as e:
                        logger.exception("Failed to create filled PRN after variable update: %s", e)
                        # fall through without failing the whole request
                else:
                    logger.warning("Requested source_prn_filename does not exist for generation: %s", src_path)
            else:
                logger.info("No source PRN found to generate filled PRN after variable update")
    except Exception as e:
        logger.exception("Error during optional generation after update-variable: %s", e)

    return jsonify(response)


@app.route("/delete-variable", methods=["DELETE"])
def delete_variable():
    """
    Delete a variable. Requires stage context.
    JSON: { "product_name":"...", "stage":"Raw", "field_name":"..." }
    """
    data = request.get_json(force=True, silent=True)
    if not data or "product_name" not in data or "field_name" not in data:
        return jsonify_error("product_name and field_name required", 400)

    stage = require_stage_from_request(data)
    if not stage:
        return jsonify_error("stage required and must be one of: Raw, SFG, FG", 400)

    product_name = str(data["product_name"]).strip()
    field_name = str(data["field_name"]).strip()

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM products WHERE product_name = ?", (product_name,))
    row = cur.fetchone()
    if not row:
        return jsonify_error("product not found", 404)
    product_id = row["id"]

    cur.execute("DELETE FROM variables WHERE product_id = ? AND stage = ? AND field_name = ?", (product_id, stage, field_name))
    if cur.rowcount == 0:
        return jsonify_error("variable not found for this product+stage", 404)
    conn.commit()
    return jsonify({"message": "variable deleted", "field_name": field_name, "product_name": product_name, "stage": stage})


@app.route("/list-prns/<path:product_name>/<path:stage>", methods=["GET"])
def list_prns(product_name, stage):
    st = stage_folder_name(stage)
    if not st:
        return jsonify_error("invalid stage", 400)
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM products WHERE product_name = ?", (product_name,))
    prod = cur.fetchone()
    if not prod:
        return jsonify([])
    pid = prod["id"]
    cur.execute("SELECT id, prn_filename, prn_path, preview_path, uploaded_at FROM product_prns WHERE product_id = ? AND stage = ? ORDER BY uploaded_at DESC", (pid, st))
    rows = cur.fetchall()
    res = [
        {
            "id": r["id"],
            "prn_filename": r["prn_filename"],
            "prn_path": r["prn_path"],
            "preview_path": r["preview_path"],
            "uploaded_at": r["uploaded_at"],
        }
        for r in rows
    ]
    return jsonify(res)


@app.route("/get-prn/<path:product_name>/<path:stage>/<path:filename>", methods=["GET"])
def get_prn(product_name, stage, filename):
    """Return the PRN file as an attachment if it belongs to the product+stage."""
    st = stage_folder_name(stage)
    if not st:
        return jsonify_error("invalid stage", 400)
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM products WHERE product_name = ?", (product_name,))
    prod = cur.fetchone()
    if not prod:
        return jsonify_error("product not found", 404)
    pid = prod["id"]
    fname = secure_filename(filename)
    cur.execute("SELECT prn_path FROM product_prns WHERE product_id = ? AND stage = ? AND prn_filename = ?", (pid, st, fname))
    r = cur.fetchone()
    if not r:
        return jsonify_error("prn not found", 404)
    path = r["prn_path"]
    if not os.path.exists(path):
        return jsonify_error("file missing on disk", 404)
    return send_file(path, as_attachment=True)


@app.route("/preview/<path:product_name>/<path:stage>/<path:filename>", methods=["GET"])
def serve_preview(product_name, stage, filename):
    # Serve generated PNG preview if exists for that PRN record
    st = stage_folder_name(stage)
    if not st:
        return jsonify_error("invalid stage", 400)
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM products WHERE product_name = ?", (product_name,))
    prod = cur.fetchone()
    if not prod:
        return jsonify_error("product not found", 404)
    pid = prod["id"]
    fname = secure_filename(filename)
    cur.execute("SELECT preview_path FROM product_prns WHERE product_id = ? AND stage = ? AND prn_filename = ?", (pid, st, fname))
    r = cur.fetchone()
    if not r or not r["preview_path"]:
        return jsonify_error("preview not available", 404)
    preview_path = r["preview_path"]
    if not os.path.exists(preview_path):
        return jsonify_error("preview file missing", 404)

    # support ?download=1 to force download/attachment
    download_flag = (request.args.get("download") or "").lower() in ("1", "true", "yes")
    try:
        return send_file(preview_path, mimetype="image/png", as_attachment=download_flag)
    except Exception as e:
        logger.exception("Error sending preview file: %s", e)
        return jsonify_error("failed to send preview", 500, e)


@app.route("/delete-prn", methods=["DELETE"])
def delete_prn():
    """
    Delete a PRN record and remove the files.
    JSON body required: { "product_name": "...", "stage": "Raw|SFG|FG", "prn_filename": "file.prn" }
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify_error("invalid json body", 400)
    for k in ("product_name", "stage", "prn_filename"):
        if k not in data:
            return jsonify_error(f"{k} required", 400)

    st = stage_folder_name(data["stage"])
    if not st:
        return jsonify_error("invalid stage", 400)

    pname = str(data["product_name"]).strip()
    fname = secure_filename(data["prn_filename"])

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM products WHERE product_name = ?", (pname,))
    prod = cur.fetchone()
    if not prod:
        return jsonify_error("product not found", 404)
    pid = prod["id"]

    cur.execute("SELECT id, prn_path, preview_path FROM product_prns WHERE product_id = ? AND stage = ? AND prn_filename = ?", (pid, st, fname))
    r = cur.fetchone()
    if not r:
        return jsonify_error("prn not found", 404)
    prn_id = r["id"]
    prn_path = r["prn_path"]
    preview_path = r["preview_path"]

    cur.execute("DELETE FROM product_prns WHERE id = ?", (prn_id,))
    conn.commit()

    # best-effort file removal, do not fail the API if file deletion fails
    try:
        if prn_path and os.path.exists(prn_path):
            os.remove(prn_path)
        if preview_path and os.path.exists(preview_path):
            os.remove(preview_path)
    except Exception as e:
        logger.warning("Error removing files during delete-prn: %s", e)

    return jsonify({"message": "deleted", "prn_filename": fname})


@app.route("/download/<path:filename>", methods=["GET"])
def download(filename):
    """
    Backwards compatible convenience endpoint that searches the uploads tree.
    Prefer using /get-prn/<product>/<stage>/<filename>.
    """
    filename = secure_filename(filename)
    # check immediate uploads root
    file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    # search tree
    for root, dirs, files in os.walk(app.config["UPLOAD_FOLDER"]):
        if filename in files:
            return send_file(os.path.join(root, filename), as_attachment=True)
    abort(404)

if __name__ == "__main__":
    logger.info("Starting PRN Manager on 0.0.0.0:5001")
    app.run(host="0.0.0.0", port=5001, debug=True)
