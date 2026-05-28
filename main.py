import os

import json
import threading
from datetime import datetime, timezone

import redis
import threading
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit

from google.cloud import pubsub_v1, tasks_v2, storage, firestore
from google.cloud import monitoring_v3

import time


app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

r = redis.Redis(
    host=os.environ.get("REDIS_HOST", "127.0.0.1"),
    port=int(os.environ.get("REDIS_PORT", 6379)),
    decode_responses=True
)

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode='threading'
)

# HOSTNAME est exposé automatiquement par Cloud Run. 
# Il identifie le conteneur de façon unique — chaque conteneur a un hostname différent
# même si Cloud Run scale le même service à plusieurs conteneurs.
# En local, la variable n'existe pas — on utilise "local" par défaut.
SERVER_ID = os.environ.get("K_SERVICE", "local")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "tp-cloud-tasks-firestore")
GCP_REGION = os.environ.get("REGION", "europe-west1")

db = firestore.Client()
storage_client = storage.Client()
tasks_client = tasks_v2.CloudTasksClient()

# Pub/Sub Configuration
TOPIC_NAME = os.environ.get("TOPIC_NAME", "events-topic")
SUBSCRIPTION_NAME = os.environ.get("SUBSCRIPTION_NAME", "events-subscription-1")

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

TOPIC_PATH = publisher.topic_path(GCP_PROJECT_ID, TOPIC_NAME)
SUBSCRIPTION_PATH = subscriber.subscription_path(GCP_PROJECT_ID, SUBSCRIPTION_NAME)

# Cloud Tasks & Storage Configuration
TASK_QUEUE = os.environ.get("TASK_QUEUE", "")
PROCESSOR_URL = os.environ.get("PROCESSOR_URL", "")
SNAPSHOT_BUCKET = os.environ.get("SNAPSHOT_BUCKET", "")

# Rate Limiting Settings
RATE_LIMIT = int(os.environ.get("RATE_LIMIT_PER_MIN", 10))
RATE_WINDOW = 60
PROTECTED_ROUTES = {"/publish"}

monitoring_client = monitoring_v3.MetricServiceClient()
MONITORING_PROJECT_NAME = f"projects/{GCP_PROJECT_ID}"


@app.before_request
def rate_limit_middleware():
    if request.path not in PROTECTED_ROUTES:
        return

    if request.method != "POST":
        return
    
    body = request.get_json(silent=True) or {}
    player_id = request.headers.get("X-Player-ID", "anonymous")

    try:
        allowed, player_limit = _check_rate_limit(player_id)
    except Exception as e:
        app.logger.error(f"Rate limit check failed: {e}")
        return

    if not allowed:
        _record_rate_limit_event(player_id)

        return jsonify({
            "error":   "Rate limit exceeded",
            "limit":   player_limit,
            "window":  f"{RATE_WINDOW}s",
            "player_id": player_id
        }), 429


@firestore.transactional
def _update_rate_limit(transaction, doc_ref, now, player_limit):
    snapshot = doc_ref.get(transaction=transaction)
    data = snapshot.to_dict() if snapshot.exists else {}

    window_start = data.get("window_start")
    count = data.get("count", 0)

    if window_start is None or (now - window_start.timestamp()) > RATE_WINDOW:
        count = 0
        window_start = now

    if count >= player_limit:
        return False

    transaction.set(doc_ref, {
        "count": count + 1,
        "window_start": datetime.fromtimestamp(window_start if isinstance(window_start, float) else window_start.timestamp(), tz=timezone.utc),
        "last_request": datetime.now(timezone.utc),
    })

    return True


def _get_player_rate_limit(player_id: str) -> int:
    try:
        doc = db.collection("players").document(player_id).get()
        if doc.exists:
            return int(doc.to_dict().get("rate_limit", RATE_LIMIT))
    except Exception as e:
        app.logger.warning(f"Impossible de lire le quota Firestore pour {player_id}: {e}")

    return RATE_LIMIT



def _check_rate_limit(player_id: str) -> tuple[bool, int]:
    player_limit = _get_player_rate_limit(player_id)
    doc_ref = db.collection("rate_limits").document(player_id)
    transaction = db.transaction()
    now = datetime.now(timezone.utc).timestamp()
    allowed = _update_rate_limit(transaction, doc_ref, now, player_limit)

    return allowed, player_limit


def _create_snapshot_task(redis_key: str):
    queue_path = tasks_client.queue_path(GCP_PROJECT_ID, GCP_REGION, TASK_QUEUE)
    payload = json.dumps({"redis_key": redis_key}).encode()

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": f"{PROCESSOR_URL}/process",
            "headers": {"Content-Type": "application/json"},
            "body": payload,
        }
    }
    tasks_client.create_task(request={
        "parent": queue_path,
        "task": task
    })


@app.route("/publish", methods=["POST"])
def publish():
    data = request.get_json()

    if "message" not in data:
        return jsonify({"error": "Champ 'message' requis"}), 400

    # Le serveur enrichit la donnée avant de l'écrire dans Redis.
    # Le client n'a pas à connaître l'identité du serveur ni l'heure.
    entry = {
        "message": data["message"],
        "server_id": SERVER_ID,
        "published_at": datetime.now(timezone.utc).isoformat()
    }

    # La clé Redis inclut server_id et published_at pour être unique.
    # setex écrit la valeur avec un TTL de 3600 secondes (1 heure).
    key = f"event:{SERVER_ID}:{entry['published_at']}"
    r.setex(key, 3600, json.dumps(entry))

    if publisher and TOPIC_PATH:
        publisher.publish(TOPIC_PATH, key.encode())

    if GCP_PROJECT_ID and TASK_QUEUE and PROCESSOR_URL:
        _create_snapshot_task(key)

    player_id = request.headers.get("X-Player-ID", "anonymous")
    _update_analytics_async(player_id)

    return jsonify({
        "status": "published",
        "redis_key": key,
        "data": entry
    })


@app.route("/process", methods=["POST"])
def process():
    body = request.get_json()
    trigger_key = body.get("redis_key")

    if not trigger_key:
        return jsonify({
            "error": "redis_key manquant"
        }), 400
    
    trigger_data = r.get(trigger_key)

    if not trigger_data:
        return jsonify({
            "status": "skipped",
            "reason": "key expired"
        }), 200
    
    game_state = {}
    cursor = 0

    while True:
        cursor, keys = r.scan(cursor=cursor, match="event:*", count=100)

        for key in keys:
            value = r.get(key)
            if value:
                game_state[key] = json.loads(value)

        if cursor == 0:
            break

    snapshot = {
        "snapshot_at": datetime.now(timezone.utc).isoformat(),
        "trigger_key": trigger_key,
        "trigger_event": json.loads(trigger_data),
        "game_state": game_state,
        "event_count": len(game_state),
    }

    now = datetime.now(timezone.utc)
    blob_name = f"snapshots/{now.strftime('%Y-%m-%d')}/{now.timestamp():.0f}.json"
    bucket = storage_client.bucket(SNAPSHOT_BUCKET)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(
        json.dumps(snapshot, indent=2),
        content_type="application/json"
    )

    return jsonify({
        "status": "snapshot_saved",
        "blob": blob_name,
        "event_count": len(game_state)
    }), 200


@app.route("/analytics")
def analytics():
    if request.headers.get("X-Admin-Key") != os.environ.get("ADMIN_KEY", "changeme"):
        return jsonify({"error": "Unauthorized"}), 401
    
    results = {}
    for doc in db.collection("analytics").stream():
        results[doc.id] = doc.to_dict()

    quotas = {}
    for doc in db.collection("rate_limits").stream():
        quotas[doc.id] = doc.to_dict()

    return jsonify({
        "server_id": SERVER_ID,
        "analytics": results,
        "quotas": quotas
    })


@app.route("/data")
def data():
    # SCAN parcourt les clés par lots sans bloquer le serveur Redis.
    # KEYS("event:*") ferait la même chose mais bloque Redis le temps de parcourir
    # toutes les clés — inacceptable en production sur de gros volumes.
    result = {}
    cursor = 0

    while True:
        cursor, keys = r.scan(cursor=cursor, match="event:*", count=100)

        for key in keys:
            value = r.get(key)
            ttl   = r.ttl(key)

            if value:
                result[key] = {
                    "data": json.loads(value),
                    "ttl_remaining_seconds": ttl
                }
                               
        if cursor == 0:
            break

    return jsonify({
        "server_id": SERVER_ID,
        "count": len(result),
        "entries": result,
        "timestamp": datetime.now(timezone.utc).isoformat()
    })
        

@app.route("/health")
def health():
    try:
        r.ping()
        return jsonify({
            "status": "healthy",
            "server_id": SERVER_ID,
            "redis": "connected",
        }), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 503
    

@app.route("/snapshots", methods=["GET"])
def snapshots():
    if not SNAPSHOT_BUCKET:
        return jsonify({
            "error": "SNAPSHOT_BUCKET non configuré"
        }), 500

    bucket = storage_client.bucket(SNAPSHOT_BUCKET)
    requested_file = request.args.get("file")

    # GET /snapshots?file=...
    # Snapshot content
    if requested_file:
        blob = bucket.blob(requested_file)

        if not blob.exists():
            return jsonify({
                "error": "Snapshot introuvable",
                "file": requested_file
            }), 404

        try:
            content = blob.download_as_text()
            snapshot_data = json.loads(content)

            return jsonify({
                "status": "loaded",
                "snapshot": snapshot_data
            }), 200

        except Exception as e:
            return jsonify({
                "error": "Impossible de lire le snapshot",
                "details": str(e)
            }), 500

    # GET /snapshots
    # Snapshots list
    try:
        blobs = storage_client.list_blobs(
            SNAPSHOT_BUCKET,
            prefix="snapshots/"
        )

        snapshots = []

        for blob in blobs:
            snapshots.append({
                "name": blob.name,
                "size_bytes": blob.size,
                "updated": blob.updated.isoformat() if blob.updated else None,
                "generation": blob.generation,
            })

        snapshots.sort(key=lambda x: x["updated"] or "", reverse=True)

        return jsonify({
            "bucket": SNAPSHOT_BUCKET,
            "count": len(snapshots),
            "snapshots": snapshots
        }), 200

    except Exception as e:
        return jsonify({
            "error": "Erreur listing snapshots",
            "details": str(e)
        }), 500


@socketio.on('connect')
def handle_connect():
    result = {}

    for key in r.scan_iter("event:*"):
        val = r.get(key)
        if val:
            result[key] = json.loads(val)
    
    emit('initial_state', {
        "server_id": SERVER_ID,
        "data": result
    })


def _update_analytics_async(player_id: str):
    def _write():
        try:
            doc_ref = db.collection("analytics").document(player_id)
            doc_ref.set({
                "total_requests": firestore.Increment(1),
                "last_seen": datetime.now(timezone.utc),
            }, merge=True)
        except Exception as e:
            app.logger.warning(f"Analytics write failed: {e}")

    threading.Thread(target=_write, daemon=True).start()


def _record_rate_limit_event(player_id: str):
    def _send():
        try:
            series = monitoring_v3.TimeSeries()
            now = time.time()

            series.metric.type = "custom.googleapis.com/api/rate_limit_exceeded"

            series.metric.labels.update({
                "player_id": player_id,
                "server_id": SERVER_ID
            })

            series.resource.type = "global"
            series.resource.labels.update({
                "project_id": GCP_PROJECT_ID
            })

            point = monitoring_v3.Point()

            # métrique GAUGE
            point.value.int64_value = 1

            point.interval = monitoring_v3.TimeInterval(
                end_time={"seconds": int(now)}
            )

            series.points = [point]

            monitoring_client.create_time_series(
                name=MONITORING_PROJECT_NAME,
                time_series=[series]
            )

        except Exception as e:
            app.logger.warning(f"Monitoring metric failed: {e}")

    threading.Thread(target=_send, daemon=True).start()


def listen_to_pubsub():
    if not GCP_PROJECT_ID or not SUBSCRIPTION_NAME:
        print("Pub/Sub listener désactivé (variables manquantes).")
        return

    print(f"[*] Démarrage de l'écouteur sur : {SUBSCRIPTION_PATH}")

    def callback(message):
        try:
            key = message.data.decode("utf-8")
            data = r.get(key)

            if data:
                socketio.emit("update", {
                    "key": key,
                    "data": data
                })
            message.ack()
        except Exception as e:
            print(f"Erreur callback Pub/Sub: {e}")

    streaming_pull_future = subscriber.subscribe(SUBSCRIPTION_PATH, callback=callback)
    
    try:
        streaming_pull_future.result()
    except Exception as e:
        print(f"Erreur écouteur Pub/Sub: {e}")

if SUBSCRIPTION_NAME:
    threading.Thread(target=listen_to_pubsub, daemon=True).start()


if __name__ == "__main__":
    socketio.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8080)),
        use_reloader=False,
        debug=True,
        log_output=True
    )