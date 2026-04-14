import os
import json
from datetime import datetime, timezone
import redis
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from google.cloud import pubsub_v1
import threading


app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

r = redis.Redis(
    host=os.environ.get("REDIS_HOST", "127.0.0.1"),
    port=int(os.environ.get("REDIS_PORT", 6379)),
    decode_responses=True
)

# SocketIO
socketio = SocketIO(
    app,
    async_mode='threading',
    cors_allowed_origins="*",
    ping_timeout=60,
    ping_interval=25
)

# HOSTNAME est exposé automatiquement par Cloud Run. 
# Il identifie le conteneur de façon unique — chaque conteneur a un hostname différent
# même si Cloud Run scale le même service à plusieurs conteneurs.
# En local, la variable n'existe pas — on utilise "local" par défaut.
SERVER_ID = os.environ.get("K_SERVICE", "local")

# GCP Pub/Sub
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "mon-projet-gcp-game-redis")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "events-topic")
SUBSCRIPTION_NAME = os.environ.get("SUBSCRIPTION_NAME", "events-subscription-1")

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

TOPIC_PATH = publisher.topic_path(GCP_PROJECT_ID, TOPIC_NAME)
SUBSCRIPTION_PATH = subscriber.subscription_path(GCP_PROJECT_ID, SUBSCRIPTION_NAME)


def pubsub_callback(message):
    try:
        redis_key = message.data.decode("utf-8")
        print("Pub/Sub reçu:", redis_key)

        value = r.get(redis_key)

        if value:
            data = json.loads(value)

            socketio.emit("update", {
                "key": redis_key,
                "data": data
            })

        message.ack()

    except Exception as e:
        print("Erreur Pub/Sub callback:", e)


@socketio.on("connect")
def handle_connect():
    print(f"Client connecté à l'instance {SERVER_ID}")

    result = {}
    cursor = 0

    while True:
        cursor, keys = r.scan(cursor=cursor, match="event:*", count=100)
        for key in keys:
            value = r.get(key)
            ttl   = r.ttl(key)
            if value:
                result[key] = {"data": json.loads(value), "ttl_remaining_seconds": ttl}
        if cursor == 0:
            break

    emit("initial_state", {
        "server_id": SERVER_ID,
        "count": len(result),
        "entries": result,
        "timestamp": datetime.now(timezone.utc).isoformat()
    })


@socketio.on("disconnect")
def handle_disconnect():
    print(f"Client déconnecté de l'instance {SERVER_ID}")


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

    # Publier le message sur Pub/Sub
    publisher.publish(TOPIC_PATH, key.encode("utf-8"))

    return jsonify({
        "status": "published",
        "redis_key": key,
        "data": entry
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
                result[key] = {"data": json.loads(value), "ttl_remaining_seconds": ttl}
        if cursor == 0:
            break
    return jsonify({"server_id": SERVER_ID, "count": len(result), "entries": result, "timestamp": datetime.now(timezone.utc).isoformat()})


@app.route("/health")
def health():
    try:
        r.ping()
        return jsonify({"status": "healthy", "server_id": SERVER_ID, "redis": "connected"}), 200
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 503


def start_pubsub_listener():
    print("Démarrage listener Pub/Sub...")

    streaming_pull_future = subscriber.subscribe(
        SUBSCRIPTION_PATH,
        callback=pubsub_callback
    )

    print("Listener Pub/Sub actif")

    try:
        streaming_pull_future.result()
    except Exception as e:
        print("Erreur listener Pub/Sub:", e)
        streaming_pull_future.cancel()


if __name__ == "__main__":
    threading.Thread(
        target=start_pubsub_listener,
        daemon=True
    ).start()

    socketio.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8080)),
        debug=True
    )