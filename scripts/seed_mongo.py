from datetime import datetime, timedelta
from random import choice, randint
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["etl_mongo_db"]

user_sessions = db["user_sessions"]
event_logs = db["event_logs"]
support_tickets = db["support_tickets"]

user_sessions.delete_many({})
event_logs.delete_many({})
support_tickets.delete_many({})

users = [f"user_{i}" for i in range(1, 11)]
pages = ["/home", "/catalog", "/product/1", "/product/2", "/cart", "/checkout"]
devices = ["mobile", "desktop", "tablet"]
actions_pool = ["login", "view_product", "add_to_cart", "checkout", "logout"]
issue_types = ["payment", "delivery", "account", "refund"]
statuses = ["open", "closed", "in_progress"]

base_time = datetime(2024, 1, 1, 10, 0, 0)

sessions_docs = []
for i in range(1, 101):
    start = base_time + timedelta(minutes=randint(0, 5000))
    duration = randint(5, 90)
    end = start + timedelta(minutes=duration)
    visited = [choice(pages) for _ in range(randint(2, 6))]
    acts = [choice(actions_pool) for _ in range(randint(2, 6))]

    sessions_docs.append({
        "session_id": f"sess_{i}",
        "user_id": choice(users),
        "start_time": start,
        "end_time": end,
        "pages_visited": visited,
        "device": choice(devices),
        "actions": acts
    })

events_docs = []
for i in range(1, 201):
    ts = base_time + timedelta(minutes=randint(0, 5000))
    events_docs.append({
        "event_id": f"evt_{i}",
        "timestamp": ts,
        "event_type": choice(["клик", "просмотр", "скролл", "покупка"]),
        "details": choice(pages)
    })

tickets_docs = []
for i in range(1, 51):
    created = base_time + timedelta(hours=randint(0, 300))
    updated = created + timedelta(hours=randint(1, 72))
    tickets_docs.append({
        "ticket_id": f"ticket_{i}",
        "user_id": choice(users),
        "status": choice(statuses),
        "issue_type": choice(issue_types),
        "messages": [
            {
                "sender": "user",
                "message": "Нужна помощь",
                "timestamp": created
            },
            {
                "sender": "support",
                "message": "Проверяем",
                "timestamp": updated
            }
        ],
        "created_at": created,
        "updated_at": updated
    })

user_sessions.insert_many(sessions_docs)
event_logs.insert_many(events_docs)
support_tickets.insert_many(tickets_docs)
