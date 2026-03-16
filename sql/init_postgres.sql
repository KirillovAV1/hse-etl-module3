CREATE TABLE IF NOT EXISTS user_sessions (
    session_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    pages_visited TEXT,
    device VARCHAR(50),
    actions TEXT
);

CREATE TABLE IF NOT EXISTS event_logs (
    event_id VARCHAR(100) PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    details TEXT
);

CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    issue_type VARCHAR(100) NOT NULL,
    messages TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS mart_user_activity (
    user_id VARCHAR(100) PRIMARY KEY,
    sessions_count INT NOT NULL,
    avg_session_duration_minutes NUMERIC(10,2) NOT NULL,
    total_actions INT NOT NULL
);

CREATE TABLE IF NOT EXISTS mart_support_stats (
    issue_type VARCHAR(100) PRIMARY KEY,
    tickets_count INT NOT NULL,
    open_tickets INT NOT NULL,
    avg_resolution_hours NUMERIC(10,2) NOT NULL
);