CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    session_id TEXT
);

-- Events table
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    -- Nullable for recurring events
    start_time DATETIME,
    end_time DATETIME,
    location TEXT,
    all_day BOOLEAN DEFAULT 0,
    calendar_id TEXT DEFAULT 'work',
    -- Recurrence support
    rrule TEXT,
    duration TEXT,
    exdate TEXT, -- JSON array of ISO strings
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    session_id TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Calendars table
CREATE TABLE IF NOT EXISTS calendars (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    color TEXT NOT NULL,
    text_color TEXT NOT NULL,
    owner_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    session_id TEXT,
    FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Calendar shares table
CREATE TABLE IF NOT EXISTS calendar_shares (
    calendar_id TEXT NOT NULL,
    owner_id INTEGER NOT NULL,
    shared_with_id INTEGER NOT NULL,
    can_view INTEGER DEFAULT 1,
    can_edit INTEGER DEFAULT 0,
    can_delete INTEGER DEFAULT 0,
    can_share INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    session_id TEXT,
    PRIMARY KEY (calendar_id, shared_with_id),
    FOREIGN KEY (calendar_id) REFERENCES calendars(id) ON DELETE CASCADE,
    FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (shared_with_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Attendees table (many-to-many relationship)
CREATE TABLE IF NOT EXISTS attendees (
    event_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    status TEXT CHECK(status IN ('pending', 'accepted', 'declined')) DEFAULT 'pending',
    responded_at DATETIME,
    session_id TEXT,
    PRIMARY KEY (event_id, user_id),
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);