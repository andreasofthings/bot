# Requirements Specification: Extensible Matrix Multi-Capability Bot

This document defines the functional and non-functional requirements for the Matrix Multi-Capability Bot. The bot acts as an extensible gateway, providing bridging to other platforms, feed syndication with advanced filters, and market monitoring with technical indicator subscriptions.

Furthermore, this specification includes **productization and lead capture** systems, alongside **deployment, operations, and database abstraction/migration** requirements for production-ready resilience in Kubernetes.

---

## 1. System Architecture & Extensibility

The bot must follow a **microkernel (plugin-based) architecture** to allow developers to add capabilities without altering the core Matrix connection manager, message routing systems, or core user/tier handling logic.

### 1.1 Core Components
1. **Matrix Client Core:** Handles lifecycle (login, sync loop, reconnection), joins rooms, monitors messages, and formats outgoing rich-text (HTML) messages.
2. **Plugin Manager:** Discovers, loads, and initializes plugin capabilities dynamically at runtime.
3. **Database Connector (DAL):** Provides an abstract interface for state persistence (users, rooms, subscriptions, leads, activation codes, history).
4. **Task Scheduler:** Orchestrates periodic background jobs (e.g., polling RSS feeds every 10 minutes, checking stock indicators every 5 minutes).
5. **Onboarding & User Manager:** Manages user state transitions, contact capture flow, and subscription limits (Free vs. Premium).

### 1.2 Plugin Capability Interface
Every plugin must implement a common interface exposing:
*   `plugin_id`: Unique identifier (string).
*   `commands`: List of command keywords this plugin handles.
*   `on_message(event, room)`: Method invoked when a command is matched.
*   `on_tick()`: Method invoked by the task scheduler for periodic background work.
*   `get_help()`: Detailed usage strings for the plugin.

### 1.3 Database Abstraction & Schema Migrations
To ensure compatibility across developer environments and managed production clouds, the database layer must satisfy:
*   **FR-Db-1.1: Multi-Dialect Support:** The database persistence layer must abstract SQL operations using an Object-Relational Mapper (ORM), such as SQLAlchemy. The system must support:
    *   **SQLite** (for local development and testing).
    *   **PostgreSQL** (preferred for clustered/cloud deployment).
    *   **MySQL/MariaDB** (for managed cloud database backends).
*   **FR-Db-1.2: Schema Migrations:** The database schema must be versioned and managed using a migration tool (e.g., Alembic). Schema changes must be tracked sequentially via migration scripts, allowing upgrades and downgrades without manual SQL execution.
*   **FR-Db-1.3: Startup Migrations:** The bot container must be capable of executing pending database migrations automatically at startup before accepting connections (often invoked via a readiness/entrypoint script).
*   **FR-Db-1.4: Connection Pooling & Retries:** The bot must configure robust connection pools (max overflow, pool size, recycle lifetime) and handle transient network disconnects via connection retries (especially critical for PostgreSQL/MySQL backends).

---

## 2. Chat & Bridging Requirements (FR-Chat)

### 2.1 Room & DM Support
*   **FR-Chat-1.1: Multi-Channel Execution:** The bot must be able to join and operate in multiple rooms simultaneously, listening for triggers and broadcasting alerts.
*   **FR-Chat-1.2: One-on-One DMs:** The bot must accept and handle direct-message (DM) invites, providing a private workspace for configuring personal RSS and stock subscriptions.

### 2.2 Bridging (Signal & WhatsApp)
*   **FR-Chat-2.1: Bridged Message Receipt:** The bot must process messages entering bridged rooms (e.g., rooms bridged via `mautrix-signal` or `mautrix-whatsapp`). 
*   **FR-Chat-2.2: Bridged User Identification:** The bot must identify users posting through bridges (normally mapped as virtual/ghost users like `@signal_123456789:domain.org` or `@whatsapp_123456789:domain.org`) and associate their subscriptions and lead profiles with their virtual identities so that their configuration persists across bridging boundaries.
*   **FR-Chat-2.3: Formatting Compatibility:** The bot must strip complex HTML elements (or fallback to plain text) for messages routed to bridged rooms that do not support rich-text Markdown (e.g. Signal and WhatsApp).

---

## 3. RSS Syndication & Relevance Filtering (FR-RSS)

### 3.1 Feed Ingestion
*   **FR-RSS-1.1: Feed Formats:** The system must parse standard RSS 1.0/2.0 and Atom feeds.
*   **FR-RSS-1.2: Polling Loop:** The bot will poll feeds periodically (default: every 15 minutes) and compare entries against the local database to avoid double-processing.

### 3.2 Advanced Relevance Engine
When a new item is fetched, the bot must evaluate it against user subscriptions. An item is considered a match if it fulfills the configured subscription filters:

1. **Keyword Filters:** Matches specific exact substrings or regular expressions in the title or content body (case-insensitive).
2. **Relevance Entities:**
   *   **Company Name/Ticker:** Detects specified company names (e.g., "Microsoft", "Tesla") or ticker symbols in parentheses (e.g., "(MSFT)", "(TSLA)").
   *   **Geography:** Matches country names, major cities, or regional keywords (e.g., "Germany", "Tokyo", "European Union").
   *   **Key Representatives:** Matches names of key executives, founders, or spokespersons (e.g., "Jensen Huang", "Tim Cook", "Sundar Pichai").

*Matching logic must support word boundaries (e.g., filtering for "India" should not match "Indiana") and handle basic alias mappings (e.g., "Google" matches "Alphabet Inc."). *

---

## 4. Stock Market Monitoring & Technical Indicators (FR-Stock)

### 4.1 Data Ingestion
*   **FR-Stock-1.1: Historical and Real-time Data:** The system must fetch historical OHLCV (Open, High, Low, Close, Volume) data for specified tickers.
*   **FR-Stock-1.2: Resolution Support:** The bot must support daily (1D) close data and intraday intervals (e.g., 1-hour, 15-minute) if supported by the stock data API.

### 4.2 Technical Analysis (TA) Signals
The bot must compute the following technical indicators:
*   **SMA (Simple Moving Average):** Calculates average price over a period $N$.
*   **EMA (Exponential Moving Average):** Calculates exponentially weighted average over a period $N$.
*   **RSI (Relative Strength Index):** Calculates standard momentum oscillator (typically 14-period).
*   **MACD (Moving Average Convergence Divergence):** Calculates MACD line, signal line, and histogram (typically 12, 26, 9 periods).
*   **Bollinger Bands:** Calculates simple moving average with upper and lower bands set at $K$ standard deviations (typically 20 periods, 2 standard deviations).

### 4.3 Alert Trigger Evaluation
*   **FR-Stock-3.1: Trigger Conditions:** Users can subscribe to trigger conditions:
    *   `ABOVE`: Indicator crosses or stays above a value.
    *   `BELOW`: Indicator crosses or stays below a value.
    *   `CROSS_ABOVE`: Current period's indicator crosses above a second indicator or threshold line (e.g., MACD crossing above signal line).
    *   `CROSS_BELOW`: Current period's indicator crosses below a second indicator or threshold line.
*   **FR-Stock-3.2: Alert Cooldown:** To prevent alert storms (e.g., when RSI bounces around 30 repeatedly), an alert must only trigger once per candle period (e.g., once per day for 1D resolution, or once per hour for 1H resolution) or adhere to a user-configured cooldown period (default: 4 hours).

---

## 5. Productization, Lead Capture & Licensing (FR-Product)

### 5.1 Interactive Lead Onboarding
*   **FR-Product-1.1: Welcome Trigger:** When a user sends their first message in a DM room (or when the bot is invited by a new user), the bot must intercept it and initiate an interactive welcome questionnaire.
*   **FR-Product-1.2: Information Capture:** The bot must sequentially prompt the user for:
    1.  Full Name
    2.  Company Name
    3.  Professional Email Address
    4.  Consent to store data and contact them.
*   **FR-Product-1.3: Access Lockdown:** Until onboarding is successfully completed, the user's account state is marked as `PENDING` and they cannot run RSS or stock commands.

### 5.2 Contact Metadata Harvesting
*   **FR-Product-2.1: Automatic Extraction:** In addition to manual onboarding inputs, the bot must extract and log:
    *   Matrix User ID (e.g., `@name:matrix.org`).
    *   Display Name (from Matrix profile).
    *   Bridge platform source (e.g., `Signal`, `WhatsApp`, `Matrix`) inferred from the username syntax.
    *   Contact numbers (e.g., phone number parsed from Signal/WhatsApp virtual IDs where available).

### 5.3 Administrative Notifications & Management
*   **FR-Product-3.1: Admin Alerts:** The bot must post a real-time notification to a configured private Administrator Matrix room whenever a new user completes onboarding, detailing all captured contact info.
*   **FR-Product-3.2: Admin Commands:** Administrators (identifiable by user IDs in a configurable `admin_list`) must have access to:
    *   `!admin leads` - Lists or exports all collected contacts/leads.
    *   `!admin set_tier <user_id> <free|premium>` - Manually elevates or demotes a user's subscription tier.
    *   `!admin codes` - Generates unique activation keys to be sent to paying users.

### 5.4 Subscription Tiers (Paywall)
*   **FR-Product-4.1: Tier Limits:**
    *   `Free` Tier: A user/room may only have a maximum of **2 active subscriptions** in total (e.g., 1 RSS filter and 1 Stock trigger, or 2 RSS filters).
    *   `Premium` Tier: Unlimited subscriptions.
*   **FR-Product-4.2: License Activation:**
    *   Users can type `!activate <code>` to upgrade their account to the Premium tier.
    *   The code must be verified against unused activation keys in the database.
*   **FR-Product-4.3: HTTP Webhook Integration (Optional future enhancement):** Expose an HTTP listener endpoint that can receive a web hook (e.g. from Stripe Checkout completion) to automatically upgrade a user or generate a premium code.

### 5.5 Privacy Compliance
*   **FR-Product-5.1: Right to be Forgotten:** To comply with GDPR/privacy standards, the bot must support the `!forgetme` command, which permanently deletes the user's name, email, company, and all active alerts from the database.

---

## 6. Deployment & Operations (FR-Ops)

### 6.1 Containerization
*   **FR-Ops-1.1: Dockerization:** The bot application must be buildable as a standard Docker/OCI container image. The container must run as a non-root user (security best practices).

### 6.2 Configuration & Secrets Management
*   **FR-Ops-2.1: 12-Factor Environment Configuration:** All configuration settings, access tokens, credentials, and API keys must be loaded exclusively from environment variables. No secrets or configurations may be hardcoded or written to persistent files within the image.
*   **FR-Ops-2.2: Required Environment Variables:**
    *   `MATRIX_HOMESERVER`: The homeserver URL (e.g., `https://matrix.org`).
    *   `MATRIX_USER_ID`: The bot's Matrix user ID.
    *   `MATRIX_ACCESS_TOKEN` / `MATRIX_PASSWORD`: Authentication credentials.
    *   `DATABASE_URL`: Connection URL. For SQLite, `sqlite+aiosqlite:////data/bot.db` (for async). For PostgreSQL, `postgresql+asyncpg://user:pass@host:port/dbname`.
    *   `STOCK_API_KEY`: API token for market data.
    *   `ADMIN_ROOM_ID`: The ID of the private administrator room.
    *   `ADMIN_USERS`: Comma-separated list of admin Matrix IDs.

### 6.3 Kubernetes Integration
*   **FR-Ops-3.1: Health Probes:** The bot must expose endpoint logic for:
    *   `Liveness Probe`: Indicates whether the bot process is running and the Matrix event listener loop is active.
    *   `Readiness Probe`: Indicates whether the bot has successfully established database connections, run pending migrations, and connected to the Matrix homeserver.
    *   *Implementation:* Can be done via a local lightweight HTTP port (e.g., port `8080` exposing `/healthz/live` and `/healthz/ready`) or via file-based heartbeats.
*   **FR-Ops-3.2: Persistent Volume (SQLite support):** When SQLite is used, the directory specified in `DATABASE_URL` must support mounting a Kubernetes `PersistentVolume` via a `PersistentVolumeClaim` (PVC) to preserve database state across pod recreation.

### 6.4 Operational Resiliency
*   **FR-Ops-4.1: Graceful Shutdown:** The bot must intercept termination signals (`SIGTERM` and `SIGINT`) and shut down gracefully within the Kubernetes termination grace period (default 30 seconds). It must stop the RSS/Stock polling loops, close all active database connections cleanly, and log a shutdown confirmation.
*   **FR-Ops-4.2: Logging:** All logs must write to standard output (`stdout`) and standard error (`stderr`) using a structured format (JSON logging preferred) to allow integration with Kubernetes log forwarders (e.g., FluentBit, Promtail, Loki).

---

## 7. Natural Language Interface & Intent Routing (FR-NL)

### 7.1 Natural Language Processing & Command Routing
*   **FR-NL-1.1: Conversational Command Translation:** When the bot receives a natural language message in a DM room (or when mentioned in a public room) that does not start with the strict command prefix (`!`), the bot must be capable of translating the user's intent into a structured bot command (e.g., translating "Alert me if SAP RSI 14 drops below 30" into `!stock subscribe SAP.DE RSI 14 BELOW 30`).
*   **FR-NL-1.2: Conversational Fallback:** If the user's natural language input does not map to any structured command intent (e.g., greeting the bot or asking general questions), the bot must reply with a natural conversational response rather than an error or usage helper.
*   **FR-NL-1.3: Visual Interpretation Feedback:** When a natural language input is successfully parsed as a structured command, the bot must notify the user of the interpreted command format (e.g., posting a notice like `*(Interpreted: !stock check SAP.DE)*`) before outputting the command results, ensuring transparency.

### 7.2 Integration and Fallbacks
*   **FR-NL-2.1: Gemini API Gateway:** The intent translator must interface with the Google Gemini API (e.g., `gemini-2.5-flash`) via an asynchronous HTTP POST handler using `httpx`.
*   **FR-NL-2.2: API Key Configuration:** The router must be enabled dynamically by checking for a `GEMINI_API_KEY` configuration setting in the environment. If the API key is not configured, the bot must fallback to ignoring normal text messages and only responding to explicit `!` command structures, ensuring backwards compatibility and off-line functionality.

