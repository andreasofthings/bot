import time
from typing import List, Optional, Dict, Any, Tuple
import httpx
from sqlalchemy import select, update
from nio import AsyncClient, MatrixRoom, RoomMessageText

from src.core.plugin import Plugin
from src.core.database import get_db_session
from src.models.hubspot import HubSpotConnection
from src.config import load_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

HUBSPOT_API_BASE = "https://api.hubapi.com"


def mask_token(token: str) -> str:
    """Masks sensitive access token for safe chat display."""
    if not token:
        return "None"
    if len(token) <= 10:
        return "****"
    return f"{token[:8]}****...{token[-4:]}"


async def send_rich_message(client: AsyncClient, room_id: str, plain: str, html: str) -> None:
    """Sends a rich HTML and fallback plain text message to a Matrix room."""
    await client.room_send(
        room_id=room_id,
        message_type="m.room.message",
        content={
            "msgtype": "m.text",
            "format": "org.matrix.custom.html",
            "body": plain,
            "formatted_body": html,
        },
    )


class HubSpotAuthService:
    """Handles communication with HubSpot's authentication and account APIs."""

    @staticmethod
    async def verify_token(token: str) -> Tuple[bool, Dict[str, Any], Optional[str]]:
        """Verifies a HubSpot Private App access token and retrieves account diagnostics.
        
        Returns:
            (is_valid, diagnostics_dict, error_message)
        """
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "MatrixHubSpotBot/1.0",
        }

        diagnostics: Dict[str, Any] = {
            "token_masked": mask_token(token),
            "portal_id": None,
            "account_type": None,
            "time_zone": None,
            "currency": None,
            "data_hosting_location": None,
            "user": None,
            "hub_domain": None,
            "scopes": [],
        }

        start_time = time.time()

        try:
            async with httpx.AsyncClient(timeout=10.0) as http_client:
                # 1. Query Account Information Details
                acc_resp = await http_client.get(
                    f"{HUBSPOT_API_BASE}/account-info/v3/details",
                    headers=headers,
                )
                latency_ms = int((time.time() - start_time) * 1000)
                diagnostics["latency_ms"] = latency_ms

                if acc_resp.status_code == 200:
                    data = acc_resp.json()
                    diagnostics["portal_id"] = str(data.get("portalId", ""))
                    diagnostics["account_type"] = data.get("accountType")
                    diagnostics["time_zone"] = data.get("timeZone")
                    diagnostics["currency"] = data.get("companyCurrency")
                    diagnostics["data_hosting_location"] = data.get("dataHostingLocation")
                elif acc_resp.status_code in (401, 403):
                    error_json = acc_resp.json() if acc_resp.headers.get("content-type", "").startswith("application/json") else {}
                    err_msg = error_json.get("message", f"HTTP {acc_resp.status_code}: Unauthorized / Invalid Token")
                    return False, diagnostics, err_msg

                # 2. Query OAuth Token Details (for scopes, user, and hub domain)
                try:
                    token_resp = await http_client.get(
                        f"{HUBSPOT_API_BASE}/oauth/v1/access-tokens/{token}",
                        headers=headers,
                    )
                    if token_resp.status_code == 200:
                        token_data = token_resp.json()
                        diagnostics["user"] = token_data.get("user")
                        diagnostics["hub_domain"] = token_data.get("hub_domain")
                        diagnostics["scopes"] = token_data.get("scopes", [])
                        if not diagnostics["portal_id"]:
                            diagnostics["portal_id"] = str(token_data.get("hub_id", ""))
                except Exception as scope_err:
                    logger.debug("Failed to retrieve token details endpoint", error=str(scope_err))

                # 3. If portal_id was obtained or account-info was 200, authentication is verified
                if diagnostics["portal_id"]:
                    return True, diagnostics, None

                # Fallback CRM test probe if account-info didn't populate portalId
                crm_resp = await http_client.get(
                    f"{HUBSPOT_API_BASE}/crm/v3/objects/contacts?limit=1",
                    headers=headers,
                )
                if crm_resp.status_code in (200, 404):
                    return True, diagnostics, None
                elif crm_resp.status_code in (401, 403):
                    error_json = crm_resp.json() if crm_resp.headers.get("content-type", "").startswith("application/json") else {}
                    return False, diagnostics, error_json.get("message", "Invalid or unauthorized access token.")

                return False, diagnostics, f"Unexpected response from HubSpot API: HTTP {acc_resp.status_code}"

        except httpx.RequestError as exc:
            logger.error("Network error while verifying HubSpot token", error=str(exc))
            return False, diagnostics, f"Connection to HubSpot API failed: {exc}"
        except Exception as exc:
            logger.exception("Unexpected error during HubSpot token verification", error=str(exc))
            return False, diagnostics, f"Internal error during verification: {exc}"


class HubSpotPlugin(Plugin):
    """HubSpot CRM integration plugin.
    
    Provides authentication, connection testing, and account diagnostics for HubSpot CRM.
    """

    def __init__(self):
        self.auth_service = HubSpotAuthService()

    @property
    def plugin_id(self) -> str:
        return "hubspot"

    @property
    def commands(self) -> List[str]:
        return ["hubspot", "crm"]

    async def _get_active_token(self) -> Tuple[Optional[str], str]:
        """Retrieves active access token, checking DB first then environment settings.
        
        Returns:
            (access_token, source_description)
        """
        # 1. Check database for active stored connection
        async with get_db_session() as session:
            stmt = select(HubSpotConnection).where(HubSpotConnection.is_active == True).order_by(HubSpotConnection.updated_at.desc())
            result = await session.execute(stmt)
            conn = result.scalars().first()
            if conn and conn.access_token:
                return conn.access_token, f"Database (Portal {conn.portal_id})"

        # 2. Check environment configuration
        settings = load_settings()
        if settings.hubspot_access_token:
            return settings.hubspot_access_token, "Environment Config (HUBSPOT_ACCESS_TOKEN)"

        return None, "Not configured"

    async def on_message(
        self,
        client: AsyncClient,
        room: MatrixRoom,
        event: RoomMessageText,
        command: str,
        args: List[str],
    ) -> None:
        """Routes HubSpot/CRM commands."""
        if not args:
            await self._handle_help(client, room.room_id)
            return

        subcmd = args[0].lower()
        subargs = args[1:]

        if subcmd in ("status", "check", "info"):
            await self._handle_status(client, room.room_id)
        elif subcmd in ("auth", "connect", "token", "login"):
            await self._handle_auth(client, room, event, subargs)
        elif subcmd in ("disconnect", "logout", "revoke"):
            await self._handle_disconnect(client, room.room_id)
        elif subcmd in ("test", "ping"):
            await self._handle_test(client, room.room_id)
        else:
            await self._handle_help(client, room.room_id)

    async def _handle_status(self, client: AsyncClient, room_id: str) -> None:
        """Checks authentication against HubSpot and presents a rich diagnostic status card."""
        token, source = await self._get_active_token()

        if not token:
            plain_msg = (
                "HubSpot CRM Status: Not Connected\n\n"
                "No active HubSpot Private App access token is configured.\n"
                "To connect:\n"
                "• Set HUBSPOT_ACCESS_TOKEN in .env, or\n"
                "• Send '!hubspot auth <token>' in a private chat."
            )
            html_msg = (
                "<h4>🟠 HubSpot CRM: Not Connected</h4>"
                "<p>No active HubSpot Private App access token was found.</p>"
                "<b>How to connect:</b>"
                "<ol>"
                "<li>Generate a <b>Private App Access Token</b> in HubSpot (Settings &gt; Integrations &gt; Private Apps).</li>"
                "<li>Configure <code>HUBSPOT_ACCESS_TOKEN</code> in <code>.env</code>, OR</li>"
                "<li>Run <code>!hubspot auth &lt;token&gt;</code> (preferably in a 1-on-1 private chat with the bot).</li>"
                "</ol>"
            )
            await send_rich_message(client, room_id, plain_msg, html_msg)
            return

        # Perform live verification with HubSpot
        is_valid, diag, err_msg = await self.auth_service.verify_token(token)

        if is_valid:
            scopes_str = ", ".join(diag["scopes"]) if diag["scopes"] else "Standard CRM Permissions"
            scopes_html = "".join([f"<li><code>{s}</code></li>" for s in diag["scopes"]]) if diag["scopes"] else "<li><i>Standard Scopes</i></li>"

            plain_msg = (
                "HubSpot CRM Status: CONNECTED\n"
                f"• Portal ID: {diag.get('portal_id') or 'N/A'}\n"
                f"• Domain: {diag.get('hub_domain') or 'N/A'}\n"
                f"• Account Type: {diag.get('account_type') or 'N/A'}\n"
                f"• Region: {diag.get('data_hosting_location') or 'N/A'}\n"
                f"• Timezone: {diag.get('time_zone') or 'N/A'}\n"
                f"• Currency: {diag.get('currency') or 'N/A'}\n"
                f"• Associated User: {diag.get('user') or 'Private App'}\n"
                f"• Token: {diag.get('token_masked')}\n"
                f"• Source: {source}\n"
                f"• Latency: {diag.get('latency_ms', 0)}ms\n"
                f"• Scopes: {scopes_str}"
            )
            html_msg = (
                "<h4>🟢 HubSpot CRM: Authenticated &amp; Connected</h4>"
                "<table border='1' cellpadding='4' cellspacing='0'>"
                f"<tr><td><b>Status</b></td><td><span style='color:green;'><b>Authenticated</b></span></td></tr>"
                f"<tr><td><b>Portal ID</b></td><td><code>{diag.get('portal_id') or 'N/A'}</code></td></tr>"
                f"<tr><td><b>Hub Domain</b></td><td>{diag.get('hub_domain') or 'N/A'}</td></tr>"
                f"<tr><td><b>Account Type</b></td><td>{diag.get('account_type') or 'N/A'}</td></tr>"
                f"<tr><td><b>Hosting Region</b></td><td><code>{diag.get('data_hosting_location') or 'N/A'}</code></td></tr>"
                f"<tr><td><b>Timezone</b></td><td>{diag.get('time_zone') or 'N/A'}</td></tr>"
                f"<tr><td><b>Currency</b></td><td>{diag.get('currency') or 'N/A'}</td></tr>"
                f"<tr><td><b>User / App</b></td><td>{diag.get('user') or 'Private App'}</td></tr>"
                f"<tr><td><b>Active Token</b></td><td><code>{diag.get('token_masked')}</code></td></tr>"
                f"<tr><td><b>Source</b></td><td>{source}</td></tr>"
                f"<tr><td><b>Ping Latency</b></td><td>{diag.get('latency_ms', 0)} ms</td></tr>"
                "</table>"
                f"<br><b>Granted Scopes:</b><ul>{scopes_html}</ul>"
            )
            await send_rich_message(client, room_id, plain_msg, html_msg)
        else:
            plain_msg = (
                "HubSpot CRM Status: AUTHENTICATION FAILED\n"
                f"• Source: {source}\n"
                f"• Token: {diag.get('token_masked')}\n"
                f"• Error: {err_msg}\n\n"
                "Please verify or renew your HubSpot Private App token via '!hubspot auth <token>'."
            )
            html_msg = (
                "<h4>🔴 HubSpot CRM: Authentication Failed</h4>"
                f"<p><span style='color:red;'><b>Error:</b> {err_msg}</span></p>"
                "<table border='1' cellpadding='4' cellspacing='0'>"
                f"<tr><td><b>Configured Token</b></td><td><code>{diag.get('token_masked')}</code></td></tr>"
                f"<tr><td><b>Source</b></td><td>{source}</td></tr>"
                "</table>"
                "<p>To update the token, run <code>!hubspot auth &lt;new_token&gt;</code>.</p>"
            )
            await send_rich_message(client, room_id, plain_msg, html_msg)

    async def _handle_auth(
        self,
        client: AsyncClient,
        room: MatrixRoom,
        event: RoomMessageText,
        args: List[str],
    ) -> None:
        """Connects and stores a new HubSpot access token after live verification."""
        if not args:
            plain_msg = "Usage: !hubspot auth <pat-... access token>"
            html_msg = "<b>Usage:</b> <code>!hubspot auth &lt;access_token&gt;</code>"
            await send_rich_message(client, room.room_id, plain_msg, html_msg)
            return

        raw_token = args[0].strip()

        # Security check: If in a public channel with multiple participants, warn the user
        is_dm = len(room.users) <= 2
        security_warning = ""
        if not is_dm:
            security_warning = (
                "<p>⚠️ <b>Security Notice:</b> You shared an access token in a multi-user room. "
                "For better security, consider creating/updating tokens in a private direct message (DM) with the bot.</p>"
            )

        # 1. Verify token with HubSpot API
        is_valid, diag, err_msg = await self.auth_service.verify_token(raw_token)

        if not is_valid:
            plain_msg = (
                f"HubSpot Authentication Failed!\n"
                f"HubSpot API rejected this token: {err_msg}\n"
                "The token has NOT been saved."
            )
            html_msg = (
                "<h4>❌ HubSpot Authentication Failed</h4>"
                f"<p><span style='color:red;'><b>Error:</b> {err_msg}</span></p>"
                "<p>The token was rejected by HubSpot and has <b>not</b> been saved. Please check your Private App token and try again.</p>"
                f"{security_warning}"
            )
            await send_rich_message(client, room.room_id, plain_msg, html_msg)
            return

        portal_id = diag.get("portal_id") or "unknown"
        scopes_text = ",".join(diag.get("scopes", []))

        # 2. Persist to database
        async with get_db_session() as session:
            # Deactivate any previous connections
            await session.execute(
                update(HubSpotConnection).values(is_active=False)
            )

            # Insert or update connection for this portal
            stmt = select(HubSpotConnection).where(HubSpotConnection.portal_id == portal_id)
            result = await session.execute(stmt)
            existing = result.scalars().first()

            if existing:
                existing.access_token = raw_token
                existing.account_name = diag.get("hub_domain") or diag.get("user")
                existing.scopes = scopes_text
                existing.data_hosting_location = diag.get("data_hosting_location")
                existing.timezone = diag.get("time_zone")
                existing.currency = diag.get("currency")
                existing.is_active = True
                existing.connected_by = event.sender
            else:
                new_conn = HubSpotConnection(
                    portal_id=portal_id,
                    access_token=raw_token,
                    account_name=diag.get("hub_domain") or diag.get("user"),
                    scopes=scopes_text,
                    data_hosting_location=diag.get("data_hosting_location"),
                    timezone=diag.get("time_zone"),
                    currency=diag.get("currency"),
                    is_active=True,
                    connected_by=event.sender,
                )
                session.add(new_conn)

        logger.info(
            "HubSpot connection authenticated and saved",
            portal_id=portal_id,
            user=event.sender,
            scopes_count=len(diag.get("scopes", [])),
        )

        plain_msg = (
            f"HubSpot Connected Successfully!\n"
            f"• Portal ID: {portal_id}\n"
            f"• Account: {diag.get('hub_domain') or diag.get('user') or 'HubSpot Private App'}\n"
            f"• Region: {diag.get('data_hosting_location') or 'Default'}\n"
            f"• Token: {diag.get('token_masked')}\n"
            f"• Status: Authenticated and Ready"
        )
        html_msg = (
            "<h4>🎉 HubSpot Connected Successfully!</h4>"
            "<p>Your HubSpot credentials have been verified and saved.</p>"
            "<table border='1' cellpadding='4' cellspacing='0'>"
            f"<tr><td><b>Portal ID</b></td><td><code>{portal_id}</code></td></tr>"
            f"<tr><td><b>Hub Domain</b></td><td>{diag.get('hub_domain') or 'N/A'}</td></tr>"
            f"<tr><td><b>Region</b></td><td><code>{diag.get('data_hosting_location') or 'N/A'}</code></td></tr>"
            f"<tr><td><b>Timezone / Currency</b></td><td>{diag.get('time_zone') or 'N/A'} / {diag.get('currency') or 'N/A'}</td></tr>"
            f"<tr><td><b>Connected By</b></td><td>{event.sender}</td></tr>"
            f"<tr><td><b>Active Token</b></td><td><code>{diag.get('token_masked')}</code></td></tr>"
            "</table>"
            f"{security_warning}"
        )
        await send_rich_message(client, room.room_id, plain_msg, html_msg)

    async def _handle_disconnect(self, client: AsyncClient, room_id: str) -> None:
        """Deactivates active HubSpot credentials in the database."""
        async with get_db_session() as session:
            stmt = update(HubSpotConnection).where(HubSpotConnection.is_active == True).values(is_active=False)
            result = await session.execute(stmt)
            count = result.rowcount

        logger.info("HubSpot credentials disconnected", rows_affected=count)

        settings = load_settings()
        env_note = ""
        if settings.hubspot_access_token:
            env_note = "<br><i>Note: An environment token is still defined in <code>HUBSPOT_ACCESS_TOKEN</code>. Remove it from <code>.env</code> to completely revoke access.</i>"

        plain_msg = "HubSpot CRM credentials have been disconnected and deactivated."
        html_msg = (
            "<h4>🔌 HubSpot Disconnected</h4>"
            "<p>Active HubSpot CRM database credentials have been deactivated.</p>"
            f"{env_note}"
        )
        await send_rich_message(client, room_id, plain_msg, html_msg)

    async def _handle_test(self, client: AsyncClient, room_id: str) -> None:
        """Performs a live connectivity test to the HubSpot API."""
        token, source = await self._get_active_token()
        if not token:
            await self._handle_status(client, room_id)
            return

        start_time = time.time()
        is_valid, diag, err_msg = await self.auth_service.verify_token(token)
        latency = int((time.time() - start_time) * 1000)

        if is_valid:
            plain_msg = f"HubSpot API Ping Test: OK (HTTP 200, {latency}ms latency). Portal ID: {diag.get('portal_id')}."
            html_msg = (
                "<h4>⚡ HubSpot API Ping: OK</h4>"
                f"<p>Connection verified in <b>{latency} ms</b>.</p>"
                f"<ul>"
                f"<li><b>Portal ID:</b> <code>{diag.get('portal_id')}</code></li>"
                f"<li><b>Hosting Region:</b> {diag.get('data_hosting_location') or 'Standard'}</li>"
                f"<li><b>Status:</b> Healthy</li>"
                f"</ul>"
            )
        else:
            plain_msg = f"HubSpot API Ping Test: FAILED ({latency}ms). Error: {err_msg}"
            html_msg = (
                "<h4>⚡ HubSpot API Ping: Failed</h4>"
                f"<p><span style='color:red;'><b>Error:</b> {err_msg}</span> (took {latency}ms)</p>"
            )

        await send_rich_message(client, room_id, plain_msg, html_msg)

    async def _handle_help(self, client: AsyncClient, room_id: str) -> None:
        """Sends HubSpot CRM command usage instructions."""
        plain_msg = (
            "HubSpot CRM Commands:\n"
            "- !hubspot status: Check connection status and account diagnostics\n"
            "- !hubspot auth <token>: Connect a HubSpot Private App access token\n"
            "- !hubspot test: Perform API latency ping test\n"
            "- !hubspot disconnect: Deactivate stored HubSpot credentials"
        )
        html_msg = (
            "<b>HubSpot CRM Commands:</b><ul>"
            "<li><code>!hubspot status</code>: Verifies connection and displays portal diagnostics (region, timezone, scopes).</li>"
            "<li><code>!hubspot auth &lt;token&gt;</code>: Authenticates and saves a HubSpot Private App access token (recommended in private DM).</li>"
            "<li><code>!hubspot test</code>: Runs a live latency ping against HubSpot API.</li>"
            "<li><code>!hubspot disconnect</code>: Deactivates active database credentials.</li>"
            "</ul>"
            "<i>Tip: You can also use <code>!crm</code> as an alias.</i>"
        )
        await send_rich_message(client, room_id, plain_msg, html_msg)

    def get_help(self) -> str:
        return (
            "• <b>!hubspot status</b>: Checks authentication status and displays HubSpot portal details.<br>"
            "• <b>!hubspot auth &lt;token&gt;</b>: Authenticates and connects a HubSpot Private App access token.<br>"
            "• <b>!hubspot test</b>: Pings the HubSpot API to measure response latency.<br>"
            "• <b>!hubspot disconnect</b>: Clears stored credentials.<br>"
            "<i>(Alias: <code>!crm &lt;subcommand&gt;</code>)</i>"
        )
