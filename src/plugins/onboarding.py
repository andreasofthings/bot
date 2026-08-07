import uuid
import re
from datetime import datetime
from typing import List, Optional
from nio import AsyncClient, MatrixRoom, RoomMessageText
from src.core.plugin import Plugin
from src.config import load_settings
from src.core.database import get_db_session
from src.models.user import User, LicenseCode
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Basic email validation regex
EMAIL_REGEX = re.compile(r"^[^@]+@[^@]+\.[^@]+$")


async def send_rich_message(client: AsyncClient, room_id: str, plain: str, html: str) -> None:
    """Helper to send a Matrix message with both plain text and rich HTML fallbacks."""
    await client.room_send(
        room_id=room_id,
        message_type="m.room.message",
        content={
            "msgtype": "m.text",
            "format": "org.matrix.custom.html",
            "body": plain,
            "formatted_body": html
        }
    )


async def handle_onboarding_message(
    client: AsyncClient, 
    room: MatrixRoom, 
    event: RoomMessageText, 
    user: Optional[User], 
    platform: str
) -> None:
    """Executes the step-by-step user onboarding flow (lead capture state machine)."""
    settings = load_settings()
    body = event.body.strip()
    
    async with get_db_session() as session:
        # 1. State: Brand new user (User record doesn't exist yet)
        if user is None:
            new_user = User(
                user_id=event.sender,
                onboarding_state="ASKED_NAME",
                consent_given=False,
                tier="FREE"
            )
            session.add(new_user)
            
            html = (
                "<h3>Welcome to the Matrix Multi-Capability Bot! 🤖</h3>"
                "Before we can activate your access, I need to collect some basic contact information.<br><br>"
                "To get started, <b>what is your full name?</b>"
            )
            plain = (
                "Welcome to the Matrix Multi-Capability Bot! 🤖\n"
                "Before we can activate your access, I need to collect some basic contact information.\n\n"
                "To get started, what is your full name?"
            )
            await send_rich_message(client, room.room_id, plain, html)
            return

        # Fetch user in active transaction session
        db_user = await session.get(User, user.user_id)
        if not db_user:
            return

        # 2. State: PENDING (Restart onboarding)
        if db_user.onboarding_state == "PENDING":
            db_user.onboarding_state = "ASKED_NAME"
            html = "Let's restart the registration. <b>What is your full name?</b>"
            plain = "Let's restart the registration. What is your full name?"
            await send_rich_message(client, room.room_id, plain, html)
            return

        # 3. State: ASKED_NAME -> Save name, Ask company
        elif db_user.onboarding_state == "ASKED_NAME":
            db_user.name = body
            db_user.onboarding_state = "ASKED_COMPANY"
            html = f"Thanks, <b>{db_user.name}</b>! Next, <b>which company or organization do you represent?</b>"
            plain = f"Thanks, {db_user.name}! Next, which company or organization do you represent?"
            await send_rich_message(client, room.room_id, plain, html)
            return

        # 4. State: ASKED_COMPANY -> Save company, Ask email
        elif db_user.onboarding_state == "ASKED_COMPANY":
            db_user.company = body
            db_user.onboarding_state = "ASKED_EMAIL"
            html = "Got it. <b>What is your professional email address?</b>"
            plain = "Got it. What is your professional email address?"
            await send_rich_message(client, room.room_id, plain, html)
            return

        # 5. State: ASKED_EMAIL -> Validate and save email, Ask consent
        elif db_user.onboarding_state == "ASKED_EMAIL":
            email = body.lower()
            if not EMAIL_REGEX.match(email):
                html = "❌ <i>That email format looks incorrect.</i> Please enter a valid email (e.g., name@company.com):"
                plain = "That email format looks incorrect. Please enter a valid email (e.g., name@company.com):"
                await send_rich_message(client, room.room_id, plain, html)
                return
            
            db_user.email = email
            db_user.onboarding_state = "ASKED_CONSENT"
            html = (
                f"Thank you. Lastly, do you consent to us storing your contact details "
                f"and sending you notifications? (Reply <b>yes</b> or <b>no</b>)"
            )
            plain = (
                "Thank you. Lastly, do you consent to us storing your contact details "
                "and sending you notifications? (Reply yes or no)"
            )
            await send_rich_message(client, room.room_id, plain, html)
            return

        # 6. State: ASKED_CONSENT -> Save consent, Complete onboarding
        elif db_user.onboarding_state == "ASKED_CONSENT":
            consent_reply = body.lower().strip()
            
            if consent_reply in ["yes", "y", "ja"]:
                db_user.consent_given = True
                db_user.onboarding_state = "COMPLETED"
                
                html = (
                    "🎉 <b>Registration Complete!</b><br>"
                    "Your account has been activated on the <b>Free Tier</b> (allows up to 2 active alerts/subscriptions).<br><br>"
                    "Type <b>!help</b> to view available commands."
                )
                plain = (
                    "Registration Complete!\n"
                    "Your account has been activated on the Free Tier (allows up to 2 active alerts/subscriptions).\n\n"
                    "Type !help to view available commands."
                )
                await send_rich_message(client, room.room_id, plain, html)

                # Send Admin Notification Alert
                if settings.admin_room_id:
                    admin_html = (
                        f"📢 <b>New Lead Registered!</b><br>"
                        f"<ul>"
                        f"<li><b>Matrix ID:</b> <code>{db_user.user_id}</code></li>"
                        f"<li><b>Name:</b> {db_user.name}</li>"
                        f"<li><b>Company:</b> {db_user.company}</li>"
                        f"<li><b>Email:</b> {db_user.email}</li>"
                        f"<li><b>Platform:</b> {platform}</li>"
                        f"</ul>"
                    )
                    admin_plain = (
                        f"New Lead Registered!\n"
                        f"- User ID: {db_user.user_id}\n"
                        f"- Name: {db_user.name}\n"
                        f"- Company: {db_user.company}\n"
                        f"- Email: {db_user.email}\n"
                        f"- Platform: {platform}"
                    )
                    try:
                        await send_rich_message(client, settings.admin_room_id, admin_plain, admin_html)
                    except Exception as e:
                        logger.error("Failed to notify admin room of new lead", error=str(e))
                return
            
            elif consent_reply in ["no", "n", "nein"]:
                db_user.onboarding_state = "PENDING"
                html = (
                    "⚠️ <b>Consent is required</b> to use the bot services.<br>"
                    "Your contact data has not been stored. Type anything to restart onboarding."
                )
                plain = (
                    "Consent is required to use the bot services.\n"
                    "Your contact data has not been stored. Type anything to restart onboarding."
                )
                await send_rich_message(client, room.room_id, plain, html)
                return
            
            else:
                html = "Please answer <b>yes</b> or <b>no</b>:"
                plain = "Please answer yes or no:"
                await send_rich_message(client, room.room_id, plain, html)
                return


class OnboardingPlugin(Plugin):
    """Plugin managing user licensing, data deletion, and administrator dashboard utilities."""

    @property
    def plugin_id(self) -> str:
        return "onboarding"

    @property
    def commands(self) -> List[str]:
        return ["activate", "forgetme", "admin"]

    async def on_message(
        self, 
        client: AsyncClient, 
        room: MatrixRoom, 
        event: RoomMessageText, 
        command: str, 
        args: List[str]
    ) -> None:
        cmd = command.lower()
        settings = load_settings()
        
        # 1. Command: !activate <code>
        if cmd == "activate":
            if not args:
                await send_rich_message(
                    client, room.room_id, 
                    "Usage: !activate <code>", 
                    "Usage: <code>!activate &lt;code&gt;</code>"
                )
                return
            
            code_str = args[0].strip()
            
            async with get_db_session() as session:
                # Query code
                code_record = await session.get(LicenseCode, code_str)
                if not code_record or code_record.is_used:
                    html = "❌ <b>Activation Failed:</b> Invalid or already used license code."
                    plain = "Activation Failed: Invalid or already used license code."
                    await send_rich_message(client, room.room_id, plain, html)
                    return
                
                # Retrieve user record
                user_record = await session.get(User, event.sender)
                if not user_record:
                    # Fallback if user bypasses onboarding check somehow
                    user_record = User(user_id=event.sender, onboarding_state="COMPLETED")
                    session.add(user_record)
                
                # Consume code and upgrade user
                code_record.is_used = True
                code_record.used_by = event.sender
                code_record.used_at = datetime.now()
                user_record.tier = "PREMIUM"
                
                html = "🎉 <b>Premium Tier Activated!</b> Your account has been upgraded to Premium. You have unlimited subscriptions."
                plain = "Premium Tier Activated! Your account has been upgraded to Premium. You have unlimited subscriptions."
                await send_rich_message(client, room.room_id, plain, html)
                return

        # 2. Command: !forgetme (GDPR compliance)
        elif cmd == "forgetme":
            async with get_db_session() as session:
                user_record = await session.get(User, event.sender)
                if user_record:
                    await session.delete(user_record)
                    html = "🗑️ <b>Data Deleted:</b> All your personal contact records and alert subscriptions have been permanently erased."
                    plain = "Data Deleted: All your personal contact records and alert subscriptions have been permanently erased."
                else:
                    html = "No user record found for your account."
                    plain = "No user record found for your account."
                    
                await send_rich_message(client, room.room_id, plain, html)
                return

        # 3. Command: !admin (Admin only)
        elif cmd == "admin":
            if event.sender not in settings.admin_users_list:
                await send_rich_message(
                    client, room.room_id, 
                    "Permission Denied: Admin privileges required.", 
                    "❌ <b>Permission Denied:</b> Admin privileges required."
                )
                return
            
            if not args:
                await send_rich_message(
                    client, room.room_id,
                    "Admin commands: !admin leads, !admin codes, !admin set_tier <user_id> <free|premium>",
                    "<b>Admin Panel Subcommands:</b><ul>"
                    "<li><code>!admin leads</code>: Lists all captured leads</li>"
                    "<li><code>!admin codes</code>: Generates a premium activation code</li>"
                    "<li><code>!admin set_tier &lt;user_id&gt; &lt;free|premium&gt;</code>: Forces user tier</li>"
                    "</ul>"
                )
                return
            
            sub_cmd = args[0].lower()
            
            # Subcommand: !admin leads
            if sub_cmd in ["leads", "list"]:
                from sqlalchemy import select
                async with get_db_session() as session:
                    result = await session.execute(select(User))
                    users = result.scalars().all()
                    
                    if not users:
                        await send_rich_message(client, room.room_id, "No registered users in database.", "No registered users in database.")
                        return
                    
                    html_lines = ["<h4>Captured Leads</h4><table border='1'><tr><th>User ID</th><th>Name</th><th>Company</th><th>Email</th><th>Tier</th></tr>"]
                    plain_lines = ["Captured Leads:"]
                    for u in users:
                        html_lines.append(f"<tr><td>{u.user_id}</td><td>{u.name or ''}</td><td>{u.company or ''}</td><td>{u.email or ''}</td><td>{u.tier}</td></tr>")
                        plain_lines.append(f"- {u.user_id} | Name: {u.name} | Co: {u.company} | Email: {u.email} | Tier: {u.tier}")
                    html_lines.append("</table>")
                    
                    await send_rich_message(client, room.room_id, "\n".join(plain_lines), "".join(html_lines))
                    return
            
            # Subcommand: !admin codes
            elif sub_cmd == "codes":
                code_str = f"PREM-{str(uuid.uuid4())[:8].upper()}-{str(uuid.uuid4())[:8].upper()}"
                async with get_db_session() as session:
                    new_code = LicenseCode(code=code_str, is_used=False)
                    session.add(new_code)
                    
                    html = f"🔑 <b>New Premium Activation Key Generated:</b> <code>{code_str}</code>"
                    plain = f"New Premium Activation Key Generated: {code_str}"
                    await send_rich_message(client, room.room_id, plain, html)
                    return

            # Subcommand: !admin set_tier <user_id> <free|premium>
            elif sub_cmd == "set_tier":
                if len(args) < 3:
                    await send_rich_message(
                        client, room.room_id, 
                        "Usage: !admin set_tier <user_id> <free|premium>", 
                        "Usage: <code>!admin set_tier &lt;user_id&gt; &lt;free|premium&gt;</code>"
                    )
                    return
                
                target_user_id = args[1].strip()
                target_tier = args[2].upper().strip()
                
                if target_tier not in ["FREE", "PREMIUM"]:
                    await send_rich_message(client, room.room_id, "Tier must be FREE or PREMIUM.", "Tier must be FREE or PREMIUM.")
                    return
                
                async with get_db_session() as session:
                    user_record = await session.get(User, target_user_id)
                    if not user_record:
                        await send_rich_message(client, room.room_id, f"User {target_user_id} not found.", f"User {target_user_id} not found.")
                        return
                    
                    user_record.tier = target_tier
                    html = f"Success: User <code>{target_user_id}</code> licensing tier updated to <b>{target_tier}</b>."
                    plain = f"Success: User {target_user_id} licensing tier updated to {target_tier}."
                    await send_rich_message(client, room.room_id, plain, html)
                    return

    def get_help(self) -> str:
        return (
            "• <b>!activate &lt;code&gt;</b>: Activates a licensing key to upgrade your account to Premium.<br>"
            "• <b>!forgetme</b>: Wipes all your personal records and subscriptions (GDPR compliance).<br>"
            "• <b>!admin &lt;command&gt;</b>: (Admin only) Lists leads, changes user tiers, or generates license codes."
        )
