import asyncio
import httpx
import pandas as pd
import ta
import re
from datetime import datetime, timedelta
from typing import List, Optional, Any, Dict
from sqlalchemy import select, delete
from nio import AsyncClient, MatrixRoom, RoomMessageText
from src.core.plugin import Plugin
from src.core.database import get_db_session
from src.models.stock import StockSubscription
from src.models.user import User
from src.config import load_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Yahoo Finance headers to prevent blocking
YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


async def send_rich_message(client: AsyncClient, room_id: str, plain: str, html: str) -> None:
    """Helper to send HTML-formatted Matrix messages."""
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


class StockPlugin(Plugin):
    """Stock Market Monitoring plugin. Evaluates Technical Indicators and alerts users on threshold crossings."""

    def __init__(self):
        self._last_tick_time = 0
        self._poll_interval_seconds = 14400  # Poll stocks once every 4 hours default
        self._cooldown_hours = 24  # Alert cooldown hours

    @property
    def plugin_id(self) -> str:
        return "stock"

    @property
    def commands(self) -> List[str]:
        return ["stock"]

    async def on_message(
        self, 
        client: AsyncClient, 
        room: MatrixRoom, 
        event: RoomMessageText, 
        command: str, 
        args: List[str]
    ) -> None:
        if not args:
            await self._send_usage(client, room.room_id)
            return

        sub_cmd = args[0].lower()

        # 1. Command: !stock list
        if sub_cmd == "list":
            await self._handle_list(client, room, event)
            return

        # 2. Command: !stock subscribe <ticker> <indicator> <param_1> <condition> <threshold>
        elif sub_cmd == "subscribe":
            await self._handle_subscribe(client, room, event, args[1:])
            return

        # 3. Command: !stock unsubscribe <id_or_ticker>
        elif sub_cmd == "unsubscribe":
            await self._handle_unsubscribe(client, room, event, args[1:])
            return

        # 4. Command: !stock check <ticker> [indicator] [param_1]
        elif sub_cmd == "check":
            await self._handle_check(client, room, event, args[1:])
            return

        else:
            await self._send_usage(client, room.room_id)

    async def _handle_list(self, client: AsyncClient, room: MatrixRoom, event: RoomMessageText) -> None:
        """Lists active stock alert subscriptions for the current context."""
        # Find context room ID or DM user ID
        subscriber_id = room.canonical_alias if room.canonical_alias else room.room_id
        if len(room.users) <= 2:
            subscriber_id = event.sender

        async with get_db_session() as session:
            q = select(StockSubscription).where(
                (StockSubscription.room_id == room.room_id) | 
                (StockSubscription.user_id == event.sender)
            )
            res = await session.execute(q)
            subs = res.scalars().all()

            if not subs:
                await send_rich_message(client, room.room_id, "No active stock alerts found.", "No active stock alerts found.")
                return

            html_lines = ["<b>Stock Alerts configured in this room:</b><ul>"]
            plain_lines = ["Stock Alerts configured in this room:"]
            for s in subs:
                details = f"{s.ticker} {s.indicator}({s.parameter_1}) {s.condition_type} {s.threshold}"
                html_lines.append(f"<li>ID {s.id} - <code>{details}</code> (configured by <code>{s.user_id}</code>)</li>")
                plain_lines.append(f"  ID {s.id} - {details} (configured by {s.user_id})")
            html_lines.append("</ul>")

            await send_rich_message(client, room.room_id, "\n".join(plain_lines), "".join(html_lines))

    async def _handle_subscribe(self, client: AsyncClient, room: MatrixRoom, event: RoomMessageText, sub_args: List[str]) -> None:
        """Parses and validates a new stock alert subscription."""
        settings = load_settings()
        if len(sub_args) < 5:
            await send_rich_message(
                client, room.room_id,
                "Usage: !stock subscribe <ticker> <indicator> <param_1> <condition> <threshold>",
                "Usage: <code>!stock subscribe &lt;ticker&gt; &lt;indicator&gt; &lt;param_1&gt; &lt;condition&gt; &lt;threshold&gt;</code>"
            )
            return

        ticker = sub_args[0].upper().strip()
        indicator = sub_args[1].upper().strip()
        
        try:
            param_1 = int(sub_args[2])
            condition = sub_args[3].upper().strip()
            threshold = float(sub_args[4])
        except ValueError:
            await send_rich_message(client, room.room_id, "Error: param_1 must be integer, threshold must be a number.", "❌ <b>Format Error:</b> Period parameter must be an integer, and threshold must be a valid number.")
            return

        # Validate indicator support
        valid_indicators = ["RSI", "SMA", "EMA", "MACD", "BOLLINGER_HIGH", "BOLLINGER_LOW"]
        if indicator not in valid_indicators:
            await send_rich_message(
                client, room.room_id,
                f"Supported indicators: {', '.join(valid_indicators)}",
                f"❌ <b>Unsupported Indicator:</b> Choose from: {', '.join([f'<code>{i}</code>' for i in valid_indicators])}."
            )
            return

        # Validate condition support
        valid_conditions = ["ABOVE", "BELOW", "CROSS_ABOVE", "CROSS_BELOW"]
        if condition not in valid_conditions:
            await send_rich_message(
                client, room.room_id,
                f"Supported conditions: {', '.join(valid_conditions)}",
                f"❌ <b>Unsupported Condition:</b> Choose from: {', '.join([f'<code>{c}</code>' for c in valid_conditions])}."
            )
            return

        async with get_db_session() as session:
            # 1. Enforce Free Tier limits (max 2 active alerts)
            user_rec = await session.get(User, event.sender)
            if user_rec and user_rec.tier == "FREE":
                q_count = select(StockSubscription).where(StockSubscription.user_id == event.sender)
                res_count = await session.execute(q_count)
                if len(res_count.scalars().all()) >= settings.free_tier_limit:
                    await send_rich_message(
                        client, room.room_id,
                        f"Free Tier Limit Exceeded. You can only have {settings.free_tier_limit} stock alerts active. Upgrade to Premium.",
                        f"❌ <b>Limit Exceeded:</b> Free Tier is restricted to {settings.free_tier_limit} stock alerts. Upgrade to Premium with <code>!activate &lt;code&gt;</code>."
                    )
                    return

            # 2. Add the Stock Subscription
            new_sub = StockSubscription(
                user_id=event.sender,
                room_id=room.room_id,
                ticker=ticker,
                indicator=indicator,
                parameter_1=param_1,
                condition_type=condition,
                threshold=threshold
            )
            session.add(new_sub)

            html_msg = (
                f"📊 <b>Stock Alert Configured!</b><br>"
                f"• <b>Ticker:</b> <code>{ticker}</code><br>"
                f"• <b>Indicator:</b> {indicator}({param_1})<br>"
                f"• <b>Trigger Condition:</b> {condition} {threshold:.2f}"
            )
            plain_msg = f"Stock Alert Configured! Ticker: {ticker}, Indicator: {indicator}({param_1}), Condition: {condition} {threshold}"
            await send_rich_message(client, room.room_id, plain_msg, html_msg)

    async def _handle_unsubscribe(self, client: AsyncClient, room: MatrixRoom, event: RoomMessageText, unsub_args: List[str]) -> None:
        """Removes a stock alert subscription by its ID or ticker."""
        if not unsub_args:
            await send_rich_message(client, room.room_id, "Usage: !stock unsubscribe <id_or_ticker>", "Usage: <code>!stock unsubscribe &lt;id_or_ticker&gt;</code>")
            return

        target = unsub_args[0].strip()

        async with get_db_session() as session:
            if target.isdigit():
                # Delete by subscription integer ID
                sub_id = int(target)
                q = select(StockSubscription).where(
                    StockSubscription.id == sub_id,
                    (StockSubscription.room_id == room.room_id) | (StockSubscription.user_id == event.sender)
                )
            else:
                # Delete all matching ticker alerts configured by the user in this room
                ticker_upper = target.upper()
                q = select(StockSubscription).where(
                    StockSubscription.ticker == ticker_upper,
                    (StockSubscription.room_id == room.room_id) | (StockSubscription.user_id == event.sender)
                )

            res = await session.execute(q)
            subs = res.scalars().all()
            if not subs:
                await send_rich_message(client, room.room_id, "Alert subscription not found.", "Alert subscription not found.")
                return

            for s in subs:
                await session.delete(s)
            
            await send_rich_message(client, room.room_id, f"Successfully removed alert(s).", f"✅ Successfully removed alert subscription(s) for <code>{target}</code>.")

    async def _handle_check(self, client: AsyncClient, room: MatrixRoom, event: RoomMessageText, check_args: List[str]) -> None:
        """Evaluates and responds with technical indicators for a ticker on-demand."""
        if not check_args:
            await send_rich_message(
                client, room.room_id,
                "Usage: !stock check <ticker> [indicator] [param_1]",
                "Usage: <code>!stock check &lt;ticker&gt; [indicator] [param_1]</code>"
            )
            return

        ticker = check_args[0].upper().strip()
        
        req_indicator = None
        req_param = None
        if len(check_args) >= 3:
            req_indicator = check_args[1].upper().strip()
            try:
                req_param = int(check_args[2])
            except ValueError:
                await send_rich_message(client, room.room_id, "Error: Period parameter must be an integer.", "❌ <b>Format Error:</b> Period parameter must be an integer.")
                return
                
            valid_indicators = ["RSI", "SMA", "EMA", "MACD", "BOLLINGER_HIGH", "BOLLINGER_LOW"]
            if req_indicator not in valid_indicators:
                await send_rich_message(
                    client, room.room_id,
                    f"Supported indicators: {', '.join(valid_indicators)}",
                    f"❌ <b>Unsupported Indicator:</b> Choose from: {', '.join([f'<code>{i}</code>' for i in valid_indicators])}."
                )
                return

        # Fetch stock data
        async with httpx.AsyncClient(timeout=15.0) as http_client:
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=6mo&interval=1d"
                resp = await http_client.get(url, headers=YAHOO_HEADERS)
                if resp.status_code != 200:
                    await send_rich_message(
                        client, room.room_id,
                        f"Failed to fetch quotes for {ticker} (status {resp.status_code}).",
                        f"❌ <b>Error:</b> Failed to fetch quotes for <code>{ticker}</code> from Yahoo Finance."
                    )
                    return

                data = resp.json()
                chart_result = data["chart"]["result"][0]
                quote = chart_result["indicators"]["quote"][0]
                
                df = pd.DataFrame({
                    "close": quote["close"],
                    "high": quote["high"],
                    "low": quote["low"],
                    "open": quote["open"],
                    "volume": quote["volume"]
                })
                df = df.dropna().reset_index(drop=True)

                if df.empty or len(df) < 50:
                    await send_rich_message(
                        client, room.room_id,
                        f"Insufficient quotes history for {ticker}.",
                        f"❌ <b>Error:</b> Insufficient quote history for <code>{ticker}</code> to compute indicators."
                    )
                    return

                latest_close = float(df["close"].iloc[-1])

                if req_indicator and req_param:
                    # Calculate single requested indicator
                    col_name = f"{req_indicator}_{req_param}"
                    if req_indicator == "RSI":
                        df[col_name] = ta.momentum.rsi(df["close"], window=req_param)
                    elif req_indicator == "SMA":
                        df[col_name] = ta.trend.sma_indicator(df["close"], window=req_param)
                    elif req_indicator == "EMA":
                        df[col_name] = ta.trend.ema_indicator(df["close"], window=req_param)
                    elif req_indicator == "BOLLINGER_HIGH":
                        df[col_name] = ta.volatility.bollinger_hband(df["close"], window=req_param)
                    elif req_indicator == "BOLLINGER_LOW":
                        df[col_name] = ta.volatility.bollinger_lband(df["close"], window=req_param)
                    elif req_indicator == "MACD":
                        df[col_name] = ta.trend.macd(df["close"])

                    val = df[col_name].iloc[-1]
                    if pd.isna(val):
                        await send_rich_message(client, room.room_id, f"Not enough data to calculate {req_indicator}({req_param}).", f"Not enough data to calculate {req_indicator}({req_param}).")
                        return

                    html = f"📊 <b>Technicals for {ticker}:</b><br>• <b>{req_indicator}({req_param})</b>: <code>{val:.2f}</code><br>• <b>Latest Close</b>: <code>{latest_close:.2f}</code>"
                    plain = f"Technicals for {ticker}: {req_indicator}({req_param}): {val:.2f}, Close: {latest_close:.2f}"
                    await send_rich_message(client, room.room_id, plain, html)
                else:
                    # Calculate all standard indicators
                    df["rsi_14"] = ta.momentum.rsi(df["close"], window=14)
                    df["sma_50"] = ta.trend.sma_indicator(df["close"], window=50)
                    df["ema_20"] = ta.trend.ema_indicator(df["close"], window=20)
                    df["bb_high"] = ta.volatility.bollinger_hband(df["close"], window=20)
                    df["bb_low"] = ta.volatility.bollinger_lband(df["close"], window=20)

                    rsi_val = df["rsi_14"].iloc[-1]
                    sma_val = df["sma_50"].iloc[-1]
                    ema_val = df["ema_20"].iloc[-1]
                    bbh_val = df["bb_high"].iloc[-1]
                    bbl_val = df["bb_low"].iloc[-1]

                    html = (
                        f"<h3>📊 On-Demand Technical Summary: {ticker}</h3>"
                        f"<ul>"
                        f"<li><b>Latest Close:</b> <code>{latest_close:.2f}</code></li>"
                        f"<li><b>RSI (14):</b> <code>{rsi_val:.2f}</code></li>"
                        f"<li><b>SMA (50):</b> <code>{sma_val:.2f}</code></li>"
                        f"<li><b>EMA (20):</b> <code>{ema_val:.2f}</code></li>"
                        f"<li><b>Bollinger Band Upper (20):</b> <code>{bbh_val:.2f}</code></li>"
                        f"<li><b>Bollinger Band Lower (20):</b> <code>{bbl_val:.2f}</code></li>"
                        f"</ul>"
                    )
                    plain = f"Technicals for {ticker}: Close: {latest_close:.2f}, RSI(14): {rsi_val:.2f}, SMA(50): {sma_val:.2f}"
                    await send_rich_message(client, room.room_id, plain, html)

            except Exception as e:
                logger.exception("Failed to run stock check command", ticker=ticker, error=str(e))
                await send_rich_message(client, room.room_id, "An error occurred while fetching indicators.", "❌ <b>Error:</b> An error occurred while computing technical indicators.")

    async def on_tick(self, client: AsyncClient) -> None:
        """Triggered periodically. Downloads prices, computes indicators, and triggers alerts."""
        now_ts = int(datetime.now().timestamp())
        if now_ts - self._last_tick_time < self._poll_interval_seconds:
            return  # Throttle checks
        
        self._last_tick_time = now_ts
        logger.info("Executing stock TA alert checks...")

        async with get_db_session() as session:
            # 1. Fetch all active stock subscriptions
            q_subs = select(StockSubscription)
            res_subs = await session.execute(q_subs)
            subscriptions = res_subs.scalars().all()

        if not subscriptions:
            logger.debug("No active stock alerts to check.")
            return

        # Group subscriptions by ticker to fetch each symbol only once per poll
        ticker_groups: Dict[str, List[StockSubscription]] = {}
        for sub in subscriptions:
            ticker_groups.setdefault(sub.ticker, []).append(sub)

        async with httpx.AsyncClient(timeout=15.0) as http_client:
            for ticker, subs in ticker_groups.items():
                try:
                    logger.info("Fetching stock data", ticker=ticker)
                    # Fetch 6 months range of daily charts to support SMA 200 checks
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=6mo&interval=1d"
                    resp = await http_client.get(url, headers=YAHOO_HEADERS)
                    
                    if resp.status_code != 200:
                        logger.warning("Failed to fetch stock prices from Yahoo Finance", ticker=ticker, status=resp.status_code)
                        continue

                    # Parse quote series
                    data = resp.json()
                    chart_result = data["chart"]["result"][0]
                    quote = chart_result["indicators"]["quote"][0]
                    timestamps = chart_result["timestamp"]

                    df = pd.DataFrame({
                        "close": quote["close"],
                        "high": quote["high"],
                        "low": quote["low"],
                        "open": quote["open"],
                        "volume": quote["volume"]
                    })
                    df = df.dropna().reset_index(drop=True)

                    if df.empty or len(df) < 50:
                        logger.warning("Insufficient historical quotes for indicator calculation", ticker=ticker, rows=len(df))
                        continue

                    # Process indicators and check triggers
                    await self._evaluate_and_trigger_alerts(client, ticker, df, subs)

                except Exception as e:
                    logger.exception("Failed to run stock check for ticker", ticker=ticker, error=str(e))

    async def _evaluate_and_trigger_alerts(
        self, 
        client: AsyncClient, 
        ticker: str, 
        df: pd.DataFrame, 
        subs: List[StockSubscription]
    ) -> None:
        """Calculates indicators and evaluates triggers for a specific ticker dataframe."""
        # Calculate indicator columns dynamically based on subscription parameters
        for sub in subs:
            try:
                # 1. Compute target indicator column
                col_name = f"{sub.indicator}_{sub.parameter_1}"
                
                if sub.indicator == "RSI":
                    df[col_name] = ta.momentum.rsi(df["close"], window=sub.parameter_1)
                elif sub.indicator == "SMA":
                    df[col_name] = ta.trend.sma_indicator(df["close"], window=sub.parameter_1)
                elif sub.indicator == "EMA":
                    df[col_name] = ta.trend.ema_indicator(df["close"], window=sub.parameter_1)
                elif sub.indicator == "BOLLINGER_HIGH":
                    df[col_name] = ta.volatility.bollinger_hband(df["close"], window=sub.parameter_1)
                elif sub.indicator == "BOLLINGER_LOW":
                    df[col_name] = ta.volatility.bollinger_lband(df["close"], window=sub.parameter_1)
                elif sub.indicator == "MACD":
                    # Parameter 1 is standard fast period, we can hardcode slow/signal or base them on default
                    df[col_name] = ta.trend.macd(df["close"])
                else:
                    continue

                # Ensure we have calculated values (need enough periods)
                if df[col_name].isna().iloc[-1] or df[col_name].isna().iloc[-2]:
                    logger.debug("Indicator series contains NaN at check index", col=col_name)
                    continue

                current_val = float(df[col_name].iloc[-1])
                prev_val = float(df[col_name].iloc[-2])
                threshold = sub.threshold

                # 2. Evaluate alert trigger condition
                triggered = False
                if sub.condition_type == "ABOVE":
                    triggered = current_val > threshold
                elif sub.condition_type == "BELOW":
                    triggered = current_val < threshold
                elif sub.condition_type == "CROSS_ABOVE":
                    triggered = prev_val <= threshold and current_val > threshold
                elif sub.condition_type == "CROSS_BELOW":
                    triggered = prev_val >= threshold and current_val < threshold

                if triggered:
                    # 3. Handle Alert Cooldown checks
                    cooldown_expired = True
                    if sub.last_triggered:
                        cooldown_expired = datetime.now() - sub.last_triggered > timedelta(hours=self._cooldown_hours)

                    if cooldown_expired:
                        logger.info("Stock Alert Triggered!", subscription_id=sub.id, ticker=ticker)
                        
                        # Deliver notification
                        html = (
                            f"📈 <b>Stock Indicator Alert: {ticker}</b><br>"
                            f"The Technical Indicator <b>{sub.indicator}({sub.parameter_1})</b> is currently <b>{current_val:.2f}</b>, "
                            f"crossing/remaining <b>{sub.condition_type}</b> the threshold of <b>{threshold:.2f}</b>!<br>"
                            f"<i>(Latest Close: {df['close'].iloc[-1]:.2f})</i>"
                        )
                        plain = (
                            f"Stock Alert: {ticker} - {sub.indicator}({sub.parameter_1}) is {current_val:.2f}, "
                            f"which is {sub.condition_type} threshold {threshold:.2f}."
                        )
                        
                        try:
                            await send_rich_message(client, sub.room_id, plain, html)
                            
                            # Update last_triggered timestamp in database
                            async with get_db_session() as session:
                                db_sub = await session.get(StockSubscription, sub.id)
                                if db_sub:
                                    db_sub.last_triggered = datetime.now()
                        except Exception as ex:
                            logger.error("Failed to post stock alert notification", room_id=sub.room_id, error=str(ex))
            except Exception as e:
                logger.error("Error computing indicators or checking stock condition", sub_id=sub.id, error=str(e))

    async def _send_usage(self, client: AsyncClient, room_id: str) -> None:
        """Sends stock command assistance usage block."""
        html_msg = (
            "<b>Stock Alerts Panel Commands:</b><ul>"
            "<li><code>!stock list</code>: Lists alert configurations active in this room.</li>"
            "<li><code>!stock check &lt;ticker&gt; [indicator] [param]</code>: Evaluates and responds with technical indicators on-demand.</li>"
            "<li><code>!stock subscribe &lt;ticker&gt; &lt;indicator&gt; &lt;param&gt; &lt;condition&gt; &lt;threshold&gt;</code>: Adds alert subscription (e.g. <code>!stock subscribe SAP.DE RSI 14 BELOW 30</code>).</li>"
            "<li><code>!stock unsubscribe &lt;id_or_ticker&gt;</code>: Cancels configured alerts.</li>"
            "</ul>"
            "<br><b>Available Indicators for check/subscribe:</b>"
            "<table border='1'><tr><th>Indicator</th><th>Description</th><th>Typical Parameter</th></tr>"
            "<tr><td><code>RSI</code></td><td>Relative Strength Index (momentum)</td><td>14</td></tr>"
            "<tr><td><code>SMA</code></td><td>Simple Moving Average (trend follow)</td><td>50, 200</td></tr>"
            "<tr><td><code>EMA</code></td><td>Exponential Moving Average (fast trend)</td><td>20, 50</td></tr>"
            "<tr><td><code>MACD</code></td><td>Moving Average Convergence Divergence</td><td>12 (default)</td></tr>"
            "<tr><td><code>BOLLINGER_HIGH</code></td><td>Upper Bollinger Band (volatility ceiling)</td><td>20</td></tr>"
            "<tr><td><code>BOLLINGER_LOW</code></td><td>Lower Bollinger Band (volatility floor)</td><td>20</td></tr>"
            "</table>"
        )
        plain_msg = (
            "Stock Commands:\n"
            "- !stock list\n"
            "- !stock check <ticker> [indicator] [param]\n"
            "- !stock subscribe <ticker> <indicator> <param> <condition> <threshold>\n"
            "- !stock unsubscribe <id_or_ticker>\n\n"
            "Available Indicators:\n"
            "- RSI: Relative Strength Index (Typical period: 14)\n"
            "- SMA: Simple Moving Average (Typical periods: 50, 200)\n"
            "- EMA: Exponential Moving Average (Typical periods: 20, 50)\n"
            "- MACD: Moving Average Convergence Divergence\n"
            "- BOLLINGER_HIGH: Upper Bollinger Band (Typical period: 20)\n"
            "- BOLLINGER_LOW: Lower Bollinger Band (Typical period: 20)"
        )
        await send_rich_message(client, room_id, plain_msg, html_msg)

    def get_help(self) -> str:
        return (
            "• <b>!stock list</b>: Lists stock alert subscriptions active in this context.<br>"
            "• <b>!stock check &lt;ticker&gt; [indicator] [param]</b>: Responds with technical indicator levels for a stock on-demand.<br>"
            "• <b>!stock subscribe &lt;ticker&gt; &lt;indicator&gt; &lt;param&gt; &lt;condition&gt; &lt;threshold&gt;</b>: Subscribes this channel to alert triggers on indicators (RSI, SMA, EMA, BOLLINGER_HIGH/LOW, MACD).<br>"
            "• <b>!stock unsubscribe &lt;id_or_ticker&gt;</b>: Cancels stock alerts."
        )
