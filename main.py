import asyncio
import base64
import datetime
import json
from dataclasses import field as dc_field
from typing import Any, Optional, cast

import aiohttp
from pydantic import Field
from pydantic.dataclasses import dataclass

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import MessageChain, filter, AstrMessageEvent
from astrbot.api.star import Context, Star
from astrbot.core.agent.run_context import ContextWrapper
from astrbot.core.agent.tool import FunctionTool, ToolExecResult
from astrbot.core.astr_agent_context import AstrAgentContext


@dataclass
class FetchNtfyMessagesTool(FunctionTool[AstrAgentContext]):
    name: str = "fetch_ntfy_messages"
    description: str = "Fetch recent messages from a ntfy.sh topic."
    parameters: dict = Field(
        default_factory=lambda: {
            "type": "object",
            "properties": {
                "topic": {
                    "type": "string",
                    "description": "The ntfy topic name to fetch messages from.",
                },
                "since": {
                    "type": "string",
                    "description": 'Time range such as "10m", "1h", "all". Default is "10m".',
                },
            },
            "required": ["topic"],
        }
    )
    _fetch_topic: Any = dc_field(init=False, default=None)

    async def call(
        self,
        _context: ContextWrapper[AstrAgentContext],  # type: ignore[valid-type]
        **kwargs: Any,
    ) -> ToolExecResult:
        topic = kwargs.get("topic", "")
        since = kwargs.get("since", "10m")
        result = cast(
            Optional[list], await self._fetch_topic(topic, since)
        )
        if result is None:
            return "Failed to fetch messages from ntfy."
        if not result:
            return f"No messages in topic '{topic}' for the last {since}."

        lines = [f"Topic '{topic}' — {len(result)} message(s):"]
        for msg in result[-20:]:
            t = datetime.datetime.fromtimestamp(
                msg.get("time", 0)
            ).strftime("%Y-%m-%d %H:%M:%S")
            line = f"[{t}] {msg.get('title', '')}: {msg.get('message', '')}"
            if msg.get("tags"):
                line += f" [tags: {', '.join(msg['tags'])}]"
            lines.append(line)

        return "\n".join(lines)


class NtfyPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._conn_task: Optional[asyncio.Task] = None
        self._running = False

        tool = FetchNtfyMessagesTool()
        object.__setattr__(tool, "_fetch_topic", self._fetch_topic)
        self.context.add_llm_tools(tool)  # type: ignore[attr-defined]

        conv_mgr = self.context.conversation_manager  # type: ignore[attr-defined]
        conv_mgr.register_on_session_deleted(self._on_session_deleted)

    async def initialize(self):
        self._running = True
        subs = cast(dict, await self.get_kv_data("subscriptions", {}))
        if subs:
            topic_count = sum(len(v.get("topics", [])) for v in subs.values())
            logger.info(
                f"ntfy: restoring {topic_count} topic(s) across {len(subs)} session(s)"
            )
            await self._start_connection()

    async def terminate(self):
        self._running = False
        if self._conn_task:
            self._conn_task.cancel()
            try:
                await self._conn_task
            except asyncio.CancelledError:
                pass
        logger.info("ntfy plugin terminated")

    async def _on_session_deleted(self, session_id: str):
        subs = cast(dict, await self.get_kv_data("subscriptions", {}))
        if session_id in subs:
            topics = subs[session_id].get("topics", [])
            del subs[session_id]
            await self.put_kv_data("subscriptions", subs)
            await self._start_connection()
            logger.info(
                f"ntfy: cleaned up subscription for deleted session {session_id} "
                f"(topics: {topics})"
            )

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _get_auth_header(self) -> str:
        method = self.config.get("auth_method", "none")
        if method == "basic":
            user = self.config.get("username", "")
            pwd = self.config.get("password", "")
            if user and pwd:
                creds = base64.b64encode(f"{user}:{pwd}".encode()).decode()
                return f"Basic {creds}"
        elif method == "token":
            token = self.config.get("access_token", "")
            if token:
                return f"Bearer {token}"
        return ""

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def _start_connection(self):
        if self._conn_task and not self._conn_task.done():
            self._conn_task.cancel()
            try:
                await self._conn_task
            except asyncio.CancelledError:
                pass
        self._conn_task = asyncio.create_task(self._listen())

    async def _get_all_topics(self) -> list:
        subs = cast(dict, await self.get_kv_data("subscriptions", {}))
        topics: set[str] = set()
        for info in subs.values():
            for t in info.get("topics", []):
                topics.add(t)
        return sorted(topics)

    async def _listen(self):
        server = self.config.get("server_url", "https://ntfy.sh").rstrip("/")
        auth = self._get_auth_header()
        retry_delay = 1
        max_delay = self.config.get("reconnect_max_delay", 60)

        while self._running:
            topics = await self._get_all_topics()
            if not topics:
                await asyncio.sleep(5)
                continue

            url = f"{server}/{','.join(topics)}/json"
            params = {}

            priority = self.config.get("default_priority", "")
            tags = self.config.get("default_tags", "")
            if priority:
                params["priority"] = priority
            if tags:
                params["tags"] = tags

            last_id = await self.get_kv_data("last_message_id", "")
            if last_id:
                params["since"] = last_id

            headers = {}
            if auth:
                headers["Authorization"] = auth

            logger.info(f"ntfy connecting to {url}")

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url, params=params, headers=headers
                    ) as resp:
                        if resp.status != 200:
                            body = await resp.text()
                            logger.error(
                                f"ntfy returned {resp.status}: {body[:200]}"
                            )
                            await asyncio.sleep(retry_delay)
                            retry_delay = min(retry_delay * 2, max_delay)
                            continue

                        retry_delay = 1
                        logger.info("ntfy stream connected")

                        async for line in resp.content:
                            if not self._running:
                                break
                            if not line:
                                continue
                            try:
                                data = json.loads(line)
                            except json.JSONDecodeError:
                                continue

                            await self._handle_event(data)

                            if data.get("event") == "message" and data.get("id"):
                                await self.put_kv_data(
                                    "last_message_id", data["id"]
                                )

            except asyncio.CancelledError:
                break
            except aiohttp.ClientError as e:
                logger.error(
                    f"ntfy connection error: {e}, retrying in {retry_delay}s"
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_delay)
            except Exception as e:
                logger.error(
                    f"ntfy unexpected error: {e}, retrying in {retry_delay}s"
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_delay)

    async def _handle_event(self, data: dict):
        if data.get("event") != "message":
            return

        topic = data.get("topic", "")
        title = data.get("title", "")
        message = data.get("message", "")
        msg_tags = data.get("tags", [])
        click = data.get("click", "")

        parts = [f"[ntfy/{topic}]"]
        if title:
            parts.append(title)
        if message:
            parts.append(message)
        if msg_tags:
            parts.append(f"Tags: {', '.join(msg_tags)}")
        if click:
            parts.append(click)

        text = "\n".join(parts)

        subs = cast(dict, await self.get_kv_data("subscriptions", {}))
        for session_id, info in subs.items():
            if topic not in info.get("topics", []):
                continue
            if not self._match_filters(data, info):
                continue
            try:
                await self.context.send_message(session_id, MessageChain().message(text))  # type: ignore[attr-defined]
            except Exception as e:
                logger.error(f"ntfy failed to send to {session_id}: {e}")

    def _match_filters(self, data: dict, info: dict) -> bool:
        """Apply per-session priority and tags filters."""
        pf = info.get("priority_filter", "")
        if pf:
            try:
                allowed = {int(p.strip()) for p in pf.split(",") if p.strip()}
                if data.get("priority", 3) not in allowed:
                    return False
            except ValueError:
                pass

        tf = info.get("tags_filter", "")
        if tf:
            required = {t.strip().lower() for t in tf.split(",") if t.strip()}
            msg_tags = {t.lower() for t in data.get("tags", [])}
            if not required.issubset(msg_tags):
                return False

        return True

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    @filter.command("ntfy_sub")
    async def ntfy_sub(self, event: AstrMessageEvent):
        """Subscribe current chat to a ntfy topic"""
        umo = event.unified_msg_origin
        if not umo:
            yield event.plain_result(
                "This platform does not support proactive push messages."
            )
            return

        parts = event.message_str.strip().split()
        if len(parts) < 2:
            yield event.plain_result(
                "Usage: /ntfy_sub <topic> [priority_filter] [tags_filter]\n"
                "Examples:\n"
                "  /ntfy_sub alerts\n"
                "  /ntfy_sub alerts 4,5 error"
            )
            return

        topic = parts[1]
        priority = parts[2] if len(parts) > 2 else ""
        tags = parts[3] if len(parts) > 3 else ""

        subs = cast(dict, await self.get_kv_data("subscriptions", {}))

        umo_subs = subs.setdefault(umo, {})
        umo_subs.setdefault("topics", [])
        if topic in umo_subs["topics"]:
            yield event.plain_result(f"Already subscribed to topic: {topic}")
            return

        umo_subs["topics"].append(topic)
        if priority:
            umo_subs["priority_filter"] = priority
        if tags:
            umo_subs["tags_filter"] = tags

        await self.put_kv_data("subscriptions", subs)
        await self._start_connection()
        yield event.plain_result(f"Subscribed to topic: {topic}")

    @filter.command("ntfy_unsub")
    async def ntfy_unsub(self, event: AstrMessageEvent):
        """Unsubscribe from a ntfy topic"""
        umo = event.unified_msg_origin
        if not umo:
            yield event.plain_result(
                "This platform does not support proactive push messages."
            )
            return

        parts = event.message_str.strip().split()
        if len(parts) < 2:
            yield event.plain_result("Usage: /ntfy_unsub <topic>")
            return

        topic = parts[1]
        subs = cast(dict, await self.get_kv_data("subscriptions", {}))

        if umo not in subs or topic not in subs[umo].get("topics", []):
            yield event.plain_result(f"Not subscribed to topic: {topic}")
            return

        subs[umo]["topics"].remove(topic)
        if not subs[umo]["topics"]:
            del subs[umo]

        await self.put_kv_data("subscriptions", subs)
        await self._start_connection()
        yield event.plain_result(f"Unsubscribed from topic: {topic}")

    @filter.command("ntfy_list")
    async def ntfy_list(self, event: AstrMessageEvent):
        """List ntfy subscriptions for current chat"""
        umo = event.unified_msg_origin
        subs = cast(dict, await self.get_kv_data("subscriptions", {}))

        if umo not in subs or not subs[umo].get("topics"):
            yield event.plain_result("No ntfy subscriptions in this chat.")
            return

        info = subs[umo]
        yield event.plain_result(
            f"Subscriptions in this chat:\n"
            f"Topics: {', '.join(info.get('topics', []))}\n"
            f"Priority filter: {info.get('priority_filter') or 'none'}\n"
            f"Tags filter: {info.get('tags_filter') or 'none'}"
        )

    @filter.command("ntfy_fetch")
    async def ntfy_fetch(self, event: AstrMessageEvent):
        """Fetch recent messages from a ntfy topic"""
        parts = event.message_str.strip().split()
        if len(parts) < 2:
            yield event.plain_result(
                "Usage: /ntfy_fetch <topic> [since]\n"
                "Examples:\n"
                "  /ntfy_fetch alerts\n"
                "  /ntfy_fetch alerts 10m\n"
                "  /ntfy_fetch alerts all"
            )
            return

        topic = parts[1]
        since = parts[2] if len(parts) > 2 else "10m"

        messages = await self._fetch_topic(topic, since)
        if messages is None:
            yield event.plain_result("Failed to fetch messages. Check logs for details.")
            return

        if not messages:
            yield event.plain_result(f"No messages for topic '{topic}' (since={since}).")
            return

        lines = [f"Topic '{topic}' — {len(messages)} message(s):"]
        for msg in messages[-10:]:
            t = datetime.datetime.fromtimestamp(msg.get("time", 0)).strftime("%m-%d %H:%M")
            line = f"[{t}]"
            if msg.get("title"):
                line += f" {msg['title']}"
            if msg.get("message"):
                line += f" — {msg['message']}"
            lines.append(line)

        yield event.plain_result("\n".join(lines))

    # ------------------------------------------------------------------
    # Shared fetch helper
    # ------------------------------------------------------------------

    async def _fetch_topic(self, topic: str, since: str = "10m") -> Optional[list]:
        server = self.config.get("server_url", "https://ntfy.sh").rstrip("/")
        auth = self._get_auth_header()

        try:
            async with aiohttp.ClientSession() as session:
                params = {"poll": "1", "since": since}
                headers = {}
                if auth:
                    headers["Authorization"] = auth

                async with session.get(
                    f"{server}/{topic}/json", params=params, headers=headers
                ) as resp:
                    if resp.status != 200:
                        logger.error(f"ntfy fetch returned {resp.status}")
                        return None

                    messages = []
                    async for line in resp.content:
                        if line:
                            data = json.loads(line)
                            if data.get("event") == "message":
                                messages.append(data)

                    return messages

        except Exception as e:
            logger.error(f"ntfy fetch error: {e}")
            return None
