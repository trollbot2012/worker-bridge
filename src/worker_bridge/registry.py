"""Worker discovery and adapter registration."""

from __future__ import annotations

import asyncio
from typing import Iterable

from worker_bridge.adapters import (
    ClaudeCodeAdapter,
    CodexAdapter,
    DiscoveryOnlyAdapter,
    GenericCliAdapter,
    MockWorkerAdapter,
    OpenCodeAdapter,
    VSCodeBridgeAdapter,
    WorkerAdapter,
    ZCodeGlmAdapter,
)


class WorkerRegistry:
    def __init__(self, adapters: Iterable[WorkerAdapter] | None = None) -> None:
        self._adapters: dict[str, WorkerAdapter] = {}
        for adapter in adapters or (
            CodexAdapter(),
            ClaudeCodeAdapter(),
            OpenCodeAdapter(),
            ZCodeGlmAdapter(),
            VSCodeBridgeAdapter(),
            DiscoveryOnlyAdapter("gemini-cli", "gemini"),
            MockWorkerAdapter(),
        ):
            self.register(adapter)

    def register(self, adapter: WorkerAdapter) -> None:
        if not adapter.name:
            raise ValueError("adapter name is required")
        self._adapters[adapter.name] = adapter

    def get(self, name: str) -> WorkerAdapter:
        try:
            return self._adapters[name]
        except KeyError as exc:
            raise KeyError(f"unknown worker: {name}") from exc

    async def inspect(self, name: str) -> dict:
        adapter = self.get(name)
        availability, capabilities, health = await asyncio.gather(
            adapter.detect(), adapter.capabilities(), adapter.health_check()
        )
        return {
            "worker": name,
            "availability": {slot: getattr(availability, slot) for slot in availability.__slots__},
            "capabilities": {slot: getattr(capabilities, slot) for slot in capabilities.__slots__},
            "health": {slot: getattr(health, slot) for slot in health.__slots__},
        }

    async def list(self) -> list[dict]:
        return await asyncio.gather(*(self.inspect(name) for name in sorted(self._adapters)))

    async def discover(self, kind: str = "all") -> list[dict]:
        from worker_bridge.discovery import discover_ecosystem

        records = await discover_ecosystem(self)
        if kind == "all":
            return records
        normalized = "assistant" if kind in {"worker", "workers", "client", "clients"} else kind.rstrip("s")
        return [record for record in records if record["kind"] == normalized]

    @classmethod
    def from_config(cls, config: dict | None) -> "WorkerRegistry":
        registry = cls()
        cfg = config or {}
        # Honor either a flat config (`workers:` at top level) or a namespaced
        # `worker_bridge:` section.
        definitions = ((cfg.get("worker_bridge") or cfg).get("workers")) or {}
        for name, item in definitions.items():
            if not isinstance(item, dict) or not item.get("command"):
                continue
            command = [str(part) for part in item["command"]]
            resume = item.get("resume_command")
            registry.register(
                GenericCliAdapter(
                    str(name),
                    command,
                    resume_command=[str(part) for part in resume] if resume else None,
                    maximum_concurrency=int(item.get("maximum_concurrency", 1)),
                )
            )
        return registry
