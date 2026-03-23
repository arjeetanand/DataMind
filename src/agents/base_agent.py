"""
DataMind — A2A (Agent-to-Agent) Base Protocol

All agents inherit from BaseAgent and communicate via
standardised A2AMessage envelopes — same protocol as the
Multi-Agent A2A project on the resume.

Message flow:
  Orchestrator → DataAgent → InsightAgent → ActionAgent
                    ↑_____________________________|  (feedback loop)
"""

import uuid
import time
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional
from enum import Enum

log = logging.getLogger(__name__)


class AgentRole(str, Enum):
    ORCHESTRATOR = "orchestrator"
    DATA         = "data_agent"
    INSIGHT      = "insight_agent"
    ACTION       = "action_agent"


class MessageStatus(str, Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    SUCCESS   = "success"
    ERROR     = "error"
    DELEGATED = "delegated"


@dataclass
class A2AMessage:
    """Standardised inter-agent message envelope."""
    sender      : AgentRole
    receiver    : AgentRole
    intent      : str                          # e.g. "fetch_revenue", "generate_insight"
    payload     : dict = field(default_factory=dict)
    message_id  : str  = field(default_factory=lambda: str(uuid.uuid4())[:8])
    timestamp   : float = field(default_factory=time.time)
    status      : MessageStatus = MessageStatus.PENDING
    result      : Optional[Any] = None
    error       : Optional[str] = None
    trace       : list = field(default_factory=list)   # execution trace for observability

    def succeed(self, result: Any) -> "A2AMessage":
        """Mark the message as successfully processed and store the result.
        Appends a success indicator to the execution trace for observability."""
        self.status = MessageStatus.SUCCESS
        self.result = result
        self.trace.append(f"[{self.receiver}] SUCCESS at {time.strftime('%H:%M:%S')}")
        return self

    def fail(self, error: str) -> "A2AMessage":
        """Mark the message as failed and record the error description.
        Appends the error details to the execution trace for debugging."""
        self.status = MessageStatus.ERROR
        self.error  = error
        self.trace.append(f"[{self.receiver}] ERROR: {error}")
        return self

    def to_dict(self) -> dict:
        """Convert the message envelope into a serialisable dictionary.
        Used for logging, inter-process communication, and API responses."""
        return {
            "message_id" : self.message_id,
            "sender"     : self.sender,
            "receiver"   : self.receiver,
            "intent"     : self.intent,
            "status"     : self.status,
            "result"     : self.result,
            "error"      : self.error,
            "trace"      : self.trace,
        }


class BaseAgent(ABC):
    """
    Abstract base for all DataMind agents.
    Implements retry logic, logging, and A2A message handling.
    """

    def __init__(self, role: AgentRole, max_retries: int = 3):
        """Initialise a new agent with a specific architectural role.
        Sets up the internal logger and retry threshold for operations."""
        self.role        = role

    def handle(self, message: A2AMessage) -> A2AMessage:
        """Execute the agent's logic on a message with built-in retry safety.
        Validates the receiver and coordinates the internal _execute call."""
        if message.receiver != self.role:
            return message.fail(f"Wrong receiver: expected {self.role}, got {message.receiver}")

        message.status = MessageStatus.RUNNING
        message.trace.append(f"[{self.role}] Received intent='{message.intent}'")

        for attempt in range(1, self.max_retries + 1):
            try:
                result = self._execute(message)
                return message.succeed(result)
            except Exception as e:
                self.log.warning(f"Attempt {attempt}/{self.max_retries} failed: {e}")
                if attempt == self.max_retries:
                    return message.fail(str(e))
                time.sleep(0.5 * attempt)

    @abstractmethod
    def _execute(self, message: A2AMessage) -> Any:
        """Agent-specific logic. Must return a serialisable result."""
        ...

    def __repr__(self):
        return f"<{self.__class__.__name__} role={self.role}>"
