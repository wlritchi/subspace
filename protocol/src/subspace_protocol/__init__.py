"""Wire protocol for the subspace server's WebSocket API.

Each frame is one JSON object in a WebSocket text message. The ``type`` field
names the frame. A client request carries an integer ``req_id``, and the
server copies it into the reply. ``register`` and ``ack`` get no reply.
``notify`` frames are pushed by the server and carry no ``req_id``.

To connect, a client sends ``Authorization: Bearer <token>`` in the WebSocket
handshake. The token selects an account: the master token selects the default
account, and an ``ssp_`` credential selects the account that it belongs to. If
the token is wrong, the server closes the connection with code 1008, also later
if the credential is revoked. If the server does not know a frame ``type``, it
replies with an ``error`` frame. A frame that needs a permission that the
account does not have gets an ``error`` frame with ``forbidden: <permission>``.

A session is identified by its account and a session id that the client
supplies (1-128 characters in 0x21-0x7E). A handle is a kebab-case name that a
session registers so that other sessions can address it. Inside an account a
handle is written bare (``reviewer``); from another account it is written
``reviewer@ci``. Fields that name a session or a handle (``AgentRecord.name``,
``Message.sender``, ``MessageMeta.from``, member lists) use the form that is
correct for the reader.

A minor version adds optional fields, frame types, or values of a type that
is marked ``Open``. Each side ignores fields it does not know, and a client
ignores frame types it does not know. A major version removes or changes
something that an existing client or server depends on.

This module is the specification. It has only constants and type
definitions, and it imports nothing outside the standard library.
"""

from typing import Annotated, Final, Literal, NotRequired, TypedDict

DEFAULT_HOST: Final = "127.0.0.1"
DEFAULT_PORT: Final = 8137

AUTH_HEADER: Final = "Authorization"
UNAUTHORIZED_CLOSE_CODE: Final = 1008


def auth_header_value(token: str) -> str:
    """The value of the handshake auth header for ``token``."""
    return f"Bearer {token}"


class Open:
    """Marks a type that a minor version may extend with new values or variants.

    Use it as ``Annotated[T, Open]``. A client must accept a value or variant
    that it does not know, for example by ignoring it or by showing it as is.
    """


ReqId = int
Threshold = Literal["all", "direct", "urgent"]
Level = Literal["ambient", "direct", "urgent"]
Intent = Literal["fyi", "question", "request", "reply"]
Severity = Literal["low", "normal", "high"]
NotifySeverity = Literal["info", "high"]

# --------------------------------------------------------------------------- #
# Frame types
# --------------------------------------------------------------------------- #

# Client -> server
REGISTER: Final = "register"
ACK: Final = "ack"
SCHEDULE: Final = "schedule"
LIST: Final = "list"
SUBSCRIBE_PR: Final = "subscribe_pr"
UNSUBSCRIBE_PR: Final = "unsubscribe_pr"
REFRESH_PR: Final = "refresh_pr"
LIST_PR_SUBSCRIPTIONS: Final = "list_pr_subscriptions"
SUBSCRIBE_FORGEJO_PR: Final = "subscribe_forgejo_pr"
UNSUBSCRIBE_FORGEJO_PR: Final = "unsubscribe_forgejo_pr"
REFRESH_FORGEJO_PR: Final = "refresh_forgejo_pr"
LIST_FORGEJO_PR_SUBSCRIPTIONS: Final = "list_forgejo_pr_subscriptions"
REGISTER_AGENT: Final = "register_agent"
UNREGISTER_AGENT: Final = "unregister_agent"
LIST_AGENTS: Final = "list_agents"
SET_AVAILABILITY: Final = "set_availability"
JOIN_CHANNEL: Final = "join_channel"
LEAVE_CHANNEL: Final = "leave_channel"
POST: Final = "post"
DM: Final = "dm"
SET_THRESHOLD: Final = "set_threshold"
SET_CHANNEL_TOPIC: Final = "set_channel_topic"
LIST_CHANNELS: Final = "list_channels"
LIST_SUBSCRIPTIONS: Final = "list_subscriptions"
REACT: Final = "react"
MESSAGE_STATUS: Final = "message_status"
WHOAMI: Final = "whoami"

# Server -> client
NOTIFY: Final = "notify"
SCHEDULED: Final = "scheduled"
LIST_RESULT: Final = "list_result"
SUBSCRIBED: Final = "subscribed"
UNSUBSCRIBED: Final = "unsubscribed"
SUBSCRIPTIONS_RESULT: Final = "subscriptions_result"
PR_REFRESHED: Final = "pr_refreshed"
AGENT_OK: Final = "agent_ok"
AGENT_LIST: Final = "agent_list"
CHANNEL_JOINED: Final = "channel_joined"
POSTED: Final = "posted"
CHANNEL_LIST: Final = "channel_list"
SUBSCRIPTION_LIST: Final = "subscription_list"
MESSAGE_STATUS_RESULT: Final = "message_status_result"
WHOAMI_RESULT: Final = "whoami_result"
ERROR: Final = "error"

# --------------------------------------------------------------------------- #
# Shared objects
# --------------------------------------------------------------------------- #


class AgentRecord(TypedDict):
    # The session's handle, bare or qualified for the reader.
    name: str
    # Empty when the session is in another account than the reader.
    session_id: str
    description: str
    capabilities: str
    # Deprecated: always "".
    working_dir: str
    default_threshold: Threshold
    # When the session took its handle.
    registered_at: float
    last_seen: float
    # Deprecated: always "".
    reclaim_key_hash: str
    # Deprecated: always 1.
    generation: int
    # 0.0 means the session has not acknowledged a delivery since the server
    # started.
    last_acked: float
    # The account of the session.
    account: NotRequired[str]


class AgentListEntry(AgentRecord):
    connected: bool


class Message(TypedDict):
    """A channel or DM message, as it appears in channel history."""

    id: str  # "msg:<context>:<seq>"
    seq: int
    sender: str  # handle (bare or qualified for the reader), or "system"
    body: str
    intent: Intent | Literal["reaction"]
    severity: Severity
    mentions: list[str]
    created_at: float
    # For a reaction, the id of the message it reacts to. Otherwise "".
    target: str
    # The global "#N" handle. 0 means the message has none.
    ordinal: int
    # The sender's account. "" for "system".
    account: NotRequired[str]


class ScheduledItem(TypedDict):
    id: str
    due_at: float
    kind: str


class PrSubscription(TypedDict):
    pr: str  # "owner/repo#number"
    merged: bool
    pending: int
    instance: NotRequired[str]  # named Forgejo instances only


class ChannelInfo(TypedDict):
    name: str
    topic: str
    members: int
    last_activity: float  # 0.0 if nothing was posted


class TopicSubscription(TypedDict):
    context: str  # "chan:<name>" or "dm:<a,b,...>"
    kind: Literal["channel", "dm"]
    threshold: Threshold


class RecipientInfo(TypedDict):
    connected: bool
    # None when the recipient has no agent record.
    last_seen: float | None
    last_acked: float | None


class Reaction(TypedDict):
    by: str
    reaction: str


# --------------------------------------------------------------------------- #
# notify metadata
# --------------------------------------------------------------------------- #


class CallbackMeta(TypedDict):
    """A scheduled callback fired. ``kind`` is the value the client scheduled."""

    severity: Literal["info"]
    kind: str
    callback_id: str


class PrTruncatedMeta(TypedDict):
    """Some PR events were dropped before this session acknowledged them."""

    severity: Literal["high"]
    kind: Literal["pr_truncated"]
    pr: str


PrEventKind = Literal[
    "pr_closed",
    "pr_conflict",
    "pr_conflict_resolved",
    "pr_commits",
    "pr_thread",
    "pr_checks_green",
    "pr_label",
    "pr_review_request",
    "pr_draft",
    "pr_force_push",
    "pr_merged",
    "pr_review",
    "pr_comment",
    "pr_inline_comment",
    "pr_check",
    "pr_checks_summary",
    "pr_status",
    "pr_gone",
    "pr_auth_error",
]


class PrEventMeta(TypedDict):
    severity: NotifySeverity
    kind: Annotated[PrEventKind, Open]
    pr: str
    url: NotRequired[str]


MessageMeta = TypedDict(
    "MessageMeta",
    {
        "kind": Literal["message"],
        "context": str,
        "from": str,
        "intent": Intent | Literal["reaction"],
        "severity": Severity,
        "mentions": list[str],
        "level": Level,
        "threshold": Threshold,
        # The sender's account. Absent for "system".
        "account": NotRequired[str],
        "connections": NotRequired[int],
    },
)


class SuccessionMeta(TypedDict):
    """Deprecated: the server no longer sends it."""

    kind: Literal["succession"]
    level: Literal["direct"]
    threshold: Literal["direct"]


class MultipleConnectionsMeta(TypedDict):
    """The number of connections attached to this session went above one, or
    back to one. While it is above one, delivery can be split between them."""

    kind: Literal["multiple_connections"]
    connections: int
    level: Literal["direct"]
    threshold: Literal["direct"]


class HandleLostMeta(TypedDict):
    """Another session in the account took this session's handle."""

    kind: Literal["handle_lost"]
    handle: str
    level: Literal["direct"]
    threshold: Literal["direct"]


NotifyMeta = (
    CallbackMeta
    | PrTruncatedMeta
    | PrEventMeta
    | MessageMeta
    | SuccessionMeta
    | MultipleConnectionsMeta
    | HandleLostMeta
)

# --------------------------------------------------------------------------- #
# Client -> server frames
# --------------------------------------------------------------------------- #


class Register(TypedDict):
    """Attach this connection to a session in its account. No reply, except an
    ``error`` for a session id that is not valid.

    A session id ``@<handle>`` attaches to the session that holds the handle,
    or to a new session with that id, which then takes the handle. ``None``
    detaches the connection.
    """

    type: Literal["register"]
    session_id: str | None


class Ack(TypedDict):
    """Acknowledge a ``notify`` by its id. No reply."""

    type: Literal["ack"]
    id: str


class Schedule(TypedDict):
    """Reply: ``scheduled``."""

    type: Literal["schedule"]
    req_id: ReqId
    session_id: str
    delay_seconds: NotRequired[int]  # default 300
    kind: NotRequired[str]  # default "scheduled"


class ListScheduled(TypedDict):
    """List scheduled callbacks. Reply: ``list_result``."""

    type: Literal["list"]
    req_id: ReqId
    session_id: str


class SubscribePr(TypedDict):
    """Reply: ``subscribed``."""

    type: Literal["subscribe_pr"]
    req_id: ReqId
    session_id: str
    owner: str
    repo: str
    number: int


class UnsubscribePr(TypedDict):
    """Reply: ``unsubscribed``, also when the session was not subscribed."""

    type: Literal["unsubscribe_pr"]
    req_id: ReqId
    session_id: str
    owner: str
    repo: str
    number: int


class RefreshPr(TypedDict):
    """Poll a tracked PR now. Reply: ``pr_refreshed``."""

    type: Literal["refresh_pr"]
    req_id: ReqId
    session_id: str
    owner: str
    repo: str
    number: int


class ListPrSubscriptions(TypedDict):
    """Reply: ``subscriptions_result``."""

    type: Literal["list_pr_subscriptions"]
    req_id: ReqId
    session_id: str


class SubscribeForgejoPr(TypedDict):
    """Reply: ``subscribed``. Omit ``instance`` for the default instance."""

    type: Literal["subscribe_forgejo_pr"]
    req_id: ReqId
    session_id: str
    owner: str
    repo: str
    number: int
    instance: NotRequired[str]


class UnsubscribeForgejoPr(TypedDict):
    """Reply: ``unsubscribed``."""

    type: Literal["unsubscribe_forgejo_pr"]
    req_id: ReqId
    session_id: str
    owner: str
    repo: str
    number: int
    instance: NotRequired[str]


class RefreshForgejoPr(TypedDict):
    """Reply: ``pr_refreshed``."""

    type: Literal["refresh_forgejo_pr"]
    req_id: ReqId
    session_id: str
    owner: str
    repo: str
    number: int
    instance: NotRequired[str]


class ListForgejoPrSubscriptions(TypedDict):
    """Reply: ``subscriptions_result``."""

    type: Literal["list_forgejo_pr_subscriptions"]
    req_id: ReqId
    session_id: str


class RegisterAgent(TypedDict):
    """Register ``name`` as this session's handle. Reply: ``agent_ok`` with
    ``agent``.

    If another session in the account holds the handle, this fails unless
    ``takeover`` is true or that session has had no connection for the
    server's handle idle time. State does not move with the handle.
    """

    type: Literal["register_agent"]
    req_id: ReqId
    session_id: str
    name: str  # bare handle
    description: NotRequired[str]
    capabilities: NotRequired[str]
    # Deprecated: ignored.
    working_dir: NotRequired[str]
    default_threshold: NotRequired[Threshold]  # default "direct"
    # Deprecated: ignored.
    reclaim_key: NotRequired[str]
    takeover: NotRequired[bool]


class UnregisterAgent(TypedDict):
    """Release the handle and leave every channel and DM. Reply: ``agent_ok``
    with ``agent`` (None if the session held no handle)."""

    type: Literal["unregister_agent"]
    req_id: ReqId
    session_id: str


class ListAgents(TypedDict):
    """Reply: ``agent_list``. Registration is not necessary."""

    type: Literal["list_agents"]
    req_id: ReqId
    session_id: NotRequired[str | None]


class SetAvailability(TypedDict):
    """Reply: ``agent_ok`` with ``agent``."""

    type: Literal["set_availability"]
    req_id: ReqId
    session_id: str
    default_threshold: Threshold


class JoinChannel(TypedDict):
    """Reply: ``channel_joined``. The sender must be a registered agent.

    ``topic`` is applied only if the channel is new or has no messages.
    """

    type: Literal["join_channel"]
    req_id: ReqId
    session_id: str
    channel: str  # lowercase kebab-case, 2-64 chars
    threshold: NotRequired[Threshold]
    topic: NotRequired[str]


class LeaveChannel(TypedDict):
    """Reply: ``agent_ok`` with no fields."""

    type: Literal["leave_channel"]
    req_id: ReqId
    session_id: str
    channel: str


class Post(TypedDict):
    """Post to a channel, joining it if necessary. Reply: ``posted``."""

    type: Literal["post"]
    req_id: ReqId
    session_id: str
    channel: str
    body: str  # at most 16 KiB of UTF-8
    intent: NotRequired[Intent]  # default "fyi"
    # Default "normal". The server lowers it to the account's maximum.
    severity: NotRequired[Severity]
    mentions: NotRequired[list[str]]  # handles, bare or "handle@account"


class Dm(TypedDict):
    """Reply: ``posted``."""

    type: Literal["dm"]
    req_id: ReqId
    session_id: str
    to: list[str]  # handles, bare or "handle@account"
    body: str  # at most 16 KiB of UTF-8
    intent: NotRequired[Intent]  # default "request"
    # Default "normal". The server lowers it to the account's maximum.
    severity: NotRequired[Severity]


class SetThreshold(TypedDict):
    """Reply: ``agent_ok`` with no fields."""

    type: Literal["set_threshold"]
    req_id: ReqId
    session_id: str
    context: str  # a topic key from list_subscriptions
    threshold: Threshold


class SetChannelTopic(TypedDict):
    """Reply: ``agent_ok`` with no fields."""

    type: Literal["set_channel_topic"]
    req_id: ReqId
    session_id: str
    channel: str
    topic: str


class ListChannels(TypedDict):
    """Reply: ``channel_list``. Registration is not necessary."""

    type: Literal["list_channels"]
    req_id: ReqId
    session_id: NotRequired[str | None]


class ListSubscriptions(TypedDict):
    """List this agent's channels and DMs. Reply: ``subscription_list``."""

    type: Literal["list_subscriptions"]
    req_id: ReqId
    session_id: str


class React(TypedDict):
    """Reply: ``agent_ok`` with ``id``. The sender must be a member."""

    type: Literal["react"]
    req_id: ReqId
    session_id: str
    target: str  # "#N", "N", or a message id
    reaction: str  # 1-64 chars, no newline


class MessageStatus(TypedDict):
    """Reply: ``message_status_result``."""

    type: Literal["message_status"]
    req_id: ReqId
    session_id: str
    target: str


class Whoami(TypedDict):
    """Describe this connection's account and session. Reply: ``whoami_result``."""

    type: Literal["whoami"]
    req_id: ReqId


ClientFrame = (
    Register
    | Ack
    | Schedule
    | ListScheduled
    | SubscribePr
    | UnsubscribePr
    | RefreshPr
    | ListPrSubscriptions
    | SubscribeForgejoPr
    | UnsubscribeForgejoPr
    | RefreshForgejoPr
    | ListForgejoPrSubscriptions
    | RegisterAgent
    | UnregisterAgent
    | ListAgents
    | SetAvailability
    | JoinChannel
    | LeaveChannel
    | Post
    | Dm
    | SetThreshold
    | SetChannelTopic
    | ListChannels
    | ListSubscriptions
    | React
    | MessageStatus
    | Whoami
)

# --------------------------------------------------------------------------- #
# Server -> client frames
# --------------------------------------------------------------------------- #


class Notify(TypedDict):
    """Show ``content`` to the agent, then send ``ack`` with ``id``."""

    type: Literal["notify"]
    id: str
    content: str
    meta: Annotated[NotifyMeta, Open]


class Scheduled(TypedDict):
    type: Literal["scheduled"]
    req_id: ReqId
    id: str
    due_at: float


class ListResult(TypedDict):
    type: Literal["list_result"]
    req_id: ReqId
    items: list[ScheduledItem]


class Subscribed(TypedDict):
    type: Literal["subscribed"]
    req_id: ReqId
    pr: str
    summary: str
    merged: bool
    # True if the PR is already merged or closed, so the subscription is not kept.
    closed: bool
    instance: NotRequired[str]  # named Forgejo instances only


class Unsubscribed(TypedDict):
    type: Literal["unsubscribed"]
    req_id: ReqId
    pr: str
    instance: NotRequired[str]  # named Forgejo instances only


class SubscriptionsResult(TypedDict):
    type: Literal["subscriptions_result"]
    req_id: ReqId
    items: list[PrSubscription]


class PrRefreshed(TypedDict):
    type: Literal["pr_refreshed"]
    req_id: ReqId
    pr: str
    subscribers: int
    polled: bool
    reason: NotRequired[str]  # present when polled is false
    instance: NotRequired[str]  # named Forgejo instances only


class AgentOk(TypedDict):
    """Success reply. Which field is present depends on the request:
    ``agent`` for register_agent, set_availability, and unregister_agent
    (None if the session was not registered); ``id`` (the reaction's message
    id) for react; neither for leave_channel, set_threshold, and
    set_channel_topic."""

    type: Literal["agent_ok"]
    req_id: ReqId
    agent: NotRequired[AgentRecord | None]
    id: NotRequired[str]


class AgentList(TypedDict):
    type: Literal["agent_list"]
    req_id: ReqId
    agents: list[AgentListEntry]


class ChannelJoined(TypedDict):
    type: Literal["channel_joined"]
    req_id: ReqId
    channel: str  # bare name, without "chan:"
    members: list[str]
    topic: str
    history: list[Message]


class Posted(TypedDict):
    type: Literal["posted"]
    req_id: ReqId
    id: str
    ordinal: int
    context: str
    members: int


class ChannelList(TypedDict):
    type: Literal["channel_list"]
    req_id: ReqId
    channels: list[ChannelInfo]


class SubscriptionList(TypedDict):
    type: Literal["subscription_list"]
    req_id: ReqId
    subscriptions: list[TopicSubscription]


class MessageStatusResult(TypedDict):
    type: Literal["message_status_result"]
    req_id: ReqId
    delivered: list[str]
    pending: list[str]
    # One entry for each name in delivered and pending.
    recipients: dict[str, RecipientInfo]
    reactions: list[Reaction]


Permission = Literal["messaging", "schedule", "pr"]


class AccountLimits(TypedDict):
    """Limits for the account (sessions, posts_per_minute) and for each of its
    sessions (the others). None means no limit."""

    sessions: int | None
    posts_per_minute: int | None
    channels: int | None
    pr_subscriptions: int | None
    callbacks: int | None


class WhoamiResult(TypedDict):
    type: Literal["whoami_result"]
    req_id: ReqId
    account: str
    label: str
    default_account: str
    permissions: list[Annotated[Permission, Open]]
    # Channel name patterns ("name", "prefix*"). None means every channel.
    channels: list[str] | None
    max_severity: Severity
    limits: Annotated[AccountLimits, Open]
    # None if the connection is not attached to a session.
    session_id: str | None
    handle: str  # "" if the session holds none
    connections: int  # connections attached to the session


class Error(TypedDict):
    """A failed request. ``req_id`` is present if the request had one."""

    type: Literal["error"]
    req_id: NotRequired[ReqId]
    error: str


ServerFrame = (
    Notify
    | Scheduled
    | ListResult
    | Subscribed
    | Unsubscribed
    | SubscriptionsResult
    | PrRefreshed
    | AgentOk
    | AgentList
    | ChannelJoined
    | Posted
    | ChannelList
    | SubscriptionList
    | MessageStatusResult
    | WhoamiResult
    | Error
)
