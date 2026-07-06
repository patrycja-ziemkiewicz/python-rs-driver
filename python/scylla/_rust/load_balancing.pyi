from .cluster import ClusterState, Node
from .enums import Consistency, SerialConsistency
from .routing import Shard, Token

class NodeLocationPreference:
    """
    Describes the preferred location of nodes to contact when executing requests.

    This preference influences the order in which nodes appear in load balancing
    plans. Nodes matching the preference are considered "local" and are tried
    first, while non-matching nodes are considered "remote".

    Use ``NodeLocationPreference.ANY`` to explicitly treat all nodes equally,
    ``NodeLocationPreference.datacenter("dc1")`` to prefer a specific datacenter,
    or ``NodeLocationPreference.datacenter_and_rack("dc1", "rack1")`` to prefer
    a specific datacenter and rack.
    """

    ANY: NodeLocationPreference
    """No location preference — all nodes are treated equally."""

    @staticmethod
    def datacenter(name: str) -> NodeLocationPreference:
        """Prefer nodes located in the given datacenter."""

    @staticmethod
    def datacenter_and_rack(datacenter_name: str, rack_name: str) -> NodeLocationPreference:
        """
        Prefer nodes located in the given datacenter and rack.

        Nodes in the specified rack of the specified datacenter are tried first,
        followed by other nodes in the same datacenter, and finally nodes in
        remote datacenters.
        """

    @property
    def preferred_datacenter(self) -> str | None:
        """The preferred datacenter, or None if no datacenter preference is set."""

    @property
    def preferred_rack(self) -> str | None:
        """The preferred rack, or None if no rack preference is set."""

class RoutingInfo:
    """
    Represents info about statement that can be used by load balancing policies.
    """

    @property
    def consistency(self) -> Consistency:
        """Consistency level for the request."""

    @property
    def serial_consistency(self) -> SerialConsistency | None:
        """Serial consistency level to be used for serial part of the request, if set."""

    @property
    def token(self) -> Token | None:
        """
        Token that is the basis of token-aware routing.

        When present, it identifies the token used to choose replicas for
        vnode-based or tablet-based routing.
        """

    @property
    def keyspace(self) -> str | None:
        """Keyspace that the request is being executed against, if known."""

    @property
    def table(self) -> str | None:
        """Table that the request is being executed against, if known."""

    @property
    def is_confirmed_lwt(self) -> bool:
        """
        Whether prepare metadata confirmed that the statement is an LWT.

        If true, load balancing policies can route to replicas in a predefined
        order as a ScyllaDB-specific LWT routing optimisation. This flag alone
        is not sufficient to determine whether a request should be routed as
        LWT: a statement can also use Consistency.Serial or
        Consistency.LocalSerial as its consistency level.
        """

    @property
    def node_location_preference(self) -> NodeLocationPreference:
        """The session-level node location preference to pass to load balancing policies."""

    @property
    def preferred_rack(self) -> str | None:
        """The session-level rack preference to pass to load balancing policies."""
    @property
    def preferred_datacenter(self) -> str | None:
        """The session-level datacenter preference to pass to load balancing policies."""

class DefaultPolicy:
    """
    The default load balancing policy.

    It can be configured to be datacenter-aware, rack-aware, and token-aware.
    When the policy is datacenter-aware, you can configure whether to allow
    datacenter failover, which permits sending a query to a node from a remote
    datacenter.

    Node location preferences can be set via ``node_location_preference``.
    When ``None`` (the default), the session-level preference is used.
    When set to ``NodeLocationPreference.ANY``, all nodes are treated equally
    regardless of session-level preferences.

    Parameters
    ----------
    node_location_preference: NodeLocationPreference | None
        Node location preference for query routing. When ``None``, the
        session-level preference is used. When set, overrides the session-level
        preference.
    token_aware: bool
        Configures whether the policy takes tokens into consideration when
        creating plans. If this is true and token, keyspace, and table
        information are available, the policy prefers replicas and puts them
        earlier in the query plan.
    permit_dc_failover: bool
        Whether to permit remote nodes, meaning nodes not located in the
        preferred datacenter, in query plans. If no preferred datacenter is set,
        this has no effect.
    enable_shuffling_replicas: bool
        Whether replicas are shuffled when creating query plans. This helps
        distribute load across replicas. Disabling it can make routing more
        deterministic and may improve server-side cache locality.
    """

    def __init__(
        self,
        *,
        node_location_preference: NodeLocationPreference | None = None,
        token_aware: bool = True,
        permit_dc_failover: bool = False,
        enable_shuffling_replicas: bool = True,
    ) -> None: ...
    @property
    def node_location_preference(self) -> NodeLocationPreference | None: ...
    @property
    def preferred_datacenter(self) -> str | None: ...
    @property
    def preferred_rack(self) -> str | None: ...
    @property
    def token_aware(self) -> bool: ...
    @property
    def permit_dc_failover(self) -> bool: ...
    @property
    def enable_shuffling_replicas(self) -> bool: ...
    def pick_targets(
        self,
        routing_info: RoutingInfo,
        cluster_state: ClusterState,
    ) -> list[tuple[Node, Shard | None]]:
        """
        Returns an list of ``(Node, shard)`` tuples that are
        the preferred targets for the given request.
        """
