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
