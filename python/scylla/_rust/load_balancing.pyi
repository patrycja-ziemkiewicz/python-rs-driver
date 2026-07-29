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

