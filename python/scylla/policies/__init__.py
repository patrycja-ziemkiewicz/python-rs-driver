"""
Policies that tune how the driver routes, retries and timestamps requests.

This package only groups the policy modules and exports nothing itself.
Import each policy API from its focused submodule:

- ``scylla.policies.load_balancing`` - choosing the node and shard for a request.
- ``scylla.policies.retry`` - deciding whether a failed request is retried.
- ``scylla.policies.speculative_execution`` - sending extra requests to cut tail latency.
- ``scylla.policies.host_filter`` - which discovered nodes the driver connects to.
- ``scylla.policies.address_translator`` - rewriting the addresses nodes advertise.
- ``scylla.policies.timestamp_generator`` - client-side timestamps for requests.
"""
