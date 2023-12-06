import asyncio
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile, NoHostAvailable, ResultSet
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import (
    dict_factory,
    ordered_dict_factory,
    named_tuple_factory,
    ConsistencyLevel,
    PreparedStatement,
    BatchStatement,
    SimpleStatement,
    BatchType
)
from cassandra.policies import (
    DCAwareRoundRobinPolicy,
    DowngradingConsistencyRetryPolicy,
    # RetryPolicy
)
try:
    from cassandra.io.libevreactor import LibevConnection
    LIBEV = True
except ImportError:
    LIBEV = False


def main():
    params = {
        "host": "127.0.0.1",
        "port": "9042",
        "username": 'cassandra',
        "password": 'cassandra'
    }
    _auth = {
        "username": params["username"],
        "password": params["password"],
    }
    policy = DCAwareRoundRobinPolicy()
    defaultprofile = ExecutionProfile(
        load_balancing_policy=policy,
        retry_policy=DowngradingConsistencyRetryPolicy(),
        request_timeout=60,
        row_factory=dict_factory,
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
    )
    profiles = {
        EXEC_PROFILE_DEFAULT: defaultprofile
    }
    params = {
        "port": params["port"],
        "compression": True,
        "connection_class": AsyncioConnection,
        "protocol_version": 4,
        "connect_timeout": 60,
        "idle_heartbeat_interval": 0
    }
    auth_provider = PlainTextAuthProvider(**_auth)
    _cluster = Cluster(
        ["127.0.0.1"],
        auth_provider=auth_provider,
        execution_profiles=profiles,
        **params,
    )
    print(_cluster)
    try:
        connection = _cluster.connect(keyspace=None)
        print('CONNECTION > ', connection)
        response = connection.execute("SELECT release_version FROM system.local")
        result = [row for row in response]
        print(result)
    except NoHostAvailable as ex:
        raise RuntimeError(
            f'Not able to connect to any of the Cassandra contact points: {ex}'
        ) from ex


if __name__ == "__main__":
    try:
        main()
    finally:
        pass
