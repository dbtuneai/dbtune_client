from connectors.linux_pg_connector import LinuxPgConnector

class ConnectorFactory:
    @staticmethod
    def get_connector(connector_data=None):
        if connector_data is None:
            raise ValueError("connector_data missing")
        if "engine" not in connector_data:
            raise ValueError("DBMS engine missing in connector_data")
        if connector_data["engine"] == "postgresql":
            return LinuxPgConnector(connector_data)
        raise ValueError("DBMS engine {} not supported".format(connector_data["engine"]))
