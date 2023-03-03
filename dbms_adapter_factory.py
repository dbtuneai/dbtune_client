from adapters.ubuntu_pg_adapter import UbuntuPgAdapter

class AdapterFactory:
    @staticmethod
    def get_adapter(adapter_data=None):
        if adapter_data is None:
            raise ValueError("adapter_data missing")
        if "engine" not in adapter_data:
            raise ValueError("DBMS engine missing in adapter_data")
        if adapter_data["engine"] == "postgresql":
            return UbuntuPgAdapter(adapter_data)
        raise ValueError("DBMS engine {} not supported".format(adapter_data["engine"]))
