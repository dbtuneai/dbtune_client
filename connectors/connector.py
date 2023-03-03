class Connector:
    def establish_connection(self):
        print("Must be overridden by subclass")

    def get_client_info(self):
        print("Must be overridden by subclass")