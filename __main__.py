import sys
import config
import client
import logging

logging.root.handlers = []
logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.DEBUG , filename='tuning_session.log')

# set up logging to console
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)

api_key = config.API_KEY
db_id = config.DB_ID

if len(sys.argv) == 3:
    api_key = sys.argv[1]
    db_id = sys.argv[2]
elif len(sys.argv) == 2:
    db_id = sys.argv[1]
client.establish_database_connection(api_key, db_id)