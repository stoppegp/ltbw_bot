from ltgetter import ltgetter
from mattermost_adapter import mattermost_adapter
from datetime import datetime
from ltbw_bot_config import *

ltgetter(database_name, start_date)
mattermost_adapter(database_name, start_date)