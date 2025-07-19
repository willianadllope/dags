import sys
import os
import shutil
from time import time
from datetime import timedelta, datetime
from sqlalchemy import create_engine
import pandas as pd

import scripts.config

configs = scripts.config.pastas

print('inicio')
print(configs['scripts']['task_group_chunks']);
print('fim')
