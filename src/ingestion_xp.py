import requests
import os
import json
import pandas as pd
from datetime import datetime
from pandas.tseries.offsets import BDay

from dotenv import load_dotenv
load_dotenv()

# Get 