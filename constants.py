import logging
import os

LOG_FILE_PATH = "/var/log/auto_insights/application.log" 

# Ensure the log directory exists before setting up the file handler
log_directory = os.path.dirname(LOG_FILE_PATH)
if not os.path.exists(log_directory):
    os.makedirs(log_directory, exist_ok=True) 

# Configure the basic logging setup
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # This tells Python to write logs to your specified file
        logging.FileHandler(LOG_FILE_PATH, mode='a') # 'a' means append to the file
        # Optional: Add a StreamHandler if you also want logs to appear on the console
        # logging.StreamHandler() 
    ]
)

logger = logging.getLogger(__name__)

ENGINE_ID = None
DATAMART_ID = None
START_MONTH = None
END_MONTH = None
CNXN = None
CURSOR = None
LOGESYS_ENGINE = None
SOURCE_ENGINE = None
DF_RELATIONSHIP = None
SELECTED_INSIGHTS = None
DERIVED_MEASURES_DICT = None
DERIVED_MEASURES_DICT_EXPANDED = None
DF_SQL_TABLE_NAMES = None
DF_SQL_MEAS_FUNCTIONS = None
SIGNIFICANT_DIMENSIONS = None
SIGNIFICANT_MEASURES = None
DATE_COLUMNS = None
DATES_FILTER_DICT = None
OUTLIERS_DATES = None
DF_LIST = None
DF_LIST_LY = None
DF_LIST_TY = None
DF_LIST_LAST12MONTHS = None
DF_LIST_LAST52WEEKS = None
MAX_MONTH = None
MAX_YEAR = None
MAX_DATE = None
SIGNIFICANCE_SCORE = None
RENAME_DIM_MEAS = {}
DF_VERSION_NUMBER = None
INSIGHTS_TO_SKIP = ['Trends', 'Outliers', 'Monthly Anomalies', 'Weekly Anomalies']
DIM_ALLOWED_FOR_DERIVED_METRICS = {}
INSIGHTS_ALLOWED_FOR_DERIVED_METRICS = {}
SOURCE_TYPE = None
S3_CLIENT = None
S3_BUCKET_DERIVED_MEAS_FORMULA = "auto-insights-ask-db-cred-formula"