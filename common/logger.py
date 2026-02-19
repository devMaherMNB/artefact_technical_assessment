import logging
import sys

# Configure the root logger once for the entire project
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def getLogger(name):
    return logging.getLogger(name)
