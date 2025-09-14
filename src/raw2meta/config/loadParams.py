import yaml
from pathlib import Path
from typing import Dict

PACKAGE_LOCATION = Path(__file__).resolve().parents[3]

def load_params() -> Dict:
    """Load parameters from params.yaml
    :return: Dictionary of parameters.
    :rtype: Dict
    """
    params_file = PACKAGE_LOCATION / "params.yaml"
    
    if params_file.exists():
        with open(params_file, 'r') as f:
            return yaml.safe_load(f)
    return {}