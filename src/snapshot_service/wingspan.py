from org_utils.wingspan_utils import get_payee_flat_data
from dotenv import load_dotenv
import hashlib

load_dotenv()


def fetch_payee_data():
    df = get_payee_flat_data()
    fingerprint = df.to_json().encode("utf-8")
    sha = hashlib.sha256(fingerprint).hexdigest()
    return df, sha
