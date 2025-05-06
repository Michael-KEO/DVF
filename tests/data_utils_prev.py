import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_BASE = "https://dvf-api-5wd5.onrender.com"

def get_with_retry(url, timeout=30, retries=3):
    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=1, status_forcelist=[502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la récupération de {url} : {e}")
        return []

def get_data():
    biens = get_with_retry(f"{API_BASE}/biens")
    mutations = get_with_retry(f"{API_BASE}/mutations")
    localisations = get_with_retry(f"{API_BASE}/localisations")
    return biens, mutations, localisations

def merge_data():
    biens, mutations, localisations = get_data()

    # Création d'index pour joindre facilement
    mutation_dict = {m['ID_Mutation']: m for m in mutations}
    localisation_dict = {l['ID_Localisation']: l for l in localisations}

    merged = []
    for b in biens:
        mutation = mutation_dict.get(b['ID_Mutation'])
        localisation = localisation_dict.get(b['ID_Localisation'])
        if mutation and localisation:
            record = {**b, **mutation, **localisation}
            merged.append(record)
    return merged

