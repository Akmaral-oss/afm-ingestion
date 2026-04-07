from app.ingestion.adapters.kaspi_adapter import KaspiAdapter
from app.ingestion.adapters.halyk_adapter import HalykAdapter


def load_adapters():

    return [
        KaspiAdapter(),
        HalykAdapter(),
    ]