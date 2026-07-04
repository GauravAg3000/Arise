from pydantic import BaseModel


GATEWAY_ENDPOINT = "http://localhost:8000"


class ProduceConfig(BaseModel):
    rate: int = 100
    duration: str = "30s"
    batch_size: int = 100
    batch_timeout: str = "500ms"
    dry_run: bool = False
