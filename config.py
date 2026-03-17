from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    AFM_PG_DSN: str

    model_config = ConfigDict(env_file=".env")
    
    

settings = Settings()
print(settings.AFM_PG_DSN)