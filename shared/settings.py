from pydantic import Field
from pydantic_settings import BaseSettings


class GatewaySettings(BaseSettings):
    redis_host: str = Field(default="localhost", validation_alias="ARISE_REDIS_HOST")
    redis_port: int = Field(default=6379, validation_alias="ARISE_REDIS_PORT")


class WorkerSettings(BaseSettings):
    redis_host: str = Field(default="localhost", validation_alias="ARISE_REDIS_HOST")
    redis_port: int = Field(default=6379, validation_alias="ARISE_REDIS_PORT")

    pg_host: str = Field(validation_alias="PGHOST")
    pg_port: int = Field(default=5432, validation_alias="PGPORT")
    pg_user: str = Field(validation_alias="PGUSER")
    pg_password: str = Field(validation_alias="PGPASSWORD")
    pg_database: str = Field(validation_alias="PGDATABASE")

    mongo_uri: str = Field(default="mongodb://localhost:27017", validation_alias="MONGO_URI")
