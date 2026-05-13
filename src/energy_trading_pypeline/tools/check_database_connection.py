from sqlalchemy import make_url, text

from energy_trading_pypeline.config import get_settings
from energy_trading_pypeline.persistence.db import engine


def main() -> None:
    settings = get_settings()
    url = make_url(settings.database_url)

    print("APP_ENV:", settings.app_env)
    print("DATABASE URL:", url.render_as_string(hide_password=True))

    with engine.connect() as connection:
        result = connection.execute(text("SELECT current_database(), current_user"))
        database_name, database_user = result.one()

    print(f"Connected to database '{database_name}' as user '{database_user}'")


if __name__ == "__main__":
    main()