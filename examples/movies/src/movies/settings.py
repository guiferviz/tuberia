from tuberia.settings import Settings


class MoviesSettings(Settings):
    input_credits_path: str
    input_movies_path: str
    database: str
    database_dir: str
