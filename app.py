from warroom_backend.app import create_app

app = create_app()

if __name__ == "__main__":
    from warroom_backend.config import Settings

    settings = Settings()
    app.run(host=settings.host, port=settings.port, debug=settings.debug)
