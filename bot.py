import logging
from mmpy_bot import Bot, Settings
from Plugins import fhir_plugin, test_plugin
import configparser

def run_bot(profile: str, plugins: list):
    bot = Bot(
        settings=Settings(
            MATTERMOST_URL = config.get(profile, "url"),
            MATTERMOST_PORT = config.getint(profile, "port"),
            BOT_TOKEN = config.get(profile, "token"),
            SSL_VERIFY= config.getboolean(profile, "ssl_verify"),
            LOG_FILE="diagDiffBot.log",
            DEBUG=True
        ),
        plugins=plugins
    )
    logging.getLogger().handlers.pop() #stdout and stderr get mounted remove stderr to circumvent double logging
    bot.run()

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("bot_config.ini")
    run_bot("prodBot", [fhir_plugin.FhirPlugin(config)])