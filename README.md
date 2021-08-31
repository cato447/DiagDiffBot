# DiagDiffBot

## Deployment

**:warning: Replace all {text} formatted variables with urls or credentials for your database and mattermost server**
### config

| key | parameters |
| ------ | ------ |
| **fhir** | **username**: username of account to connect to fhir with <br> **password**: password of account to connect to fhir with |
| **buttons** | **callback_url**: url the mattermost server calls when the button gets pressed <br> **port**: port on which the postRequestListener listens |
| **postRequestHandler** |  **url** : domain to create an http server to listen for incoming post request by the mattermost server (usually localhost)|
| **botConfigs** | **url**: url of mattermost server <br> **port**: port of mattermost server <br> **token**: token of the bot for authentication <br> **ssl_verify**: set to True if the mattermost server uses the https protocol |

### Docker

```bash
$ docker build -t {name} .
$ docker run -d {name} # container needs to be able to listen for incoming post requests from the mattermost server if buttons are enabled
```

## Currently supported commands

| command | function |
|---|---|
| help | list all available commands |
| ids | all radiology ids to the users name |  
| Name | name of the user | 
| Status -[0-9]/ heute/ gestern | table with stats for the given day |  
| Diff -[0-9]/ heute/ gestern | shows diff of the top five changed reports |
| Rückblick Woche | graph of the accuracy of the reports from the past seven days |
| Status dieser Monat /letzter Monat/ -[0-9] Monat | table with stats for the given month |
| Rückblick | graph of the accuracy of the reports from the last three months (slow) |  
| Anzahl Berichte gesamt | report count of the user |

## Project structure

| function | file |
| ------ | ------ |
| main | bot.py |
| event response | fhir_plugin.py |
| database calls | plugin_helpers.py<br>custom_fhir_plugin.py<br>fhir_plugin.py |

## Development

This bot is based on the [mmpy_bot](https://github.com/attzonko/mmpy_bot) developed by attzonko and his team

### Configs
#### Bot
  * prodBot: used if you want the bot to connect to your production mattermost server
  * localBot: used if you want the bot to connect to your local mattermost server on http://localhost:8065
  * localBotDocker: used if you want the bot to be deployed on the same machine as the test mattermost server <br>
  Both docker containers need to be able to see each other over a [docker bridge network](https://docs.docker.com/network/bridge/)

### fhir_client
* fhir_client.py is a fhir client for python developed by the SHIP Team at UK Essen and lives in a non public repo

### Development enviorment
* Build the enviorment from the mattermost-bot.yml file with conda
* To test the bot locally (adding network stuff, etc.) you need to run your own instance of the mattermost server.
Follow the [tutorial](https://docs.mattermost.com/install/setting-up-local-machine-using-docker.html) from the mattermost website or run this command with docker installed <br>
  ```bash
  docker run --name mattermost-preview -d --publish 8065:8065 --add-host dockerhost:127.0.0.1 mattermost/mattermost-preview
  ```

### Plugins
* [Plugins](https://mmpy-bot.readthedocs.io/en/latest/plugins.html) are used to add functionality to the bot

### Addidtional Ideas
* Oberarztmodus: "Dashboard" for Oberärzte
* Link Dicom pictures to the diff of reports

#### If you have any questions feel free to contact me at simon.bussmann@tum.de
