FROM continuumio/miniconda3

COPY mattermost-bot.yml .
RUN conda env create -f mattermost-bot.yml

RUN apt-get update
RUN apt-get install -y locales locales-all
ENV LC_ALL de_DE.UTF-8
ENV LANG de_DE.UTF-8
ENV LANGUAGE de_DE.UTF-8

WORKDIR /app
COPY . /app

SHELL ["conda", "run", "-n", "mattermost-bot", "/bin/bash", "-c"]

ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "mattermost-bot", "python", "bot.py"]