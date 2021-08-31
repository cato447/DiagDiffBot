import datetime
import time
from dateutil.relativedelta import relativedelta
import os

import difflib

import logging

from Helpers import fhir_client_custom

from Network import postRequestHandler
from queue import Queue

from mmpy_bot import Plugin, listen_to
from mmpy_bot import Message
import pandas as pd
import numpy as np
import matplotlib

matplotlib.use('Agg') # important for non blocking operation
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mtick

from pathlib import Path
import re
from Helpers.plugin_helpers import Helpers

import seaborn as sns

import locale

class FhirPlugin(Plugin):
    def __init__(self, config):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.client = fhir_client_custom.CustomFhirClient()
        locale.setlocale(locale.LC_ALL, 'de_DE')
        self.config = config
        self.client.login(self.config.get('fhir', 'username'), self.config.get('fhir', 'password'))
        self.helpers = Helpers(self.client)
        sns.set_theme()
        self.BUTTONS_ENABLED = False

    # Adds capability to listen for button presses (probably easier solvable but i don't know about it)
    def on_start(self):
        if self.BUTTONS_ENABLED:
            self.buttonMessageQueue = Queue()
            self.driver.threadpool.add_task(postRequestHandler.PostRequestHandler().run, 
                                            self.buttonMessageQueue, 
                                            self.driver.threadpool.alive,
                                            self.config.get("postRequestHandler", "url"),
                                            self.config.getint("buttons", "port"))
            self.driver.threadpool.add_task(self.__handleKorrekturButton, self.buttonMessageQueue)
            self.driver.threadpool.add_task(self.__cleanKorrekturCache)
        self.driver.threadpool.add_task(self.__keep_fhir_alive)
        return super().on_start()

    def __cleanKorrekturCache(self):
        self.korrekturCache = {}
        cache_duration = 1800 # in s -> every 30 mins
        while self.driver.threadpool.alive:
            time.sleep(cache_duration)
            cleared_entries = 0
            for key in list(self.korrekturCache.keys()):
                if (datetime.datetime.now() - self.korrekturCache[key][2]).total_seconds() > cache_duration:
                    cleared_entries += 1
                    self.driver.posts.patch_post(key, options={"props":{}})
                    del self.korrekturCache[key]
            self.logger.info(f"Cleared {cleared_entries} entries from KorrekturCache")

    def __handleKorrekturButton(self, queue):
        self.logger.info("Starting Korrektur_Button_Handler")
        while self.driver.threadpool.alive or not queue.empty():
            message = dict(queue.get())
            if not message:
                print("Weird interaction empty post request")
            else:
                self.driver.posts.patch_post(message['post_id'], options={'props':{}})
                try:
                    if message['context']['action'] == "True":
                        try:
                            if message['post_id'] in list(self.korrekturCache.keys()):
                                original_Message, mostCorrectedReports, _ = self.korrekturCache[message['post_id']]
                                del self.korrekturCache[message['post_id']]
                                try:
                                    self.respond_with_five_worst_reports(original_Message, mostCorrectedReports)
                                except:
                                    self.logger.exception('')
                            else:
                                self.logger.error(f"No corresponding entry in cache for {message['post_id']}")
                                self.driver.create_post(message['channel_id'], "Hmm ich kann deine korrigierten Berichte nicht finden :/\nStelle deine Abfrage nochmal. Dann finde ich sie. Versprochen! :D")
                        except:
                            self.logger.info("Clicked Yes: message has no key called 'post_id'")
                    else:
                        try:
                            if message['post_id'] in list(self.korrekturCache.keys()):
                                self.logger.info("Deleting corresponding cache entry")
                                del self.korrekturCache[message['post_id']]
                        except:
                            self.logger.info("Clicked No: message has no key called 'post_id'")
                except:
                    self.logger.info("message has no ['context']['action'] key")
        self.logger.info("Queue is empty and event got set. Stopping Handler")
    
    def __addKorrekturButton(self, message: Message, lastPostId, messageContent, mostCorrectedReports):
        #instructions for buttons
        props = {
            "attachments": [
                {
                    "title" : "Zeige Top 5 korrigierte Berichte",
                    "actions": [
                        {
                            "id": "yes",
                            "name": "Ja",
                            "integration": {
                                "url": f"{self.config.get('buttons', 'callback_url')}:{self.config.get('buttons', 'port')}",
                                "context": {
                                "action": "True"
                                }
                            },
                        },
                        {
                            "id": "no",
                            "name": "Nein",
                            "integration": {
                                "url": f"{self.config.get('buttons', 'callback_url')}:{self.config.get('buttons', 'port')}",
                                "context": {
                                "action": "False"
                                }
                            },
                        },
                    ]
                }
            ]
        }
        self.driver.posts.patch_post(lastPostId, options={'message': messageContent, 'props': props})
        # get id of posted post to remove the buttons after interaction or timeout
        lastPostId = self.driver.posts.get_posts_for_channel(message.channel_id)['order'][0]
        self.korrekturCache[lastPostId] = (message, mostCorrectedReports, datetime.datetime.now())

    def __keep_fhir_alive(self):
        refresh_duration = 1440 # in s -> every 4 hours
        while self.driver.threadpool.alive:
            time.sleep(refresh_duration)
            self.client.login(self.config.get('fhir', 'username'), self.config.get('fhir', 'password'))
            self.logger.info("Refreshing FHIR token")

    def __getEmail(self, message: Message):
        id = message.user_id
        user_info = self.driver.get_user_info(id)
        return user_info['email']

    def __getIDs(self, message: Message, suppressWarnings=False):
        email = self.__getEmail(message)
        given, family = self.helpers.getFullName(email)
        ids = self.helpers.get_all_practitioner_ids_for_name(given, family)
        if len(ids) == 0:
            if not suppressWarnings:
                self.driver.reply_to(message, f"##### :warning: Keine Radiologen-IDs für den Namen {given} {family} im System")
            return None
        else:
            return ids

    def __getData(self, message: Message, dateString, date=None, suppressNoReportsMessage=False, supressIDWarnings=False):
        ids = self.__getIDs(message, suppressWarnings=supressIDWarnings)
        if ids == None:
            given, family = self.helpers.getFullName(self.__getEmail(message))
            self.logger.error(f"No radiology id found for {given} {family}")
            return None, None, None
        lengths_geschrieben, lengths_freigegeben = self.helpers.get_count_reports_per_code(
            ids, dateString)
        # Handle no reports written
        if lengths_geschrieben == [0, 0, 0, 0]:
            if not suppressNoReportsMessage:
                if date is not None:
                    self.driver.reply_to(
                        message,
                        f"##### :warning: Es wurden keine Berichte am {date.strftime('%A')}, dem {date.strftime('%d.%m.%Y')} verfasst!"
                    )
                else:
                    self.driver.reply_to(
                        message,
                        "##### :warning: Es wurden keine Berichte geschrieben"
                    )
            return None, date, None
        not_same, scores, mostCorrectedReports = self.helpers.get_score_per_code(ids, dateString)
        # Handle no match of amount of report histories and reports per code (some reports don't have histories)
        if lengths_geschrieben != not_same:
            if scores is not None: #* [Fall 1]
                len_scores = [0,0,0,0]
                # Add reports that don't have a history to the similarity_score
                for index, length_freigegeben in enumerate(lengths_freigegeben):
                    distance = length_freigegeben - len(scores[index])
                    for _ in range(distance):
                        scores[index] = scores[index].append(pd.Series([1]))
                    len_scores[index] = len(scores[index])
                if len_scores != lengths_freigegeben:
                    raise Exception(f"{len_scores} != {lengths_freigegeben}")
                oa_korrektur = [1 - score.mean() for score in scores]
            # Handle no report histories present
            else:
                oa_korrektur = [0 if length_freigegeben == 0 else 1 for length_freigegeben in lengths_freigegeben]
        # Handle all reports having a report history
        else:
            oa_korrektur = [1 - score.mean() for score in scores]

        data = pd.DataFrame(zip(lengths_geschrieben, lengths_freigegeben,
                                oa_korrektur),
                            index=['CT', 'MRT', 'KONV', 'US'],
                            columns=['Geschrieben', 'OA Freigegeben', 'Durchschnitt OA Korrektur'])
        self.logger.info(f"Calculated data from {date} for {self.__getEmail(message)}")
        return data, date, mostCorrectedReports

    def __getStatusForDay(self, message: Message, number=None, suppressNoReportsMessage=False):
        if number == None:
            number = self.helpers.extract_num_from_command(message)
        date = datetime.datetime.now().date() - datetime.timedelta(number)
        dateString = date.strftime('%Y-%m-%d')
        return self.__getData(message=message, dateString=dateString, date=date, suppressNoReportsMessage=suppressNoReportsMessage)

    def __getStatusForMonth(self, message: Message, lowerBound, higherBound, suppressNoReportsMessage=False, supressIDWarnings=False):
        date_today = datetime.datetime.now()
        date_first_day_month = date_today.replace(day=1)
        if lowerBound == 0:
            date_lowerBound=date_first_day_month
            date_higherBound = date_today
            dateString = f"ge{date_lowerBound.strftime('%Y-%m-%d')}&date=le{date_higherBound.strftime('%Y-%m-%d')}"
        else:
            if higherBound > lowerBound:
                raise Exception(f"{higherBound} > {lowerBound} is not allowed -> swap the values")
            if higherBound == lowerBound:
                raise UserWarning("Wrong values for month")
            date_lowerBound = date_first_day_month + relativedelta(months=-lowerBound)
            if higherBound == 0:
                date_higherBound = date_first_day_month + relativedelta(days=-1)
            else:
                date_higherBound = date_first_day_month + relativedelta(months=-higherBound) + relativedelta(days=-1)
            dateString = f"ge{date_lowerBound.strftime('%Y-%m-%d')}&date=le{date_higherBound.strftime('%Y-%m-%d')}"
        month_df, _, mostCorrectedReports = self.__getData(message=message, dateString=dateString, date=None, suppressNoReportsMessage=suppressNoReportsMessage)
        return date_lowerBound, month_df, mostCorrectedReports

    @listen_to("^Diff [-][0-9]+$|^Diff 0$|^Diff heute$|^Diff gestern$", re.IGNORECASE)
    def respond_with_five_worst_reports(self, message: Message, mostCorrectedReports=None):
        """
        Zeigt die fünf meistkorrigierten Berichte des Tages an
        """
        self.logger.info(f"Showing the five most corrected reports to {self.__getEmail(message)}")
        if mostCorrectedReports is None:
            _, date, mostCorrectedReports = self.__getStatusForDay(message)
            if mostCorrectedReports is None:
                return # Don't do anything if no Data is present
        if len(mostCorrectedReports[mostCorrectedReports['similarity_score'] == 1]) == 5:
            self.driver.reply_to(message, "### Unglaublich! :exploding_head:\n ##### Heute war alles bis auf den letzen Buchstaben richtig!:white_check_mark:\n ##### Herzlichen Glückwunsch :tada: :partying_face:")
            return
        #TODO: Refactor this hot mess
        self.driver.reply_to(message, f"### Top {min(5, len(mostCorrectedReports))} korrigierte Berichte")
        df = pd.DataFrame(mostCorrectedReports).convert_dtypes()
        for _, row in enumerate(df.itertuples()):
            if row.similarity_score < 1:
                diff = difflib.unified_diff(row.data_preliminary.splitlines(), row.data_final.splitlines(), n=0)
                tag = "\n".join([line for line in diff if len(line) > 5])
                sections = re.split("@@", tag)[1:]
                date = datetime.datetime.strptime(row.time_code_final, '%Y-%m-%dT%H:%M:%S.%f%z')
                msg = f"#### Code: {row.code} | Änderung: {'{0:.2f}%'.format((1 - row.similarity_score) * 100)} | Freigegeben: {date.strftime('%xT%X')}"
                for index in range(0, len(sections)-1, 2):
                    new = []
                    old = []
                    for line in sections[index+1].splitlines():
                        if re.match("^\-[^\-][\s\S]+", line):
                            old.append(line[1:])
                        elif re.match("^\+[^\+][\s\S]+", line):
                            new.append(line[1:])
                    old_series = pd.Series(old)
                    new_series = pd.Series(new)
                    diff_df = pd.DataFrame({"Gelöscht":old_series, "Neu hinzugefügt": new_series}).fillna("")
                    if not diff_df.empty:
                        msg += "\n" + f"##### Zeile {sections[index].split(' ')[1]} gelöscht, Zeile {sections[index].split(' ')[2]} hinzugefügt\n" + diff_df.to_markdown(index=False).replace("  ", "")
                self.driver.reply_to(message, msg)
            else:
                msg = f"#### Code: {row.code} | Änderung: {'{0:.2f}%'.format((1 - row.similarity_score) * 100)} | Freigegeben: {date.strftime('%xT%X')}"
                self.driver.reply_to(message, msg+"\n##### Keine Fehler! :tada: :partying_face: ")

    @listen_to("^ids$", re.IGNORECASE)
    def getIds(self, message: Message):
        """
        Radiologische PractitionerIDs des Anwenders (FHIR-Datenbank/Practitioner/{id})
        """
        self.logger.info(f"Processing command: {message.text} by {self.__getEmail(message)}")
        ids = self.__getIDs(message)
        if ids is not None: # [Fall 2]
            given, family = self.helpers.getFullName(self.__getEmail(message))
            self.driver.reply_to(message,f"#### Radiologen IDs für {given} {family}\n"+"\n".join(str(x) for x in ids))

    @listen_to("^Name$", re.IGNORECASE)
    def getName(self, message: Message):
        """
        Name des Anwenders
        """
        self.logger.info(f"Processing command: {message.text} by {self.__getEmail(message)}")
        email = self.__getEmail(message)
        given, family = self.helpers.getFullName(email)
        self.driver.reply_to(message, f"{given} {family}")

    @listen_to("^Anzahl Berichte gesamt$", re.IGNORECASE)
    def getCountReports(self, message: Message):
        """
        Anzahl der geschriebenen Berichte in der FHIR-Datenbank
        """
        self.logger.info(f"Processing command: {message.text} by {self.__getEmail(message)}")
        ids = self.__getIDs(message)
        if ids is None or len(ids) == 0:
            return
        self.driver.reply_to(message, "Berechne ...")
        lastPostId = self.driver.posts.get_posts_for_channel(message.channel_id)['order'][0]

        totalReports = 0
        firstDate = None
        for id in ids:
            formdata = dict(performer=id, _count=1, _sort="date")
            totalReports += self.client.totalMatches("DiagnosticReport",
                                                     formdata)
            date_first_report = self.client.get_date_of_first_report(formdata)
            if date_first_report is not None:
                date_first_report = datetime.datetime.strptime(
                    date_first_report.split("T")[0], "%Y-%m-%d")
                firstDate = date_first_report if firstDate is None or firstDate > date_first_report else firstDate
        self.driver.posts.patch_post(lastPostId, options={'message': f"Anzahl Berichte in FHIR ab {firstDate}:\n {totalReports}"})

    @listen_to("^Status letzter Monat$|^Status dieser Monat$|^Status Monat -[0-9]+$", re.IGNORECASE)
    def getStatisticForMonth(self, message: Message):
        """
        Status des Monats in Tabellenform

        Spalten:
            Geschrieben -- Anzahl geschriebene Berichte
            OA_Freigegeben -- Anzahl nicht selbst freigegebener Berichte
            OA_Korrektur -- Druchschnittliche prozentuelle Änderung der geschriebenen Berichte durch den/die Oberarzt*in
        """
        self.logger.info(f"Processing command: {message.text} by {self.__getEmail(message)}")
        self.driver.reply_to(message, "Berechne ...")
        lastPostId = self.driver.posts.get_posts_for_channel(message.channel_id)['order'][0]
        if "letzter" in message.text.lower():
            date_lowerBound, month_df, _ = self.__getStatusForMonth(message, 1, 0)
        elif "dieser" in message.text.lower():
            date_lowerBound, month_df, _ = self.__getStatusForMonth(message, 0, 0)
        elif "-" in message.text.lower():
            number = int(message.text.split("-")[1])
            date_lowerBound, month_df, _ = self.__getStatusForMonth(message, number, number-1)
        month_df['Durchschnitt OA Korrektur'] = [val if np.isnan(val) else "{0:.2f}%".format(val * 100) for val in month_df['Durchschnitt OA Korrektur']]
        month_df['Durchschnitt OA Korrektur'] = month_df['Durchschnitt OA Korrektur'].fillna('-')
        msg = f"#### Status für {date_lowerBound.strftime('%B')}:\n"+month_df.to_markdown()
        self.driver.posts.patch_post(lastPostId, options={'message': msg})

    @listen_to("^Status [-][0-9]+$|^Status 0$|^Status Heute$|^Status Gestern$", re.IGNORECASE)
    def getStatusTable(self, message: Message):
        """
        Status des Tages in Tabellenform
        ,
            Status -10 --> Status vor 10 Tagen
            Status 0 --> Status heute

        Spalten:
            Geschrieben -- Anzahl geschriebene Berichte
            OA_Freigegeben -- Anzahl nicht selbst freigegebener Berichte
            OA_Korrektur -- Durchschnitt der Korrekturhärte (0% ist besser als 10%)
        """
        try:
            #! DON'T PUT ANYTHING INFRONT OF THOSE THREE LINES! IF YOUR CODE IS FAULTY THE USER WONT'T GET A NOTICE THAT SOMETHING WENT WRONG!
            self.driver.reply_to(message, "Berechne ...")
            lastPostId = self.driver.posts.get_posts_for_channel(message.channel_id)['order'][0]
            self.logger.info(f"Processing command: {message.text} by {self.__getEmail(message)}")
            #! END OF DANGER ZONE
            data, date, mostCorrectedReports = self.__getStatusForDay(message)
            if data is not None:
                data['Durchschnitt OA Korrektur'] = [val if np.isnan(val) else "{0:.2f}%".format(val * 100) for val in data['Durchschnitt OA Korrektur']]
                data['Durchschnitt OA Korrektur'] = data['Durchschnitt OA Korrektur'].fillna('-')
                msg = f"##### Status {date.strftime('%A')}, {date.strftime('%d.%m.%Y')}\n" + data.to_markdown()
                if self.BUTTONS_ENABLED:
                    self.__addKorrekturButton(message, lastPostId, msg, mostCorrectedReports)
                else:
                    self.driver.posts.patch_post(lastPostId, options={'message': msg})

        except Exception as e:
            self.logger.exception("")
            self.driver.posts.patch_post(lastPostId, options={'message': "Oh nein! :scream:\nEtwas ist kaputt gegangen :sweat:\nPrüfe ob dein Name richtig geschrieben wurde mit Name oder probiers nochmal! :D\nFalls der Name falsch ist schreibe ein Email an das [SHIP team]()"})

    @listen_to("^Heute$|^Gestern$", re.IGNORECASE)

    def getFullInformation(self, message: Message):
        """
            Generelle Auswertung des heutigen oder gestrigen Tages
            (status heute und diff heute zusammen ausgeführt)
        """
        self.getStatusTable(message)
        self.respond_with_five_worst_reports(message)

    @listen_to("^Rückblick$", re.IGNORECASE)
    def review(self, message: Message):
        """
        Korrekturrate per Code der letzten drei Monate als Graph
        """
        self.logger.info(f"Processing command: {message.text} by {self.__getEmail(message)}")
        dataframe = pd.DataFrame(index=['CT', 'MRT', 'KONV', 'US'])
        for i in range(2, -1, -1):
            date_lowerbound ,month_df, _ = self.__getStatusForMonth(message, i, i-1, suppressNoReportsMessage=True, supressIDWarnings=True)
            if month_df is not None:
                dataframe[date_lowerbound] = month_df['Durchschnitt OA Korrektur'] * 100
            else:
                if self.__getIDs(message=message, suppressWarnings=True) is None:
                    return

        if not dataframe.empty:
            file = Path("/tmp/status_graph.png")
            switched_dataframe = dataframe.T
            switched_dataframe.index = pd.to_datetime(switched_dataframe.index)
            plot = switched_dataframe.plot(style='-o')
            colors = [line.get_color() for line in plot.lines] # get the colors of the markers
            switched_dataframe = switched_dataframe.interpolate(limit_area='inside') # interpolate
            lines = plot.plot(switched_dataframe.index, switched_dataframe.values) # add more lines (with a new set of colors)
            for color, line in zip(colors, lines):
                line.set_color(color) # overwrite the new lines colors with the same colors as the old lines
            plot.set_ylabel("Durchschnitt OA_Korrektur pro Monat")
            plot.yaxis.set_major_formatter(mtick.PercentFormatter())
            plot.xaxis.set_major_formatter(mdates.DateFormatter('%B %Y'))
            plot.xaxis.set_major_locator(mdates.MonthLocator())
            plt.savefig(file, bbox_inches='tight')
            self.driver.reply_to(message,
                                    f"##### Status der letzten drei Monate",
                                    file_paths=[file])
            os.remove(file)
        else:
            self.driver.reply_to(message, f"##### :warning: Es wurden keine Berichte in den letzten drei Monaten geschrieben")

    @listen_to("^Rückblick Woche$", re.IGNORECASE)
    def review_week(self, message: Message):
        """
        Korrekturrate per Code der letzten sieben Tage als Graph
        """
        self.logger.info(f"Processing command: {message.text} by {self.__getEmail(message)}")
        self.driver.reply_to(message, "Berechne ...")
        lastPostId = self.driver.posts.get_posts_for_channel(message.channel_id)['order'][0]
        dataframe = pd.DataFrame(index=['CT', 'MRT', 'KONV', 'US'])
        for i in range(6,-1,-1):
            day_df, date, _ = self.__getStatusForDay(message, number=i, suppressNoReportsMessage=True)
            if day_df is not None:
                dataframe[date] = day_df['Durchschnitt OA Korrektur'] * 100
            elif date is None:
                return
            else:
                dataframe[date] = pd.DataFrame([np.nan for i in range(4)])
        if not dataframe.isnull().values.all():
            file = Path("/tmp/status_graph.png")
            switched_dataframe = dataframe.T
            switched_dataframe.index = pd.to_datetime(switched_dataframe.index)
            plot = switched_dataframe.plot(style='-o')
            colors = [line.get_color() for line in plot.lines] # get the colors of the markers
            switched_dataframe = switched_dataframe.interpolate(limit_area='inside') # interpolate
            lines = plot.plot(switched_dataframe.index, switched_dataframe.values) # add more lines (with a new set of colors)
            for color, line in zip(colors, lines):
                line.set_color(color) # overwrite the new lines colors with the same colors as the old lines
            plot.set_ylabel("Durchschnitt OA_Korrektur pro Tag")
            plot.yaxis.set_major_formatter(mtick.PercentFormatter())
            plot.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
            plot.xaxis.set_major_locator(mdates.DayLocator())
            plot.autoscale(True)
            plt.savefig(file, bbox_inches='tight')
            self.driver.posts.patch_post(lastPostId, options={'message':"##### Status der letzten sieben Tage"})
            self.driver.reply_to(message, "", file_paths=[file])
            os.remove(file)
        else:
            self.driver.reply_to(message, f"##### :warning: Es wurden keine Berichte in den letzten sieben Tagen geschrieben")