import datetime
import time
from dateutil.relativedelta import relativedelta
import os

import difflib

import logging

from numpy.random.mtrand import noncentral_chisquare

from Network import postRequestHandler
from queue import Queue

from mmpy_bot import Plugin, listen_to
from mmpy_bot import Message
import pandas as pd
import numpy as np
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mtick

from pathlib import Path
import re

import seaborn as sns

import locale

import random

from difflib import SequenceMatcher

class TestPlugin(Plugin):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.logger = logging.getLogger(__name__)
        locale.setlocale(locale.LC_ALL, 'de_DE')
        sns.set_theme()
        self.preliminaryVersion = [ "Klinische Angaben: Doppelkarzinom C. ascendens und Rektum, ED 12\/2017. Gering differenziertes Adenokarzinom\r\nFragestellung: Verlaufskontrolle\r\n\r\nCT: Abdomen mit i.v. KM, 2-D-Rekonstruktion vom 06.04.2021:\r\nCT: Thorax mit i.v. KM, 2-D-Rekonstruktion vom 06.04.2021:\r\n\r\nZum Vergleich liegt zuletzt eine Voruntersuchung vom 13.01.2021 vor.\r\n\r\nBefund:\r\nThorax:\r\nKonstante subpleurale Verdichtung rechts ventral (ima 4\/38 2 mm, idem) a.e. unspezifisch. Kein Nachweis suspekter Rundherde. Dorsale Dystelektasen beidseits.\r\nKein Nachweis pathologisch vergr\u00f6\u00dferter Lymphknoten thorakal. Portsystem links epipektoral lagekorrekt.\r\n\r\nAbdomen:\r\nAerobillie bei konstant einliegendem Gallengangsstent im DHC und neuer Stent intrahepatisch. Weitgehend gr\u00f6\u00dfenkonstante Raumforderung in der Leberpforte (ima 4\/21 29 x 22 mm, vormals 28 x 23 mm) mit Kontakt zur VCI und mutma\u00dflicher Infiltration der V. portae ohne Verschluss des Gef\u00e4\u00dfes. Gr\u00f6\u00dfenkonstante Raumforderungen angrenzend an die Leber (ex ima 5\/151 16 x 12 mm idem neu gemessen, ima 5\/154 11 x 7 mm neu gemessen). Gr\u00f6\u00dfenkonstante Raumforderung angrenzend an die Milz (ima 4\/17 15 x 12 mm idem). Vermehrte Lymphknoten paraaortal ohne pathologische Vergr\u00f6\u00dferung.\r\nIm Rahmen der Messgenauigkeit weitgehend konstante Bauchwandmetastasen (ex ima 5\/284 40 x 29 mm idem) und mesenterial (ima 5\/181 30 x 28 mm, vormals 31 x 29 mm) und im kleinen Becken (ima 4\/69). \r\nProgredient distendierte Nierenbeckenkelchsysteme beidseits (ima 4\/34). Postoperative Ver\u00e4nderungen nach Kolonteilresektion und Anlage eines Anus praeter im linken Mittelbauch. A.e. narbige Ver\u00e4nderungen pr\u00e4sakral mit konstanter Lymphozele.\r\n\r\nSkelett:\r\nDegenerative Ver\u00e4nderungen ohne Nachweis malignit\u00e4tssuspekter oss\u00e4rer L\u00e4sionen.\r\n\r\nBeurteilung:\r\n1.\tWeitgehend konstante a.e. Lymphknotenmetastase in der Leberpforte mit Infiltration der Vena portae.\r\n2.\tKonstante peritoneale und omentale Weichteilmetastasen im Sinne einer Peritonealkarzinose sowie der ventralen Bauchwand und im kleinen Becken wahrscheinlich mit Kompression der Ureteren bei gering progredienter Nierenbeckendilation.\r\n3.\tKeine thorakale Tumormanifestation. \r\n",
                                    "Klinische Angaben: bekannte Raumforderung der linken Nebenniere, extern V.a. Ph\u00e4ochromozytom; klinisch jedoch keine Hinweise (Metanephrine normwertig). Bekannte L\u00e4sionen der rechten Lunge. Aktuell V.a. paraneoplastisches Syndrom mit Paraparese der Beine (PNP) DD spinale Raumforderung\r\nFragestellung: N\u00e4hre Einordnung der NN-Raumforderung?\r\n\r\nMRT des Abdomens vom 06.04.2021:\r\n\r\nBefund:\r\nExternes CT vom 6. November 2020 zum intermodalen Vergleich vorliegend (Chili).\r\n\r\nEingeschr\u00e4nkte Beurteilbarkeit aufgrund von Bewegungsartefakten.\r\n\r\nGr\u00f6\u00dfenkonstante Raumforderung der linken Nebenniere (ima 5\/12: 33 x 26 mm, idem) mit flauer Kontrastmittelanreicherung und deutlichem Signalabfall von In- zu Opposed-Phase; zentraler T2w-hyperintenser, T1w-hypointenser Anteil (ima 5\/11: 21 mm). Geringe Signalsteigerung des Leberparenchyms von In- zu Opposed-Phase. Weitgehend konstante Erweiterung der extra- und intrahepatischen Gallenwege, betont der Hepatikusgabel (ima 5\/20), sowie des proximalen Pankreasgangs (5\/30); kein Nachweis eines Abflusshindernisses. Wandverdickte Gallenblase, a.e. bei geringer F\u00fcllung. Nierenzysten beidseits. Teilerfasste gro\u00dfe zystische L\u00e4sion des linken Beckens (ima 3\/19). Kein Aszites. Keine pathologisch vergr\u00f6\u00dferten Lymphknoten. Degenerative Skelettver\u00e4nderungen. Keine malignomsuspekte oss\u00e4re L\u00e4sion.\r\n\r\nBeurteilung:\r\n1.\tGr\u00f6\u00dfenkonstante Raumforderung der linken Nebenniere, a.e. Adenom mit regressiven Ver\u00e4nderungen. Erneute Evaluation im Verlauf empfohlen.\r\n2.\tV.a. geringe Eisen\u00fcberlagerung der Leber.\r\n3.\tKonstante intra- und extrahepatische Cholestase ohne abgrenzbares Abflusshindernis.\r\n4.\tTeilerfasste bekannte gro\u00dfe zystische L\u00e4sion pelvin links, a.e. Ovarialzyste.\r\n\r\n",
                                    "Klinische Angaben: Multiples Myelom vom Leichtkettentyp unter Therapie. Hartn\u00e4ckige Schmerzen im Sternum und thorakal links (kardiol. Abkl\u00e4rung o.p.B.)\r\nFragestellung: Akute Fraktur? Raumforderung? Osteolysen?\r\n\r\nCT: Thorax nativ, 2-D-Rekonstruktion vom 06.04.2021:\r\n\r\nZum Vergleich liegt eine Voruntersuchung zuletzt vom 19.03.2021 vor.\r\n\r\nBefund:\r\nKeine suspekten pulmonalen Rundherde. Keine umschriebenen Infiltrate.\r\nKein Nachweis pathologisch vergr\u00f6\u00dferter Lymphknoten.\r\nKonstante Osteolysen in BWK 11 und BWK 12. Konstante Osteolysen in Manubrium und Corpus sterni (ex ima 8b\/31). Konstante Mehrsklerosierung der 7. Rippe links lateral (ima 5\/239). Bereits in der Voruntersuchung abgrenzbare Fraktur der 10 Rippe links lateral (ima 5\/347). A.e. konstante Osteolyse in der Clavicula links, etwas different erfasst (ima 5\/2). Die vorbeschriebene Osteolyse am Acromion rechts ist aktuell nicht miterfasst.\r\nMiterfasste Oberbauchorgane unauff\u00e4llig.\r\n\r\nBeurteilung:\r\n1.\tSoweit mit der Plasmocytomuntersuchung vom 19.03.2021 vergleichbar konstante Fraktur der 10. Rippe links lateral sowie konstante disseminierte Osteolysen.\r\n2.\tNativ kein Nachweis einer pulmonalen Tumormanifestation.\r\n"
                                  ]
        self.finalVersion = ["Klinische Angaben: Doppelkarzinom C. ascendens und Rektum, ED 12\/2017. Gering differenziertes Adenokarzinom\r\nFragestellung: Verlaufskontrolle\r\n\r\nCT: Abdomen mit i.v. KM, 2-D-Rekonstruktion vom 06.04.2021:\r\nCT: Thorax mit i.v. KM, 2-D-Rekonstruktion vom 06.04.2021:\r\n\r\nZum Vergleich liegt zuletzt eine Voruntersuchung vom 13.01.2021 vor.\r\n\r\nBefund:\r\nThorax:\r\nKonstante subpleurale Verdichtung rechts ventral (ima 4\/38 2 mm, idem) a.e. unspezifisch. Kein Nachweis suspekter Rundherde. Dorsale Dystelektasen beidseits.\r\nKein Nachweis pathologisch vergr\u00f6\u00dferter Lymphknoten thorakal. Portsystem links epipektoral lagekorrekt.\r\n\r\nAbdomen:\r\nAerobillie bei konstant einliegendem Gallengangsstent im DHC und neuer Stent intrahepatisch. \r\nDeutlich progrediente Tumormanifestation an der Leberpforte (ima 5\/107; 58 x 45 mm, vormals 37 x 36 mm, neu gemessen) mit zunehmender Ummauerung des Pfortaderhauptstamm mit (sub)totaler Kompression\/Infiltration sowie Einbezug der Abg\u00e4nge der Pfortaderhaupt\u00e4ste (bei Trifurkation) mit deutlicher Kompression\/Infiltration des linken Astes. Kompensatorisch prominentere portale Kollaterale. Die AHC, AHS und AGD (aus dem Tr. coeliacus) sind durch den Tumor anteilig ummauert aber durchg\u00e4ngig kontrastiert. Die AHD (aus der AMS entspringend) hat Kontakt zum Tumor und ist ebenfalls durchg\u00e4ngig kontrastiert. Ebenso zunehmende Infiltration der Leber betont bei S1. Einliegende Gallengangsstent beidseits ohne wesentlichen Aufstau.\r\n\r\nVorbekannte disseminierte peritoneale und omentale Tumormanifestationen mit Infiltration der Bauchwand. Im Vergleich zur Voruntersuchung zeigt sich eine \u00fcberwiegende Konstanz jedoch auch einzelne progrediente Herde, z.B. mit zunehmender Infiltration der Subkutis rechts an der Laparotomienarbe (ima 5\/264; nicht sinnvoll messbar). Vermehrte Lymphknoten paraaortal ohne pathologische Vergr\u00f6\u00dferung. \r\nGering akzentuiertes Nierenbeckenkelchsysteme beidseits (ima 4\/34), kein Harnstau. Postoperative Ver\u00e4nderungen nach Kolonteilresektion und Anlage eines Anus praeter im linken Mittelbauch. A.e. narbige Ver\u00e4nderungen pr\u00e4sakral mit konstanter Lymphozele.\r\n\r\nSkelett:\r\nDegenerative Ver\u00e4nderungen ohne Nachweis malignit\u00e4tssuspekter oss\u00e4rer L\u00e4sionen.\r\n\r\nBeurteilung:\r\nGegen\u00fcber 13.01.2021 zeigt sich ein Tumorprogress:\r\n\r\n1.\tDeutlich progrediente Tumormanifestation an der Leberpforte mit zunehmender Kompression und Infiltration der Pfortader und deren \u00c4ste sowie progrediente Leberinfiltration.\r\n2.\t\u00dcberwiegend konstante aber auch vereinzelt gering progrediente peritoneale und omentale Weichteilmetastasen im Sinne einer Peritonealkarzinose mit zunehmender Infiltration des subkutanen Fettgewebes im Mittelbauch.\r\n3.\tWeiterhin keine thorakale Tumormanifestation. \r\n",
                             "Klinische Angaben: bekannte Raumforderung der linken Nebenniere, extern V.a. Ph\u00e4ochromozytom; klinisch jedoch keine Hinweise (Metanephrine normwertig). Bekannte L\u00e4sionen der rechten Lunge. Aktuell V.a. paraneoplastisches Syndrom mit Paraparese der Beine (PNP) DD spinale Raumforderung\r\nFragestellung: N\u00e4hre Einordnung der NN-Raumforderung?\r\n\r\nMRT des Abdomens vom 06.04.2021:\r\n\r\nBefund:\r\nExternes CT vom 6. November 2020 zum intermodalen Vergleich vorliegend (Chili).\r\n\r\nEingeschr\u00e4nkte Beurteilbarkeit aufgrund von Bewegungsartefakten.\r\n\r\nWeitgehend gr\u00f6\u00dfenkonstante Raumforderung der linken Nebenniere (ima 5\/12: 33 x 26 mm, idem) mit flauer, inhomogener Kontrastmittelanreicherung, deutlichem Signalabfall in der Opposed-Phase und T2w-hyperintensen Anteilen (ima 5\/11: 21 mm). \r\nGeringer Signalabfall des Leberparenchyms in der Opposed-Phase. Weiterhin geringe Erweiterung der intrahepatischen Galleng\u00e4nge, bei jedoch weiter zunehmende Erweiterung des DHC (12mm zuvor 10mm), soweit intermodal vergleichbar.\r\nMR-morphologisch kein eindeutiger Nachweis eines Abflusshindernisses. Wandverdickte Gallenblase, a.e. bei geringer F\u00fcllung. Nierenzysten beidseits. Teilerfasste gro\u00dfe zystische L\u00e4sion des linken Beckens (ima 3\/19). Kein Aszites. Keine pathologisch vergr\u00f6\u00dferten Lymphknoten. Degenerative Skelettver\u00e4nderungen. Keine malignomsuspekte oss\u00e4re L\u00e4sion.\r\n\r\nBeurteilung:\r\n1.\tGr\u00f6\u00dfenkonstante Raumforderung der linken Nebenniere mit fettigen Anteilen, daher vermutlich einem Adenom entsprechend, ein Ph\u00e4ochromozytom kann jedoch nicht sicher ausgeschlossen werden. \r\n2.\tV.a. geringe Eisen\u00fcberlagerung der Leber.\r\n3.\tGeringe zunehmende Erweiterung des DHC ohne den Nachweis eines Abflusshindernisses. \r\n4.\tTeilerfasste bekannte gro\u00dfe zystische L\u00e4sion pelvin links, a.e. Ovarialzyste.\r\n\r\n",
                             "Klinische Angaben: Multiples Myelom vom Leichtkettentyp unter Therapie. Hartn\u00e4ckige Schmerzen im Sternum und thorakal links (kardiol. Abkl\u00e4rung o.p.B.)\r\nFragestellung: Akute Fraktur? Raumforderung? Osteolysen?\r\n\r\nCT: Thorax nativ, 2-D-Rekonstruktion vom 06.04.2021:\r\n\r\nZum Vergleich liegt eine Voruntersuchung zuletzt vom 19.03.2021 vor.\r\n\r\nBefund:\r\nKeine suspekten pulmonalen Rundherde. Keine umschriebenen Infiltrate.\r\nKein Nachweis pathologisch vergr\u00f6\u00dferter Lymphknoten.\r\nKonstante Osteolysen in BWK 11 und BWK 12. Konstante Osteolysen in Manubrium und Corpus sterni (ex ima 8b\/31). Konstante Mehrsklerosierung der 7. Rippe links lateral (ima 5\/239). Bereits in der Voruntersuchung abgrenzbare Fraktur der 10 Rippe links lateral (ima 5\/347). A.e. konstante Osteolyse in der Clavicula links, etwas different erfasst (ima 5\/2). Die vorbeschriebene Osteolyse am Acromion rechts ist aktuell nicht miterfasst. Fragliche Osteolyse ventrolateral in der 6. Rippe links (ima 5\/216).\r\nMiterfasste Oberbauchorgane unauff\u00e4llig.\r\n\r\nBeurteilung:\r\n1.\tSoweit mit der Plasmocytomuntersuchung vom 19.03.2021 vergleichbar konstante Fraktur der 10. Rippe links lateral sowie konstante disseminierte Osteolysen.\r\n2.\tIn Fulldose-Technik neu fassbare, fragliche Osteolyse ventrolateral in der 6. Rippe links.\r\n3.\tNativ kein Nachweis einer pulmonalen Tumormanifestation.\r\n"
                            ]


    # Adds capability to listen for button presses (probably easier solvable but i don't know about it)
    def on_start(self):
        self.buttonMessageQueue = Queue()
        self.korrekturCache = {}
        self.driver.threadpool.add_task(postRequestHandler.PostRequestHandler().run,
                                        self.buttonMessageQueue, 
                                        self.driver.threadpool.alive,
                                        self.config.get("postRequestHandler", "url"),
                                        self.config.getint("buttons", "port"))
        self.driver.threadpool.add_task(self.__handleKorrekturButton, self.buttonMessageQueue)
        self.driver.threadpool.add_task(self.__cleanKorrekturCache)
        return super().on_start()

    def __cleanKorrekturCache(self):
        cache_duration = 1800 # in seconds
        while self.driver.threadpool.alive:
            time.sleep(cache_duration)
            cleared_entries = 0
            for key in list(self.korrekturCache.keys()):
                if (datetime.datetime.now() - self.korrekturCache[key][2]).total_seconds() > cache_duration:
                    cleared_entries += 1
                    self.driver.posts.patch_post(key, options={"props":{}})
                    del self.korrekturCache[key]
            self.logger.info(f"Cleared {cleared_entries} by cache")

    def __handleKorrekturButton(self, queue):
        self.logger.info("Starting Korrekur_Button_Handler")
        while self.driver.threadpool.alive or not queue.empty():
            message = dict(queue.get())
            if not message:
                print("Wierd interaction empty post request")
            else:
                self.driver.posts.patch_post(message['post_id'], options={'props':{}})
                try:
                    if message['context']['action'] == "True":
                        try:
                            if message['post_id'] in list(self.korrekturCache.keys()):
                                original_Message, mostCorrectedReports, _ = self.korrekturCache[message['post_id']]
                                del self.korrekturCache[message['post_id']]
                                self.respond_with_five_worst_reports(original_Message, mostCorrectedReports)
                            else:
                                self.logger.error(f"No corresponding entry in cache for {message['post_id']}")
                                self.driver.create_post(message['channel_id'], "Hmm ich kann deine korregierten Berichte nicht finden :/\nStelle deine Abfrage nochmal. Dann finde ich sie. Versprochen! :D")
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
    
    def __addKorrekturButton(self, message: Message, lastPostId, messageContent, mostCorrectedReports, file_path=""):
        #instructions for buttons
        props = {
            "attachments": [
                {
                    "title" : "Zeige Top 5 korregierte Berichte",
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
        self.driver.posts.patch_post(lastPostId, options={'message': messageContent, 'file_ids':[file_path], 'props': props})
        # get id of posted post to remove the buttons after interaction or timeout
        lastPostId = self.driver.posts.get_posts_for_channel(message.channel_id)['order'][0]
        self.korrekturCache[lastPostId] = (message, mostCorrectedReports, datetime.datetime.now())

    def __getEmail(self, message: Message):
        return "Max.Mustermann@uk-essen.de"

    def __getIDs(self, message: Message, supressWarnings=False):
        ids = ["sL3HHpgQdchMsHPY","8YXgimgNJr88zhPK"]
        if len(ids) == 0:
            if not supressWarnings:
                given = "Max"
                family = "Mustermann"
                self.driver.reply_to(message, f"Keine Radiologen-IDs für den Namen {given} {family} im System")
            return None
        else:
            return ids

    def __getData(self, message: Message, dateString, date=None, suppressNoReportsMessage=False, supressIDWarnings=False):
        ids = self.__getIDs(message, supressWarnings=supressIDWarnings)
        if ids == None:
            given, family = ("Max", "Mustermann")
            self.logger.error(f"No radiology id found for {given} {family}")
            return None, None, None
        lengths_geschrieben = [random.randint(0,30) for _ in range(4)]
        lengths_freigegeben = [0 if lengths_geschrieben[i] == 0 else random.randint(max(lengths_geschrieben[i]-5, 0),lengths_geschrieben[i]) for i in range(4)]
        # Handle no reports written
        if lengths_geschrieben == [0, 0, 0, 0]:
            if not suppressNoReportsMessage:
                if date is not None:
                    self.driver.reply_to(
                        message,
                        f"Es wurden keine Berichte am {date.strftime('%A')}, dem {date.strftime('%d.%m%.%Y')} verfasst!"
                    )
                else:
                    self.driver.reply_to(
                        message,
                        "Es wurden keine Berichte geschrieben"
                    )
            return None, date, None
        # not_same = [random.randint(max(lengths_freigegeben[i]-5, 0), lengths_freigegeben[i]) for i in range(4)]
        not_same = lengths_freigegeben
        scores = [np.array([random.random()**8 for _ in range(lengths_freigegeben[i])]) if lengths_freigegeben[i] > 0 else np.array([]) for i in range(4)]
        mostCorrectedReports = pd.DataFrame({'data_preliminary': self.preliminaryVersion, 'data_final': self.finalVersion})
        # Handle no match of ammount of report histories and reports per code (some reports don't have histories)
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
                oa_korrektur = [0 if len(score) == 0 else score.mean() for score in scores]
            # Handle no report histories present
            else:
                oa_korrektur = [0 if length_freigegeben == 0 else 1 for length_freigegeben in lengths_freigegeben]
        # Handle all reports having a report history
        else:
            oa_korrektur = scores

        data = pd.DataFrame(zip(lengths_geschrieben, lengths_freigegeben,
                                oa_korrektur),
                            index=['CT', 'MRT', 'KONV', 'US'],
                            columns=['Geschrieben', 'OA Freigegeben', 'Durchschnitt OA Korrektur'])

        data.insert(2, 'OA Korrektur > 25%', [len(scores[i][np.where(scores[i] > 0.25)]) for i in range(4)])
        self.logger.info(f"Calculated data from {date} for {self.__getEmail(message)}")
        return data, date, mostCorrectedReports

    def __getStatusForDay(self, message: Message, number=None, suppressNoReportsMessage=False):
        if number is None:
            text = message.text.replace("-", "")
            if 'heute' in message.text.lower():
                number = 0
            elif 'gestern' in message.text.lower():
                number = 1
            else:
                number = int(text.split()[-1])
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

    @listen_to("^Korrektur$", re.IGNORECASE)
    def respond_with_five_worst_reports(self, message: Message, mostCorrectedReports=None):
        """
        Zeigt die drei meistkorregierten Berichte des Tages an
        """
        self.logger.info(f"Showing the five most corrected reports to {self.__getEmail(message)}")
        if mostCorrectedReports is None:
            _, date, mostCorrectedReports = self.__getStatusForDay(message, number=0)
            if date is None:
                return
        #TODO: Refactor this hot mess
        self.driver.reply_to(message, "### Top 5 korregierte Berichte")
        df = pd.DataFrame(mostCorrectedReports).convert_dtypes()
        for _, row in enumerate(df.itertuples()):
            diff = difflib.unified_diff(row.data_preliminary.splitlines(), row.data_final.splitlines(), n=0)
            tag = "\n".join([line for line in diff if len(line) > 5])
            sections = re.split("@@", tag)[1:]
            difference = 1 - SequenceMatcher(None, str(row.data_preliminary), str(row.data_final)).ratio()
            msg = f"#### Code: CTT | Änderung: {'{0:.2f}%'.format(difference * 100)} | Freigegeben: {datetime.datetime.now().strftime('%xT%X')}"
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
                msg += "\n" + f"##### Zeile {sections[index].split(' ')[1]} gelöscht, Zeile {sections[index].split(' ')[2]} hinzugefügt\n" + diff_df.to_markdown(index=False).replace("  ", "")
            self.driver.reply_to(message, msg)

    @listen_to("^ids$", re.IGNORECASE)
    def getIds(self, message: Message):
        """
        Radiologische PractitionerIDs des Anwenders (FHIR-Datenbank/Practitioner/{id})
        """
        self.logger.info(f"Processing command: {message.text} by {self.__getEmail(message)}")
        ids = self.__getIDs(message)
        if ids is not None: # [Fall 2]
            self.driver.reply_to(message,
                                "\n".join(str(x) for x in ids))

    @listen_to("^Name$", re.IGNORECASE)
    def getName(self, message: Message):
        """
        Name des Anwenders
        """
        self.logger.info(f"Processing command: {message.text} by {self.__getEmail(message)}")
        given, family = ("Max", "Mustermann")
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
            totalReports += random.randint(0, 698)
            date_first_report = datetime.datetime.now() - datetime.timedelta(random.randint(100, 600))
            if date_first_report is not None:
                firstDate = date_first_report if firstDate is None or firstDate > date_first_report else firstDate
        self.driver.posts.patch_post(lastPostId, options={'message': f"Anzahl Berichte in FHIR ab dem {firstDate.strftime('%d.%m.%Y')}:\n {totalReports}"})

    @listen_to("^Status letzter Monat$|^Status dieser Monat$|^Status Monat -[0-9]+$", re.IGNORECASE)
    def getStatisticForMonth(self, message: Message):
        """
        Status des Monats in Tabellenform

        Spalten:
            Geschrieben -- Anzahl geschriebene Berichte
            OA_Freigegeben -- Anzahl nicht selbst freigegebener Berichte
            OA_Korrektur -- Durchschnitt der Korrekturhärte (0% ist besser als 10%)
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
        month_df['Durchschnitt OA Korrektur'] = [val if np.isnan(val) else "{0:.2f}%".format(val * 100) for val in month_df['OA Korrektur']]
        month_df['Durchschnitt OA Korrektur'] = month_df['Durchschnitt OA Korrektur'].fillna('-')
        msg = f"#### Status für {date_lowerBound.strftime('%B')}:\n"+month_df.to_markdown()
        self.driver.posts.patch_post(lastPostId, options={'message': msg})

    @listen_to("^Status [-][0-9]+$|^Status 0$|^Heute$|^Gestern$", re.IGNORECASE)
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
        self.logger.info(f"Processing command: {message.text} by {self.__getEmail(message)}")
        self.driver.reply_to(message, "Berechne ...")
        lastPostId = self.driver.posts.get_posts_for_channel(message.channel_id)['order'][0]
        data, date, mostCorrectedReports = self.__getStatusForDay(message)
        if data is not None:
            data['Durchschnitt OA Korrektur'] = [val if np.isnan(val) else "{0:.2f}%".format(val * 100) for val in data['Durchschnitt OA Korrektur']]
            data['Durchschnitt OA Korrektur'] = data['Durchschnitt OA Korrektur'].fillna('-')
            msg = f"##### Status {date.strftime('%A')}, {date.strftime('%d.%m%.%Y')}\n" + data.to_markdown()
            self.__addKorrekturButton(message, lastPostId, msg, mostCorrectedReports)

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
                if self.__getIDs(message=message, supressWarnings=True) is None:
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
            self.driver.reply_to(message, f"Es wurden keine Berichte in den letzten drei Monaten geschrieben")

    @listen_to("^Rückblick Woche$", re.IGNORECASE)
    def review_week(self, message: Message):
        """
        Korrekturrate per Code der letzten sieben Tage als Graph
        """
        self.logger.info(f"Processing command: {message.text} by {self.__getEmail(message)}")
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
            self.driver.reply_to(message,
                                    f"##### Status der letzten sieben Tage",
                                    file_paths=[file])
            os.remove(file)
        else:
            self.driver.reply_to(message, f"Es wurden keine Berichte in den letzten sieben Tagen geschrieben")

    @listen_to("^test status$", re.IGNORECASE)
    def testStatus(self, message: Message):
        bad_days = []
        self.driver.reply_to(message, "Start testing")
        for i in range(360):
            try:
                self.__getStatusForDay(message, number=i, suppressNoReportsMessage=True)
            except:
                bad_days.append(i)
        if len(bad_days) > 0:
            self.driver.reply_to(message, str(bad_days))
        else:
            self.driver.reply_to(message, "Alles funktioniert :white_check_mark:")

    @listen_to("^Status Graph [-][0-9]+$|^Status Graph 0$|^Heute graph$|^Gestern graph$", re.IGNORECASE)
    def getStatusGraph(self, message: Message):
        """
        Status des Tages grafisch dargestellt

            Status -10 --> Status vor 10 Tagen
            Status 0 --> Status heute
        
        Säulen:
            Geschrieben -- Anzahl geschriebene Berichte
            OA_Freigegeben -- Anzahl nicht selbst freigegebener Berichte
            OA_Korrektur -- Durchschnitt der Korrekturhärte (0% ist besser als 10%)
        """
        self.logger.info(f"Processing command: {message.text} by {self.__getEmail(message)}")
        self.driver.reply_to(message, "Berechne ...")
        lastPostId = self.driver.posts.get_posts_for_channel(message.channel_id)['order'][0]
        data, date, mostCorrectedReports = self.__getStatusForDay(message)
        if data is not None:
            file = Path(f"/tmp/{datetime.datetime.now().timestamp()}_status_graph.png")
            data.plot(kind='bar')
            plt.gca().xaxis.set_tick_params(rotation=0)
            plt.savefig(file, bbox_inches='tight')
            self.__addKorrekturButton(message, lastPostId, f"##### Status {date.strftime('%A')}, {date.strftime('%d.%m%.%Y')}", mostCorrectedReports, file_path=str(file))
            os.remove(file)