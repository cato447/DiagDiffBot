import os
import json
import pandas as pd
from difflib import SequenceMatcher
from tqdm import tqdm
from .parse_report import DiagnosticReportJsonParser
import logging

import inspect

class DiagnosticReportHistoryJsonParser():
    def __init__(self, histories=None, disableTqdm = False) -> None:
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        self.disableTqdm = disableTqdm

        self.script_dir = os.path.dirname(__file__) #<-- absolute dir the script is in
        self.__load_data(histories)
        self.__populate_data_frame()
        self.preliminaryState, self.finalState = self.__getStates()

    def __load_data(self, histories):
        if histories is None:
            jsonFilePath = os.path.join(self.script_dir, "../../data/json/raw/report_history_data.json")
            self.logger.info(f"    Loading file {jsonFilePath}")
            with open(jsonFilePath, 'r') as json_file:
                data = json.load(json_file)
            self.logger.info("    Loading finished")
            self.logger.info("    Building dataframe")
            database_response_df = pd.DataFrame.from_dict(data, orient="index")
            self.logger.info("    Finished building a dataframe")
            self.logger.info("    Extracting histories")
            self.histories = database_response_df.T.apply(lambda x: x.dropna().tolist()).tolist()
            self.logger.info("    Finished extracting histories")
        else:
            self.logger.info("    Loading given histories")
            self.histories = histories

    def __populate_data_frame(self):
        data = pd.DataFrame()

        reportIds = []
        performerList = []
        resultInterpreterList = []
        preliminaryStates = []
        finalStates = []

        for history in tqdm(self.histories, desc="populate dataframe", disable=self.disableTqdm):
            hasPerformer = False
            hasResultsInterpreter = False
            final = False
            preliminary = False
            preliminaryState = None
            finalState = None
            
            for state in history:
                if state['resource']['status'] == "preliminary" and preliminary == False:
                    preliminaryState = state
                    preliminary = True
                    if 'performer' in state['resource'].keys():
                        hasPerformer = True
                elif state['resource']['status'] == "final" and final == False:
                    finalState = state
                    final = True
                    if 'resultsInterpreter' in state['resource'].keys():
                        hasResultsInterpreter = True
            resultInterpreterList.append(hasResultsInterpreter)
            performerList.append(hasPerformer)
            finalStates.append(finalState)
            preliminaryStates.append(preliminaryState)
            reportIds.append(history[0]['resource']['id'])

        data['reportId'] = reportIds
        data['finalState'] = finalStates
        data['preliminaryState'] = preliminaryStates
        data['has_performer'] = performerList
        data['has_interpreter'] = resultInterpreterList
        self.data = data.set_index('reportId')
    
    def __getStates(self):
        clean_data = self.data[(self.data['has_performer'] == True) & (self.data['has_interpreter'] == True)]
        return clean_data['preliminaryState'], clean_data['finalState']

    def getData(self) -> pd.DataFrame:
        self.data_df = DiagnosticReportJsonParser( disableTqdm=self.disableTqdm).parse_history_reports(self.preliminaryState, self.finalState).convert_dtypes()
        self.__add_is_same()
        self.__add_similarity_score()
        self.__add_similarity_score_beurteilungen()
        self.__add_similiarity_score_befunde()
        self.data_df = self.data_df[self.data_df['practitioner_id_preliminary'] != "Practitioner/49be0c575457c2902be370c0baea5a81bee366b600150ab87cc75461c9ebdd9d"]
        return self.data_df

    def __add_is_same(self):
        self.data_df['is_same'] = [str(preliminary) == str(final) for preliminary, final in tqdm(zip(self.data_df['data_preliminary'], self.data_df['data_final']), total=len(self.data_df['data_preliminary']), desc="annotate if data is same", disable=self.disableTqdm)]

    def __add_similarity_score(self):
        self.data_df['similarity_score'] = [None if str(preliminary) == '<NA>' or str(final) == '<NA>' else SequenceMatcher(None, str(preliminary), str(final)).ratio() for preliminary, final in tqdm(zip(self.data_df['data_preliminary'], self.data_df['data_final']),total=len(self.data_df['data_preliminary']), desc="add similarity score", disable=self.disableTqdm)]

    def __add_similiarity_score_befunde(self):
        self.data_df['similarity_befund_score'] = [None if str(preliminary) == '<NA>' or str(final) == '<NA>' else SequenceMatcher(None, str(preliminary), str(final)).ratio() for preliminary, final in tqdm(zip(self.data_df['befund_preliminary'], self.data_df['befund_final']), total=len(self.data_df['befund_preliminary']),desc="add similarity befund score", disable=self.disableTqdm)]
    
    def __add_similarity_score_beurteilungen(self):
        self.data_df['similarity_beurteilung_score'] = [None if str(preliminary) == '<NA>' or str(final) == '<NA>' else SequenceMatcher(None, str(preliminary), str(final)).ratio() for preliminary, final in tqdm(zip(self.data_df['beurteilung_preliminary'], self.data_df['beurteilung_final']), total=len(self.data_df['beurteilung_preliminary']),desc="add similarity beurteilung score", disable=self.disableTqdm)]