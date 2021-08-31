import os
import json
import base64
import numpy as np
import pandas as pd
from tqdm import tqdm
import logging

import inspect

class DiagnosticReportJsonParser():
    def __init__(self, disableTqdm=False) -> None:
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.disableTqdm = disableTqdm

    def __get_report_id(self, data=None) -> list:
        ids = []
        if data is None:
            for object in tqdm(self.data['entry'], desc="Parse report ids", disable=self.disableTqdm):
                try:
                    ids.append(object['resource']['id'])
                except:
                    ids.append(np.nan)
        else:
            for report in tqdm(data, desc="Parse report ids", disable=self.disableTqdm):
                try:
                    ids.append(report['resource']['id'])
                except:
                    ids.append(np.nan)

        return ids

    def __get_status(self, data=None) -> list:
        status = []
        if data is None:
            for object in tqdm(self.data['entry'], desc="Parse status", disable=self.disableTqdm):
                try:
                    status.append(object['resource']['status'])
                except:
                    status.append(np.nan)
        else:
            for report in tqdm(data, desc="Parse status", disable=self.disableTqdm):
                try:
                    status.append(report['resource']['status'])
                except:
                    status.append(np.nan)
        return status

    def __get_reports(self, data=None) ->list:
        reports = []
        if data is None:
            for object in tqdm(self.data['entry'], desc="Parse reports", disable=self.disableTqdm):
                try:
                    reports.append(base64.b64decode(object['resource']['presentedForm'][0]['data']).decode('utf-8'))
                except:
                    reports.append(np.nan)
        else:
            for report in tqdm(data, desc="Parse reports", disable=self.disableTqdm):
                try:
                    reports.append(base64.b64decode(report['resource']['presentedForm'][0]['data']).decode('utf-8'))
                except:
                    reports.append(np.nan)
            
        return reports
    
    def __get_befund(self, data=None) -> list:
        befund = []
        if data is None:
            for object in tqdm(self.data['entry'], desc="Parse Befunde", disable=self.disableTqdm):
                try:
                    text = base64.b64decode(object['resource']['presentedForm'][0]['data']).decode('utf-8').split('\r\nBefund:')
                    if len(text) > 1:
                        befund.append(text[-1].split('\r\nBeurteilung:')[0])
                    else:
                        befund.append(np.nan)
                except:
                    befund.append(np.nan)
        else:
            for report in tqdm(data, desc="Parse Befunde", disable=self.disableTqdm):
                try:
                    text = base64.b64decode(report['resource']['presentedForm'][0]['data']).decode('utf-8').split('\r\nBefund:')
                    if len(text) > 1:
                       befund.append(text[-1].split('\r\nBeurteilung:')[0])
                    else:
                        befund.append(np.nan)
                except:
                    befund.append(np.nan)

        return befund

    def __get_beurteilung(self, data=None) -> list:
        beurteilung = []
        if data is None:
            for object in tqdm(self.data['entry'], desc="Parse Beurteilungen", disable=self.disableTqdm):
                try:
                    text = base64.b64decode(object['resource']['presentedForm'][0]['data']).decode('utf-8').split('\r\nBeurteilung:')
                    if len(text) > 1:
                        beurteilung.append(text[-1])
                    else:
                        beurteilung.append(np.nan)
                except:
                    beurteilung.append(np.nan)
        else:
            for report in tqdm(data, desc="Parse Beurteilungen", disable=self.disableTqdm):
                try:
                    text = base64.b64decode(report['resource']['presentedForm'][0]['data']).decode('utf-8').split('\r\nBeurteilung:')
                    if len(text) > 1:
                        beurteilung.append(text[-1])
                    else:
                        beurteilung.append(np.nan)
                except:
                    beurteilung.append(np.nan)

        return beurteilung

    def __get_practitionerId(self, data=None) -> list:
        ids = []
        if data is None:
            for object in tqdm(self.data['entry'], desc="Parse practitioner ids", disable=self.disableTqdm):
                try:
                    ids.append(object['resource']['performer'][0]['reference'])
                except:
                    ids.append(np.nan)
        else:
            for report in tqdm(data, desc="Parse practitioner ids", disable=self.disableTqdm):
                try:
                    ids.append(report['resource']['performer'][0]['reference'])
                except:
                    ids.append(np.nan)

        return ids

    def __get_signerId(self, data=None):
        signerIds = []
        if data is None:
            for object in tqdm(self.data['entry'], desc="Parse signer ids", disable=self.disableTqdm):
                try:
                    signerIds.append(object['resource']['resultsInterpreter'][0]['reference'])
                except:
                    signerIds.append(np.nan)
        else:
            for report in tqdm(data, desc="Parse signer ids", disable=self.disableTqdm):
                try:
                    signerIds.append(report['resource']['resultsInterpreter'][0]['reference'])
                except:
                    signerIds.append(np.nan)

        return signerIds

    def __get_verifyerId(self, data=None) -> list:
        verifyerIds = []
        if data is None:
            for object in tqdm(self.data['entry'], desc="Parse verifyer ids", disable=self.disableTqdm):
                try:
                    verifyerIds.append(object['resource']['resultsInterpreter'][1]['reference'])
                except:
                    verifyerIds.append(np.nan)
        else:
            for report in tqdm(data, desc="Parse verifyer ids", disable=self.disableTqdm):
                try:
                    verifyerIds.append(report['resource']['resultsInterpreter'][1]['reference'])
                except:
                    verifyerIds.append(np.nan)

        return verifyerIds

    def __get_image_references(self, data=None) -> list:
        imageReferences = []
        if data is None:
            for object in tqdm(self.data['entry'], desc="Parse image references", disable=self.disableTqdm):
                try:
                    imageReferences.append(object['resource']['imagingStudy'][0]['reference'])
                except:
                    imageReferences.append(np.nan)
        else:
            for report in tqdm(data, desc="Parse image references", disable=self.disableTqdm):
                try:
                    imageReferences.append(report['resource']['imagingStudy'][0]['reference'])
                except:
                    imageReferences.append(np.nan)

        return imageReferences

    def __get_code(self, data=None) -> list:
        code = []
        if data is None:
            for object in tqdm(self.data['entry'], desc="Parse medical codes", disable=self.disableTqdm):
                try:
                    code.append(object['resource']['code']['coding'][0]['code'])
                except:
                    code.append(np.nan)
        else:
            for report in tqdm(data, desc="Parse medical codes", disable=self.disableTqdm):
                try:
                    code.append(report['resource']['code']['coding'][0]['code'])
                except:
                    code.append(np.nan)
        return code

    def __get_timeCode(self, data=None) -> list:
        timecode = []
        if data is None:
            for object in tqdm(self.data['entry'], desc="Parse time codes", disable=self.disableTqdm):
                try:
                    timecode.append(object['resource']['issued'])
                except:
                    timecode.append(np.nan)
        else:
            for report in tqdm(data, desc="Parse time codes", disable=self.disableTqdm):
                try:
                    timecode.append(report['resource']['issued'])
                except:
                    timecode.append(np.nan)
        return timecode

    def get_all_reports_fullData(self, data=None) -> pd.DataFrame:
        df = pd.DataFrame(
        {   
            'id': self.__get_report_id(data),
            'status': self.__get_status(data),
            'time_code': self.__get_timeCode(data),
            'practitioner_id': self.__get_practitionerId(data),
            'code': self.__get_code(data),
            'data': self.__get_reports(data),
            'images': self.__get_image_references(data),
            'verifyerIds': self.__get_verifyerId(data)
        })
        return df

    def parse_history_reports(self, preliminaryState: list, finalState: list) -> pd.DataFrame:
        self.logger.info("    Parsing history")
        df = pd.DataFrame(
            {
                'id': self.__get_report_id(finalState),
                'code': self.__get_code(finalState),
                'status': self.__get_status(finalState),
                'time_code_final': self.__get_timeCode(finalState),
                'time_code_preliminary': self.__get_timeCode(preliminaryState),
                'practitioner_id_preliminary': self.__get_practitionerId(preliminaryState),
                'data_final': self.__get_reports(finalState),
                'data_preliminary': self.__get_reports(preliminaryState),
                'befund_final': self.__get_befund(finalState),
                'befund_preliminary': self.__get_befund(preliminaryState),
                'beurteilung_final': self.__get_beurteilung(finalState),
                'beurteilung_preliminary': self.__get_beurteilung(preliminaryState),
                'images_final': self.__get_image_references(finalState),
                'images_preliminary': self.__get_image_references(preliminaryState),
                'verifyerIds_final': self.__get_verifyerId(finalState)
            }
        )
        return df

    def get_all_report_ids(self, data=None) -> pd.DataFrame:
        df = pd.DataFrame(
            {
                'id': self.__get_report_id(data)
            }
        )
        df = df.dropna().drop_duplicates(subset="id", keep="first")
        return df