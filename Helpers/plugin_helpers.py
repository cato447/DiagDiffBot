from pandas.core.frame import DataFrame
from .parse_report import DiagnosticReportJsonParser
from .parse_history import DiagnosticReportHistoryJsonParser
import textdistance
import logging

class Helpers():
    def __init__(self, fhir_client) -> None:
        self.client = fhir_client
        self.reportParser = DiagnosticReportJsonParser(disableTqdm=True)
        self.logger = logging.getLogger(__name__)

    def __sumLists(self, lists):
        summed_list = [0,0,0,0]
        for i in range(len(lists[0])):
            code_length = 0
            for list in lists:
                code_length += list[i]
            summed_list[i] = code_length
        return summed_list

    def extract_num_from_command(self, message):
        text = message.text.replace("-", "").lower()
        # print(text)
        if 'heute' in text:
            number = 0
        elif 'gestern' in text:
            number = 1
        elif ' ' in text:
            number = int(text.split()[-1])
        else:
            number = 0
        return number

    def __getReportHistories(self, practitionerIDs, dateString):
        report_histories = []
        for practitionerID in practitionerIDs:
            self.logger.info(f"  Fetch report histories from {dateString} by practitioner {practitionerID}")
            reports = self.__getReports(practitionerID, dateString)
            report_ids = self.reportParser.get_all_report_ids(reports)['id'].tolist()
            report_histories.extend(self.client.get_diagnosticReport_history(report_ids))
        return report_histories

    def __getReports(self, practitionerID, dateString):
        self.logger.info(f"  Fetch reports from {dateString} by practitioner {practitionerID}")
        formdata = {
            'performer' : practitionerID,
            '_sort' : "date"
        }
        return self.client.get_pagination("DiagnosticReport", formdata, f"&date={dateString}")

    def __getMostCorrectedFiveReports(self, historyDf: DataFrame):
        return historyDf.sort_values("similarity_score", ascending=True).convert_dtypes()[:5][['data_preliminary', 'data_final', 'similarity_score', 'code', 'time_code_final']]
    
    def getFullName(self, email):
        data = self.client.get_practitioner_data_by_email(email)
        family = data['entry'][0]['resource']['name'][0]['family']
        given = data['entry'][0]['resource']['name'][0]['given'][0]
        return given, family
    
    def get_all_practitioner_ids_for_name(self, given, family):
        ids = []
        formdata = {
            'given' : given,
            'family' : family,
            '_sort' : ""
        }
        data = self.client.get_pagination("Practitioner", formdata,custom_string='&identifier={identifier_url}}')
        for entry in data:
            ids.append(entry['resource']['id'])
        if len(ids) == 0: #* Im ersten Anlauf können durch das falsch schreiben des Vornames IDs unter den Tisch fallen [!Fall 2]
            ids = []
            formdata = {
                'family' : family,
                '_sort' : ""
            }
            data = self.client.get_pagination("Practitioner", formdata,custom_string='&identifier={identifier_url}')
            for entry in data:
                if textdistance.levenshtein(entry['resource']['name'][0]['given'][0], given) < 3:
                    ids.append(entry['resource']['id'])
        return ids

    def get_count_reports_per_code(self, practitionerIDs, dateString):
        lengths_geschrieben = []
        lengths_oa_freigegeben = []
        for practitionerID in practitionerIDs:
            reports = self.__getReports(practitionerID, dateString)
            if len(reports) > 0:
                reports_df = self.reportParser.get_all_reports_fullData(reports).convert_dtypes()
                codes = ['CT', 'MR', 'K', 'U']
                reports_per_code = [reports_df[reports_df['code'].str.startswith(code)] for code in codes]
                lengths_geschrieben.append([len(code) for index, code in enumerate(reports_per_code)])
                status_per_code = [code[(code['status'] == 'final') & (code['practitioner_id'] != code['verifyerIds'])] for code in reports_per_code]
                lengths_oa_freigegeben.append([len(code) for code in status_per_code])
        if len(lengths_geschrieben) > 0:
            lengths_geschrieben = self.__sumLists(lengths_geschrieben)
        else: 
            lengths_geschrieben = [0,0,0,0]
        if len(lengths_oa_freigegeben) > 0:
            lengths_freigegeben = self.__sumLists(lengths_oa_freigegeben)
        else:
            lengths_freigegeben = [0,0,0,0]
        return lengths_geschrieben, lengths_freigegeben

    def get_score_per_code(self, practitionerIDs, dateString):
        report_histories = self.__getReportHistories(practitionerIDs, dateString)
        reportHistoryParser = DiagnosticReportHistoryJsonParser(histories=report_histories, disableTqdm=True)
        if len(reportHistoryParser.histories) > 0: #* catch no history present [!Fall 1]
            report_histories_df = reportHistoryParser.getData()
            report_histories_df['code'] = report_histories_df['code'].astype(str)
            names = ['CT', 'MR', 'K', 'U']
            reports_per_code = [report_histories_df[report_histories_df['code'].str.startswith(name)] for name in names]
            not_same = [code[code['is_same'] == False] for code in reports_per_code]
            scores_per_code = [code[code['practitioner_id_preliminary'] != code['verifyerIds_final']]['similarity_score'] for code in reports_per_code]
            return [len(code) for code in not_same], scores_per_code, self.__getMostCorrectedFiveReports(report_histories_df[report_histories_df['code'].str.startswith(tuple(names))])
        else:
            return [0,0,0,0], None, None