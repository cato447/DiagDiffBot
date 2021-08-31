import requests
import urllib
import sys
sys.path.append("..")
from tqdm import tqdm
from .fhir_client import ukeFHIR

class CustomFhirClient(ukeFHIR):
    def get_diagnosticReport_history(self, ids):
        reports = []
        for id in tqdm(ids, desc="Fetch all versions of reports"):
            url = f'{self.appUrl}DiagnosticReport/{id}/_history'
            r = self.sess.get(url=url, cookies = self.cookies)
            if r.status_code == requests.codes.ok:
                r = r.json()
                reports.append(r['entry'])
            else:
                print(url)
                raise Exception
        
        return reports

    def totalMatches(self, target, formdata, custom_string = '', count=None):
        url = self.q('{}?{}', target, urllib.parse.urlencode(formdata) + custom_string)
        # print(url)
        r = self.sess.get(url=url,  cookies = self.cookies)
        if r.status_code == requests.codes.ok:
            r = r.json()
            return r.get('total')
        else:
            print(url)
            raise Exception

    def get_practitioner_data_by_email(self, email):
        url = f'{self.appUrl}Practitioner?_content="{email}"'
        print(url)
        r = self.sess.get(url=url, cookies=self.cookies)
        if r.status_code == requests.codes.ok:
            r = r.json()
            return r
        else:
            print(url)
            raise Exception("Bad URL")

    def get_practitioner_data(self, id):
        if 'Practitioner/' not in id:
            id = 'Practitioner/' + id
        url = self.appUrl + id
        r = self.sess.get(url=url,  cookies = self.cookies)
        if r.status_code == requests.codes.ok:
            r = r.json()
            return r
        else:
            print(url)
            raise Exception

    def get_date_of_first_report(self, formdata):
        url = self.q('{}?{}', "DiagnosticReport", urllib.parse.urlencode(formdata))
        r = self.sess.get(url=url,  cookies = self.cookies)
        if r.status_code == requests.codes.ok:
            r = r.json()
            try:
                return r['entry'][0]['resource']['effectiveDateTime']
            except:
                return None
        else:
            print(url)
            raise Exception()