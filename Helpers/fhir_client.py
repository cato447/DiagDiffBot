#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  1 11:14:02 2020

@author: moon
"""

import requests, urllib
from requests.auth import HTTPBasicAuth
import dateparser
import time
import math
from datetime import datetime
from tqdm import tqdm
import logging

class TqdmLoggingHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record) 

class ukeFHIR:
    def __init__(self,baseUrl="{base_url}", appUrl='{app_url}', isVerbose=True):
        self.baseUrl = baseUrl
        self.appUrl = appUrl
        self.dcmUrl = '{dicom_url}'
        self.loginUrl = '{auth_url}'
        localtime = time.localtime(time.time())
        self.localtime = localtime
        self.resources = []
        self.filetypes = {'application/pdf' : 'pdf',
                          'image/tiff' : 'tif',
                          'text/plain; charset=UTF-8' : 'txt',
                          'application/msword' : 'doc',
                          'application/octet-stream' : 'txt',
                          'application/zip' : 'zip'}
        self.logger = logging.getLogger(__name__)
        FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] - %(levelname)s - %(message)s"
        loglevel = logging.WARNING if not isVerbose else logging.DEBUG
        if not self.logger.handlers:
            tqdmLoggingHandler = TqdmLoggingHandler()
            tqdmLoggingHandler.setLevel(loglevel)
            formatter = logging.Formatter(FORMAT)
            tqdmLoggingHandler.setFormatter(formatter)
            self.logger.addHandler(tqdmLoggingHandler)
        self.logger.setLevel(loglevel)
        self.logger.propagate = False


    def q(self, pattern, *arg):
        return('{}{}'.format(self.appUrl,pattern.format(*arg)))
    
    def d(self, pattern, *arg):
        return('{}{}'.format(self.dcmUrl,pattern.format(*arg)))


    def setToken(self, token):
        self.token = token
        self.cookies = {"shipToken" : self.token}

    def login(self, username, password):
        self.sess = requests.Session()
        loginUrl = self.loginUrl
        r = self.sess.get(loginUrl, auth=HTTPBasicAuth(username, password))
        self.r = r
        if r.status_code == requests.codes.ok:
            loglevel = self.logger.getEffectiveLevel()
            self.logger.info(f'Login with username: {username} succesful')
            self.cookies = r.cookies
            self.sess.cookies = self.cookies
            # get token and set as authorization header
            token = r.text
            #print(token)
            self.token = token
            self.sess.headers.update({'Authorization': f'bearer {token}'})
            return True
        else:
            raise ValueError(f"Loging failed for username {username}!\n{r.__dict__}")

    def query(self, target, formdata, custom_string = ''):
        url = self.q('{}?{}', target, urllib.parse.urlencode(formdata) + custom_string)
        self.logger.debug(f"Making query: {url}")
        r = self.sess.get(url=url,  cookies = self.cookies)
        entry = []
        if r.status_code == requests.codes.ok:
            r = r.json()
            total = r.get('total')
            self.logger.debug(f'found {total} matche(s)')
            try:
                entry = r['entry']
                return entry
            except:
                self.logger.warning("Response JSON has no entry key")
                return entry
        else:
            raise ValueError(f"Query {url} failed!\n{r.text}")

        
        
    def get_patient(self, subject_id):
        url = self.q(f'Patient/{subject_id}')
        self.logger.debug(f"Fetching patient {subject_id}")
        r = self.sess.get(url=url,  cookies = self.cookies)
        if r.status_code == requests.codes.ok:
            r = r.json()
            return r
        else:
            raise ValueError(f"Query {url} failed!\n{r.text}")
        
        
    def dicomsend(self, StudyInstanceUID, target, source ='GEPACS'):
        if StudyInstanceUID == StudyInstanceUID:
            url = self.d(f'{source}/studies/{StudyInstanceUID}/sendto/{target}')
            self.logger.debug(f"Fetching dicomsend: {url}")
            r = self.sess.get(url=url,  cookies = self.cookies)
            if r.status_code == requests.codes.ok:
                return r.json()
            else:
                raise ValueError(f"Query {url} failed!\n{r.text}")
        else:
            raise TypeError("StudyInstanceUID should not be NAN")
        
    def dicom2seafile(self, StudyInstanceUID, target = '6645dd5c-f183-444f-82a3-5507c8700580', source ='GEPACS'):
        if StudyInstanceUID == StudyInstanceUID:
            url = self.d(f'{source}/studies/{StudyInstanceUID}/exportto/seafile/{target}')
            self.logger.debug(f"Converting dicom to seafile {url}")
            r = self.sess.get(url=url,  cookies = self.cookies)
            if r.ok == True:
                r = r.json()
                newStudyInstanceUID = r['newStudyInstanceUID']
                return newStudyInstanceUID
            else:
                raise ValueError(f"Query failed: {r.text}")
        else:
            raise ValueError("StudyInstanceUID can't be NAN")


    def get(self, target, formdata, custom_string = '', ignoreErrors=False, url=None, disableTqdm=False):
        if '_sort' not in formdata.keys():
            raise ValueError("_sort parameter missing in formdata")
        if '_sort' in custom_string:
            raise ValueError("set the _sort value in the formdata and not the custom string")
        # Allow direct passing of an already valid url (Needed for get_more)
        if url is None:
            url = self.q('{}?{}', target, urllib.parse.urlencode(formdata) + custom_string)
        self.logger.debug(f"Making query to {url}")
        r = self.sess.get(url=url,  cookies = self.cookies)
        if r.status_code == requests.codes.ok:
            r = r.json()
            total = r.get('total')
            self.logger.debug(f' found {total} matche(s)')
            entry_list = []
            Offset = 0
            count = 400
            pbar = tqdm(total=total, disable=disableTqdm)
            while  Offset < total:
                nexturl = url + f'&_count={count}&_shipOffset={Offset}' # ukefhir does not like higher count
                bundle = self.sess.get(nexturl).json()
                try:
                    entry = bundle['entry']
                    entry_list.extend(entry)
                    Offset += count
                    pbar.update(count)
                except:
                    if ignoreErrors:
                        Offset += count
                        pbar.update(count)
                    else:
                        pbar.close()
                        raise ValueError(f"Query to {url} failed\n{r.text}\nREST-Response might not contain 'entry' key")
            pbar.close()
            return entry_list
        else:
            raise ValueError(f"Query to {url} failed:\n{r.text}")

    def get_pagination(self, target, formdata, custom_string= '', ignoreErrors=False, url=None, disableTqdm=False):
        # Allow direct passing of an already valid url (Needed for get_more)
        if '_sort' not in formdata.keys():
            raise ValueError("_sort parameter missing in formdata")
        if '_sort' in custom_string:
            raise ValueError("set the _sort value in the formdata and not the custom string")
        count = 400
        if url is None:
            url = self.q('{}?{}', target, urllib.parse.urlencode(formdata) +
                        custom_string)+f"&_count={count}"
        self.logger.debug(f"Making query to {url}")
        firstResource = self.sess.get(url=url,  cookies = self.cookies)
        if firstResource.status_code == requests.codes.ok:
            firstResource_json = firstResource.json()
            total = firstResource_json.get('total')
            self.logger.debug(f'found {total} matche(s)')
            entry_list = []
            Offset = 0
            pbar = tqdm(total=total, disable=disableTqdm)
            resource_json = firstResource_json
            while  Offset < total:
                for elem in resource_json['link']:
                    if elem['relation'] == "next" or elem['relation'] == "end":
                        nexturl = self.baseUrl + elem['url']
                if Offset != 0: # prevent overwrite of first resource
                    resource_json = self.sess.get(nexturl).json()
                try:
                    entry = resource_json['entry']
                    entry_list.extend(entry)
                    Offset += count
                    pbar.update(count)
                except:
                    if ignoreErrors:
                        Offset += count
                        pbar.update(count)
                    else:
                        pbar.close()
                        raise ValueError(f"Query to {nexturl} failed!\n{resource_json}\nREST-Response might not contain 'entry' key")

            pbar.close()
            return entry_list
        else:
            raise ValueError(firstResource.text)

    def get_more(self, target, formdata : dict, custom_string = '', ignore_slow=False, disableTqdm=False):
        self.logger.warning("get_more is deprecated. Use get_pagination instead!")
        """
        GET_MORE IS DEPRECATED. US GET_PAGINATION INSTEAD!

        ONLY SUPPORTS QUERYING FROM ONE POINT IN TIME TO TODAY

        Download more than 10.000 Elements per Query --> Circumvent ElasticSearchLimit

        Include _sort in formdata

        _sort should have the same value as the key used to filter for dates:
        
        Example:
            formdata = {issued = ge2021-01-01, ... = ..., _sort = "issued"}\n
            fhir_client.get_more("DiagnosticReport", formdata)
        """
        
        if '_sort' not in formdata.keys():
            raise Exception("_sort parameter missing in formdata")
        elif formdata['_sort'].startswith('-'):
            value = formdata['_sort']
            formdata['_sort'] = value[1:]
            self.logger.warning(f"changed _sort={value} to _sort={formdata['_sort']}\nSorting from new to old is currently not supported in get_more")
        if '_sort' in custom_string:
            raise ValueError("set the _sort value in the formdata and not the custom string")
        url = self.q('{}?{}', target, urllib.parse.urlencode(formdata) + custom_string)
        r = self.sess.get(url=url,  cookies = self.cookies)
        if r.status_code == requests.codes.ok:
            r = r.json()
            total = r.get('total')
            self.logger.debug(f"{total} total matches found")
            if total < 9999 and not ignore_slow:
                raise UserWarning("""Use fhir_client.get() instead please!
                                     fhir_client.get_more() should only be used
                                     if you have to pull more than 10.000 datapoints
                                     out of the database. (Limitation with Elastic Search)""")
            else:
                self.logger.debug("Start slicing to circumevent Elastic Search request limit")
                slices = int(math.ceil(total / 9999))
                dates = []
                if formdata['_sort'] == 'date':
                    dates.append(r['entry'][0]['resource']['effectiveDateTime']) #Append starting date
                else:
                    dates.append(r['entry'][0]['resource'][f"{formdata['_sort']}"]) #Append starting date
                #find date boundaries close to 9999 entries
                for i in tqdm(range(slices), disable=disableTqdm):
                    if slices == 1:
                        return self.get(target, formdata, custom_string, disableTqdm=disableTqdm)
                    if len(dates) == 1:
                        modified_url = url+f"&_shipOffset={9999}&_count=1&_sort={formdata['_sort']}"
                    elif len(dates) > 1:
                        modified_url = url+f"&{formdata['_sort']}=ge{dates[-1]}&_shipOffset={9999}&_count=1&_sort={formdata['_sort']}"
                    r = self.sess.get(url=modified_url,  cookies = self.cookies)
                    r = r.json()
                    if formdata['_sort'] == "date":
                        date = r['entry'][0]['resource']['effectiveDateTime']
                    elif i < slices-1:
                        date = r['entry'][0]['resource'][formdata['_sort']]
                    else:
                        date = datetime.now().strftime("%Y-%m-%d")

                    dates.append(date)
        else:
            raise Exception(f"{r.content}\n{url} returned error code {r.status_code}")
        entry_list = []
        #make requests with date boundaries
        self.logger.debug("Start downloading individual slices")
        self.logger.debug(f"Number of Slices: {slices}")
        for i in range(len(dates) - 1):
            entry_list.extend(self.get(target, formdata, f"{custom_string}&{formdata['_sort']}=ge{dates[i]}&{formdata['_sort']}=lt{dates[i+1]}", disableTqdm=disableTqdm))
        return entry_list

    def get_subject_id(self, family, name, birthdate = 'gt1900-01-01', disableTqdm=False):
    
        ## first check if there is a linked patient
        formdata = {'family:contains': family, 'name:contains': name, 'birthdate': birthdate, 'identifier':'https://{ris_url}/SHIP/LinkedPatient|'} #, 'identifier':'https://{ris_url}/HIS/Cerner/Medico|'}
        bundle = self.get('Patient', formdata, disableTqdm=disableTqdm)
        
        if len(bundle) == 1:   
            self.logger.debug(f'found {len(bundle)} linked patient')
            r = bundle[0]['resource']
            link = r['link']
            
            subject_id = ''
            # get subject_id of all linked datasets
            for patient in link:
                subject_id = subject_id + ',' + patient['other']['reference'].replace('Patient/','')
            
            subject_id = subject_id[1:]
            
            ## test if subject_id works
            #formdata = {'subject': subject_id} #, 'identifier':'https://{ris_url}/HIS/Cerner/Medico|'}
            #query = fhir.get('Observation', formdata)
            
        
        elif len(bundle) == 0:
            self.logger.warning('+ found no linked patient, checking all databases')
            formdata = {'family:contains': family, 'name:contains': name, 'birthdate': birthdate} #, 'identifier':'https://{ris_url}/HIS/Cerner/Medico|'}
            bundle = self.get('Patient', formdata, disableTqdm=disableTqdm)
            if len(bundle) != 0: 
                subject_id = ''
                for item in bundle:
                    r = item['resource']
                    subject_id = subject_id + ',' + r['id']
                subject_id = subject_id[1:]
                        
            else:
                self.logger.warning(f'{family}, {name} - {birthdate} not found in the database')
                subject_id = ''
        elif len(bundle) >= 2:
            self.logger.warning('found multiple linked patient')
            subject_id = ''
        return subject_id
    
    def update_subject_id(self, slist, disableTqdm=False):
        for i in range(len(slist)):
            
            try:
                p = slist[i]
                name = p['name'].replace(' ', ',')
                family = p['family'].replace(' ', ',')
                birthdate = dateparser.parse(str(p['birthdate'])).date()
                
                if name == name and family == family:
                    subject = self.get_subject_id(family, name, birthdate, disableTqdm=disableTqdm)
                    sdict = dict(subject = subject)
                    slist[i].update(sdict)
                
            except:
                name = ''
                family = ''
                birthdate = ''                
                                
                self.logger.error(f'something went wrong in line {i}')

        return slist

    def update_link2pd(self, list_of_dict_with_subject_id):
        
        url = 'https://{pre}.{ris_url}/app/PatientDashboard/patient/'
        
        slist = list_of_dict_with_subject_id
        for i in range(len(slist)):
            
            try:
                p = slist[i]
                subject_id = p['subject']
                
                if subject_id == subject_id:
                    PD_link = url + subject_id + '?codes=2&doc-search=&lab-search='
                    ldict = dict(PD_link = PD_link)
                    slist[i].update(ldict)
                
            except:
                self.logger.error(f'something went wrong in line {i}')
        return slist

    def save_report(self, bundle_of_DiagnosticReport, outdir):
        for i in range(len(bundle_of_DiagnosticReport)):        
            r = bundle_of_DiagnosticReport[i]['resource']        
            url = r['presentedForm'][0]['url']
            contentType = r['presentedForm'][0]['contentType'] 
        
            response = self.sess.get(url, allow_redirects=True, cookies = self.cookies)
    
            try:
                filetype = self.filetypes[response.headers.get('content-type')]
            except:
                filetype = 'unknown'
                
            filename = r['identifier'][0]['value'] + '.' + filetype
            os.makedirs(outdir, exist_ok=True)  
            self.logger.debug(f'writing file: {filename} {contentType}')
            with open(f'{outdir}/{filename}', 'wb') as f:
                f.write(response.content)    


    def update_observation(self, slist, observation_code = 'zDDIMICS', date_delta = 365, disableTqdm=False):
        for i in range(len(slist)):
             subject = slist[i]['subject']
             study_date = dateparser.parse(slist[i]['Befunddatum']).date()
    
         
             obs_formdata = dict(_pretty='true',
                            subject = subject,
                            code= observation_code, # 'https://{ris_url}/LAB/Nexus/Swisslab/Observation/Code|zDDIMICS', # https://{ris_url}/HIS/Cerner/Medico/Observation/Code|zDDIMIA
                            _sort = 'date',
                            date=f'ge{study_date}'
                            )
             
             obs_data = self.get('Observation', obs_formdata, disableTqdm=disableTqdm)
             if obs_data != None:
                 for i_ in range(len(obs_data)):
                     try:
                         r = obs_data[i_]['resource']
                         obs_key = r['code']['coding'][0]['display']
                         obs_value = r['valueQuantity']['value']
                         obs_unit = r['valueQuantity']['unit']
                         try: 
                             issued = dateparser.parse(r['issued']).date() 
                         except:
                             self.logger.warning('no issued date given')
                             try:
                                 issued = dateparser.parse(r['effectiveDateTime']).date()   # last resort : effectiveInstant
                             except:
                                 self.logger.warning('even no effectiveDateTime')
                                 try:
                                     issued = dateparser.parse(r['effectiveInstant']).date()
                                 except:
                                     self.logger.error('could not find date information')
                                     issued = ''
              
                         #study_date = dateparser.parse(slist[i]['Untersuchungsdatum']).date()
                         datediff = abs(study_date - issued).days
                         self.logger.debug(f'datediff = {datediff}')
                         if datediff < date_delta:
                             
                             obs_dict = dict(unit = obs_unit
                                             )
                             
                             if date_delta < 14:
                                 timedelta_key = f'td_days_{i_}'
                                 obs_dict[timedelta_key] = datediff
                             
                             obs_key_nr = obs_key + f'_{i_}'
                             obs_dict[obs_key_nr] = obs_value
                             
                             slist[i].update(obs_dict)
                             
                             self.logger.debug(f'{obs_key}: {obs_value} {obs_unit} written !! !!')
                     except:
                         obs_key = ''
                         obs_value = ''
                         obs_unit = ''
                         issued = ''
                         datediff = ''
        return slist

    

    def get_imagestudy(self, patientid: str, modality='', disableTqdm=False):
        formdata = {'subject': patientid, 'modality': modality}
        entry = self.get('ImagingStudy', formdata, disableTqdm=disableTqdm)
        
        study_list = []
        for i in range(len(entry)):   
                study_ID = entry[i]['resource']['identifier'][1]['value'].replace('urn:oid:','')
                study_list.append(study_ID)
        return study_list




    def get_imagestudy2(self, patientid: str, modality='', disableTqdm=False):
        formdata = {'subject': patientid, 'shipProcedureCode': modality}
        entry = self.get('ImagingStudy', formdata, disableTqdm=disableTqdm)
        
        study_list = []
        for i in range(len(entry)):   
                study_ID = entry[i]['resource']['identifier'][1]['value'].replace('urn:oid:','')
                study_list.append(study_ID)
        return study_list
    
    
    
    def get_imagestudies(self, patientid: list, modality: str, disableTqdm=False):
        studylist = []
        for i in range(len(patientid)):
            imagestudy = self.get_imagestudy(patientid[i], modality, disableTqdm=disableTqdm)
            studylist.extend(imagestudy)
        return studylist
    
    def get_patient_info(self, surname, name, birthdate='gt1900-01-01', disableTqdm=False):
        formdata = {'family:contains': surname, 'name:contains': name, 'birthdate': birthdate, 'identifier':'https://{ris_url}/HIS/Cerner/Medico|'}
        patientinfo = self.get('Patient', formdata, disableTqdm=disableTqdm)
        
        if isinstance (patientinfo, list):
            for i in range(len(patientinfo)):
                r = patientinfo[i]['resource']
                family = r['name'][0]['family']
                name = r['name'][0]['given'][0]
                birthdate = r['birthDate']
                gender = r['gender']
                subject = r['id']
                try:
                    telecom = r['telecom'][0]['value'] 
                except:
                    telecom = '' 
                try:
                    street = r['address'][0]['line'][0]
                    plz = r['address'][0]['postalCode']
                    city = r['address'][0]['city']
                except:
                    telecom = ''  
                    street = ''
                    plz = ''
                    city = ''
                
                self.logger.debug(f'{family},{name}\t{birthdate}\t{telecom}\t{subject[:5]}...')
                
                
                patientinfo_dict = dict(subject = subject,
                                    family = family,
                                    name = name,
                                    birthdate = birthdate,
                                    gender = gender,
                                    street = street,
                                    plz = plz,
                                    city = city,
                                    telefon = telecom
                                    )
                
                
               # pprint.pprint(patientinfo[i])
        return patientinfo_dict
    
    
 ### updates patient informations in a dict of list with 'subject' = subject_id    
    def update_patient_info(self,list_of_dict_with_subject):
        patient_list = list_of_dict_with_subject
        for i in range(len(patient_list)):
            try:
                subject = patient_list[i]['subject']
            
                r = self.get_patient(subject)
                try:
                    family = r['name'][0]['family']
                    name = r['name'][0]['given'][0]
                    subject = r['id']
                    gender = r['gender']
                except:
                    family = ''
                    name = ''
                    subject = ''
                try:
                    birthdate = r['birthDate']
                    telecom = r['telecom'][0]['value'] 
                except:
                    telecom = '' 
                    birthdate = ''
                try:
                    street = r['address'][0]['line'][0]
                    plz = r['address'][0]['postalCode']
                    city = r['address'][0]['city']
                except:
                    street = ''
                    plz = ''
                    city = ''

                
                self.logger.debug(f'{family},{name}\t{birthdate}\t{telecom}\t{subject[:5]}...')
                
                
                info = dict(subject = subject,
                            family = family,
                            name = name,
                            gender = gender,
                            birthdate = birthdate,
                            street = street,
                            plz = plz,
                            city = city,
                            telefon = telecom
                            )
                patient_list[i].update(info)
            
            
            except:
                self.logger.warning ('no subject id for given patient')
                
            
            
        return patient_list
    
    

    def get_patient_list_from_observation(self, formdata, disableTqdm=False):
        
        bundle = self.get("Observation", formdata, disableTqdm=disableTqdm)

        patient_list = []  
        for i in range(len(bundle)):
            r = bundle[i]['resource']
            subject = r['subject']['reference'].replace('Patient/','')
            issued = dateparser.parse(r['issued']).date()
            
            info = dict(subject=subject,
                        issued=issued)
            if info not in patient_list:
                patient_list.append(info)
            
            
        return patient_list
    
    
    def get_observation_dates_from_patient_list(self, patient_list, formdata, disableTqdm=False):
        
        new_patient_list = [] 
        
        for i in range(len(patient_list)):            
            formdata['subject'] = patient_list[i]['subject']
            bundle = self.get("Observation", formdata, disableTqdm=disableTqdm)
            
            for i in range(len(bundle)):
                r = bundle[i]['resource']
                subject = r['subject']['reference'].replace('Patient/','')
                issued = dateparser.parse(r['issued']).date()
                
                info = dict(subject=subject,
                            issued=issued)
                if info not in new_patient_list:
                    new_patient_list.append(info)
                    
        return new_patient_list

        # for i in range(len(bundle)):
        #     r = bundle[i]['resource']
        #     subject = {'subject':r['subject']['reference'].replace('Patient/','')}
        #     patient_list.append(subject)
        # return new_patient_list
        
        
    def fetch_patient_list(self, observation: str):
        appUrl = self.appUrl
        url = appUrl + f'Observation?{observation}'
        self.logger.debug(f'fetching patients with Observation-code: {observation}')
        bundle = self.sess.get(url).json()
        
        total = bundle.get('total')   # get total nr of observations
        
        subject_list = []   # create empty list
        Offset = 0  # page offset begin 
        
        # iterate through all pages
        while Offset < total:               # set lower for total when testing
            nexturl = url + f'&_count=20&_shipOffset={Offset}' # ukefhir does not like higher count
            bundle = self.sess.get(nexturl).json()
            Offset += 20
            
            entry = bundle.get('entry')
            for i in range(len(entry)):   
                subject = entry[i]['resource']['subject']['reference']
                #print(subject)
                subject_list.append(subject)
        else: 
            self.logger.debug(f'Patients fetched: {len(subject_list)} Offset: {Offset}')
        
        return subject_list
        

    def fetch_patient_list_with_date(self, observation: str):
        appUrl = self.appUrl
        url = appUrl + f'Observation?{observation}'
        self.logger.debug(f'fetching patients with Observation-code: {observation}')
        bundle = self.sess.get(url).json()
        
        total = bundle.get('total')   # get total nr of observations
        
        subject_list = []   # create empty list
        Offset = 0  # page offset begin 
        
        # iterate through all pages
        while Offset < total:               # set lower for total when testing
            nexturl = url + f'&_count=20&_shipOffset={Offset}' # ukefhir does not like higher count
            bundle = self.sess.get(nexturl).json()
            Offset += 20
            
            entry = bundle.get('entry')
            for i in range(len(entry)):   
                subject = entry[i]['resource']['subject']['reference']
                date = entry[i]['resource']['issued']
                date = dateparser.parse(date)
                #print(subject)
                subject_list.append({'subject_id' : subject, 'date_obs' : date})
        else: 
            self.logger.debug(f'Patients fetched: {len(subject_list)} Offset: {Offset}')
        
        return subject_list
    
        
    def fetch_imagestudy_from_patient(self, patientid, modality, date_obs, timedelta = 21):    
        '''date_obs = time of covid test, timedelta = time diff to imagestudy and covid-test'''
        
        appUrl = self.appUrl
        url = appUrl + f'ImagingStudy?subject={patientid}&shipProcedureCode={modality}'
        bundle = self.sess.get(url).json()
        
        total = bundle.get('total')   # get total nr of observations
        
        study_list = []   # create empty list
        Offset = 0  # page offset begin 
        
        while Offset < total:
            nexturl = url + f'&_count=20&_shipOffset={Offset}'
            bundle = self.sess.get(nexturl).json()
            Offset += 20
            
            entry = bundle.get('entry')
            for i in range(len(entry)):   
                study_ID = entry[i]['resource']['identifier'][1]['value'].replace('urn:oid:','')
                date_study = entry[i]['resource']['started']
                date_study = dateparser.parse(date_study)
                date_diff = date_study - date_obs
                if abs(date_diff.days) < timedelta:
                    self.logger.debug(f'timedelta zur Studie: {date_diff}')
                    study_list.append({'StudyInstanceUID':study_ID, 'date_diff': date_diff})
                
        else: 
            self.logger.debug(f'study fetched: {len(study_list)} Offset: {Offset}')

        return study_list
    
    def extract_observations(self, bundle, disableTqdm=False):
        patient_list = []  
        for i in tqdm(range(len(bundle)), desc="Getting observation dates", disable=disableTqdm):
            r = bundle[i]['resource']
            subject = r['subject']['reference'].replace('Patient/','')
            try:
                issued = dateparser.parse(r['issued']).date()
            except:
                try:
                    #print(f'issued missing in data {i}')
                    issued = dateparser.parse(r['effectiveDateTime']['start']).date()
                except:
                    issued = dateparser.parse(r['effectivePeriod']['start']).date()
                    
            info = dict(subject=subject,
                        issued=issued)
            if info not in patient_list:
                patient_list.append(info)

        for i in tqdm(range(len(bundle)), desc="Updating observation values", disable=disableTqdm):
            r = bundle[i]['resource']
            subject = r['subject']['reference'].replace('Patient/','')
            try:
                issued = dateparser.parse(r['issued']).date()
            except:
                try:
                    #print(f'issued missing in data {i}')
                    issued = dateparser.parse(r['effectiveDateTime']['start']).date()
                except:
                    issued = dateparser.parse(r['effectivePeriod']['start']).date()
             
            info = dict(subject=subject,
                        issued=issued)
        
        
            for i_ in range(len(patient_list)):
                sub = patient_list[i_]['subject']
                iss = patient_list[i_]['issued']
                
                if info['subject'] == sub and info['issued'] == iss:
                    try:
                        code = r['code']['coding'][0]['display'][:15]
                        value = r['valueQuantity']['value']
                        #print(code)
                        #print(value)
                        #referenceRange = r['referenceRange'][0]['text']
                        obs = {}
                        obs[code] = value
                        patient_list[i_].update(obs)
                    except:
                        self.logger.warning('values missing')
            return patient_list