#!/usr/bin/env python
# -*- coding: utf-8 -*-

# from __future__ import print_function
import os, shutil, re
import sys
import argparse
from datetime import date, time, datetime, timedelta
from dateutil.relativedelta import *
import collections
from collections.abc import Iterable
import configparser
import logging
# from setuptools.command.test import test

"""
 Le but : lire une liste de sauvegardes, en extraire des noms de fichier :
 - la date,
 - l'heure
 pour pouvoir organiser les backup, par exemple :
  - toutes les sauvegardes des 7 derniers jours,
  - la dernière sauvegarde quotidienne des 4 dernieres semaine précédentes
  - la dernière sauvegarde mensuelle des 24 derniers mois
  - une sauvegarde par an des x précédentes années
 selon les regles :
 - dayly_rule(day_number, default=all) : 
    --all: toutes les sauvegardes des "day_number" derniers jour  (current - day_number)
    --last: la dernière sauvegarde de chaque journée des "day_number" derniers jour  (current - day_number)
    --first: la première sauvegarde de chaque journée des "day_number" derniers jour  (current - day_number)
 - weekly_rule(week_number, default=last) : 
    --last: la dernière sauvegarde de chaque semaine des "week_number" de chaque semaine suivant le dernier jour de la "dayly_rule"
      week((current - day_number))
    --first: la première sauvegarde de chaque semaine des "week_number"  de chaque  semaine suivant le dernier jour de la "dayly_rule"
      week((current - day_number))
 - monthly_rule(month_number, default=last) : 
    --last: la dernière sauvegarde de chaque mois des "month_number" de chaque mois suivant la derniere semaine de la "weekly_rule"
      month((current - (day_number + number_of_day(dayl_rule))
    --first: la première sauvegarde de chaque semaine des "week_number" de chaque mois suivant la derniere semaine de la "weekly_rule"
      month((current - (day_number + number_of_day(dayl_rule))
 - yearly_rule(year_number=0, default=last) :  year_number=0 <- eternel
      Si par exemple le plus ancien mois de la regle "monthly_rule" est "06/2018", la premiere année de sauvegarde sera "2018"
    --last: la dernière sauvegarde de chaque année des "year_number" de chaque année du dernier mois courant de la "monthly_rule"
      current_year(oldest_month(monthly_rule)
    --first: la première sauvegarde de chaque année des "year_number" de chaque année du dernier mois courant de la "monthly_rule"
      current_year(oldest_month(monthly_rule)
    --last_first: la dernière ET la première sauvegarde de chaque année des "year_number" de chaque année du dernier mois courant de la "monthly_rule"
      current_year(oldest_month(monthly_rule)
 - Deleting all other backup files
      
      
 il faut :
- lire la directory passée en argument 1 (*.zip)
- stocker sous dans une structure objet ? ou un simple tableau/dico
- appliquer successivement les règles : marquer à garder / a supprimer
- executer les actions et conserver le résultat dans un log

exemple de nom :
fname='2019_04_27_12_24_09_E12_LABSED2019-02-prod.zip'
nombase='E12_LABSED2019-02-prod'
2019_04_27_14_00_26_E12_LABSED2019-02-prod.zip
ou 
SAT-1703-PROD11_2019_04_27-01_00_45.zip
SAT-1703-PROD11_2019_04_27-01_01_37.zip

save and load objct : pickle
trouver une regex pour extraire :
année, mois, jour, heure, mm, sec, nom_base, ext
- ext : 4 derniers
- date et heure : 19 char, constant : 
- nom fichier : reste
datetime_list = re.findall('\d+',re.sub(nom_base,'',string_filename))
on enlève le nom de la base car il peut contenir des chiffres

RULES :
- Dayly: -d <int> <--first | --last | --all>

"""
flist =  [
'2018_05_31_14_56_44_LAB_SED-PROD-1805-21.zip',
'2018_06_30_11_30_00_LAB_SED-PROD-1805-21.zip',
'2018_06_30_23_30_11_LAB_SED-PROD-1805-21.zip',
'2018_07_24_11_30_02_LAB_SED-PROD-1805-21.zip',
'2018_08_31_23_30_21_LAB_SED-PROD-1805-21.zip',
'2018_09_30_23_30_25_LAB_SED-PROD-1805-21.zip',
'2018_10_31_23_30_00_LAB_SED-PROD-1805-21.zip',
'2018_11_07_16_34_11_LAB_SED-PROD-1805-21.zip',
'2019_03_29_15_55_20_LAB_SED-1811-02.zip',
'2019_03_29_16_34_43_E12_LABSED2019-02-prod.zip',
'2019_03_29_16_39_47_E12_LABSED2019-02-prod.zip',
'2019_03_29_23_30_20_LAB_SED-1811-02.zip',
'2019_03_30_00_00_01_E12_LABSED2019-02-prod.zip',
'2019_03_30_11_30_10_LAB_SED-1811-02.zip',
'2019_03_30_12_00_50_E12_LABSED2019-02-prod.zip',
'2019_03_30_23_30_12_LAB_SED-1811-02.zip',
'2019_03_31_00_00_24_E12_LABSED2019-02-prod.zip',
'2019_03_31_11_30_46_LAB_SED-1811-02.zip',
'2019_03_31_12_00_21_E12_LABSED2019-02-prod.zip',
'2019_03_31_23_30_32_LAB_SED-1811-02.zip',
'2019_04_01_00_00_13_E12_LABSED2019-02-prod.zip',
'2019_04_01_11_30_09_LAB_SED-1811-02.zip',
'2019_04_01_12_00_19_E12_LABSED2019-02-prod.zip',
'2019_04_01_23_30_43_LAB_SED-1811-02.zip',
'2019_04_02_00_00_05_E12_LABSED2019-02-prod.zip',
'2019_04_02_11_30_36_LAB_SED-1811-02.zip',
'2019_04_02_12_00_07_E12_LABSED2019-02-prod.zip',
'2019_04_02_23_30_14_LAB_SED-1811-02.zip',
'2019_04_03_00_00_43_E12_LABSED2019-02-prod.zip',
'2019_04_03_11_30_07_LAB_SED-1811-02.zip',
'2019_04_03_12_00_23_E12_LABSED2019-02-prod.zip',
'2019_04_03_12_03_47_E12_LABSED2019-02-prod.zip',
'2019_04_03_12_06_48_E12_LABSED2019-02-prod.zip',
'2019_04_03_23_30_21_LAB_SED-1811-02.zip',
'2019_04_04_00_00_09_E12_LABSED2019-02-prod.zip',
'2019_04_04_11_30_38_LAB_SED-1811-02.zip',
'2019_04_04_12_00_20_E12_LABSED2019-02-prod.zip',
'2019_04_04_23_30_19_LAB_SED-1811-02.zip',
'2019_04_05_00_00_18_E12_LABSED2019-02-prod.zip',
'2019_04_05_11_30_06_LAB_SED-1811-02.zip',
'2019_04_05_12_00_14_E12_LABSED2019-02-prod.zip',
'2019_04_05_23_30_19_LAB_SED-1811-02.zip',
'2019_04_06_00_00_12_E12_LABSED2019-02-prod.zip',
'2019_04_06_11_30_35_LAB_SED-1811-02.zip',
'2019_04_06_12_00_10_E12_LABSED2019-02-prod.zip',
'2019_04_06_23_30_13_LAB_SED-1811-02.zip',
'2019_04_07_00_00_09_E12_LABSED2019-02-prod.zip',
'2019_04_07_11_30_02_LAB_SED-1811-02.zip',
'2019_04_07_12_00_07_E12_LABSED2019-02-prod.zip',
'2019_04_07_23_30_03_LAB_SED-1811-02.zip',
'2019_04_08_00_00_05_E12_LABSED2019-02-prod.zip',
'2019_04_08_11_30_36_LAB_SED-1811-02.zip',
'2019_04_08_12_00_03_E12_LABSED2019-02-prod.zip',
'2019_04_08_23_30_16_LAB_SED-1811-02.zip',
'2019_04_09_00_00_01_E12_LABSED2019-02-prod.zip',
'2019_04_09_11_30_05_LAB_SED-1811-02.zip',
'2019_04_09_12_00_00_E12_LABSED2019-02-prod.zip',
'2019_04_09_23_30_14_LAB_SED-1811-02.zip',
'2019_04_10_00_00_06_E12_LABSED2019-02-prod.zip',
'2019_04_10_11_30_31_LAB_SED-1811-02.zip',
'2019_04_10_12_00_39_E12_LABSED2019-02-prod.zip',
'2019_04_10_23_30_09_LAB_SED-1811-02.zip',
'2019_04_11_00_00_17_E12_LABSED2019-02-prod.zip',
'2019_04_11_11_30_46_LAB_SED-1811-02.zip',
'2019_04_11_12_00_19_E12_LABSED2019-02-prod.zip',
'2019_04_11_23_30_20_LAB_SED-1811-02.zip',
'2019_04_12_00_00_17_E12_LABSED2019-02-prod.zip',
'2019_04_12_11_30_11_LAB_SED-1811-02.zip',
'2019_04_12_12_00_16_E12_LABSED2019-02-prod.zip',
'2019_04_12_23_30_01_LAB_SED-1811-02.zip',
'2019_04_13_00_00_17_E12_LABSED2019-02-prod.zip',
'2019_04_13_11_30_39_LAB_SED-1811-02.zip',
'2019_04_13_12_00_07_E12_LABSED2019-02-prod.zip',
'2019_04_13_23_30_21_LAB_SED-1811-02.zip',
'2019_04_14_00_00_05_E12_LABSED2019-02-prod.zip',
'2019_04_14_11_30_11_LAB_SED-1811-02.zip',
'2019_04_14_12_00_04_E12_LABSED2019-02-prod.zip',
'2019_04_14_23_30_07_LAB_SED-1811-02.zip',
'2019_04_15_00_00_02_E12_LABSED2019-02-prod.zip',
'2019_04_15_11_30_02_LAB_SED-1811-02.zip',
'2019_04_15_12_00_02_E12_LABSED2019-02-prod.zip',
'2019_04_15_23_30_40_LAB_SED-1811-02.zip',
'2019_04_16_00_00_15_E12_LABSED2019-02-prod.zip',
'2019_04_16_11_30_24_LAB_SED-1811-02.zip',
'2019_04_16_12_00_03_E12_LABSED2019-02-prod.zip',
'2019_04_16_23_30_08_LAB_SED-1811-02.zip',
'2019_04_17_00_00_05_E12_LABSED2019-02-prod.zip',
'2019_04_17_11_30_01_LAB_SED-1811-02.zip',
'2019_04_17_12_00_04_E12_LABSED2019-02-prod.zip',
'2019_04_17_23_30_01_LAB_SED-1811-02.zip',
'2019_04_18_00_00_50_E12_LABSED2019-02-prod.zip',
'2019_04_18_11_30_57_LAB_SED-1811-02.zip',
'2019_04_18_12_00_27_E12_LABSED2019-02-prod.zip',
'2019_04_18_23_30_41_LAB_SED-1811-02.zip',
'2019_04_19_00_00_15_E12_LABSED2019-02-prod.zip',
'2019_04_19_11_30_23_LAB_SED-1811-02.zip',
'2019_04_19_12_00_07_E12_LABSED2019-02-prod.zip',
'2019_04_19_12_00_20_E12_LABSED2019-02-prod.zip',
'2019_04_19_23_30_03_LAB_SED-1811-02.zip',
'2019_04_20_00_00_15_E12_LABSED2019-02-prod.zip',
'2019_04_20_00_00_36_E12_LABSED2019-02-prod.zip',
'2019_04_20_11_30_34_LAB_SED-1811-02.zip',
'2019_04_20_12_00_07_E12_LABSED2019-02-prod.zip',
'2019_04_20_12_00_27_E12_LABSED2019-02-prod.zip',
'2019_04_27_12_24_09_E12_LABSED2019-02-prod.zip',
'2019_04_27_14_00_26_E12_LABSED2019-02-prod.zip',
'2019_04_27_23_00_35_E12_LABSED2019-02-prod.zip',
'2019_04_28_08_00_41_E12_LABSED2019-02-prod.zip',
'2019_04_28_11_00_12_E12_LABSED2019-02-prod.zip',
'2019_04_28_14_00_32_E12_LABSED2019-02-prod.zip',
'2019_04_28_23_00_34_E12_LABSED2019-02-prod.zip',
'2019_04_29_08_00_49_E12_LABSED2019-02-prod.zip',
'2019_04_29_11_00_08_E12_LABSED2019-02-prod.zip',
'2019_04_29_14_00_41_E12_LABSED2019-02-prod.zip',
]


def verbose(what, aslist=False, log=False):
    if not log:
        if aslist and isinstance(what, (list, tuple)):
            printlist(what, enum=True)
        else:
            print(what)
    else:
        pass


class obj_fname(object):
    def __init__(self, fname=False, basename=False):
        self.filename = fname
        self.datetime = datetime(2000, 1, 1, 0, 0, 0)
        self.date = date(2000, 1, 1)
        self.time = time(0, 0, 0)
        self.y, self.m, self.d = 0, 0, 0
        self.h, self.mm, self.s = 0, 0, 0
        self.dbname = basename
        self.uid = False
        if fname and basename:
            self.set_extract_values(fname, basename)

    def set_extract_values(self, str=False, dbn=False):
        str_fname = self.filename if not str else str
        str_basename = self.dbname if not dbn else dbn
        if str_fname and str_basename and not (re.search(str_basename, str_fname)==None):
            datetime_list = re.findall('\d+',re.sub(str_basename, '', str_fname))
            self.y, self.m, self.d, self.h, self.mm, self.s = [int(x) for x in datetime_list]
            self.date = date(self.y, self.m, self.d)
            self.time = time(self.h, self.mm, self.s)
            self.datetime = datetime(self.y, self.m, self.d, self.h, self.mm, self.s)
            # self.date_str = ''.join(datetime_list[0:3])
            # self.time_str = ''.join(datetime_list[3:])
            self.uid = int(self.datetime.strftime('%Y%m%d%H%M%S'))

    def get_uid_from_datetime(_dt):
        if _dt:
            return int(_dt.strftime('%Y%m%d%H%M%S'))

class rule(object):

    def __init__(self, _name=False, _type=False, _number=False, _params=False):
        self.name = _name
        self.type = self.set_type(_type)
        self.keep = self.set_number(_number)
        self.keep_iso = self.set_number(_number)
        self.keep_min = self.set_number(_number)
        self.keep_max = self.set_number(_number)
        self.policy = self.set_params(_params) if _params else 'all'
        self.start_date = False
        self.end_date = False
        self.first_list = []
        self.last_list = []

    def set_type(self, type):
        if type not in ['day', 'week', 'month', 'year']:
            raise ValueError("erreur de type de regle \n must be in ['day', 'week', 'month', 'year']")
        else:
            return type

    def set_number(self, nb):
        try:
            num = int(nb)
        except ValueError:
            raise ValueError("erreur de durée de regle \n must be an integer")
        else:
            return num

    def set_params(self, params):
        # if params and params not in ['last', 'first', 'first_last', 'all']:
        if params not in ['last', 'first', 'first_last', 'all']:
            raise ValueError("erreur de params \n 'policy' param must be in : ['last', 'first', 'first_last', "
                             "'all'] in Rule: " + self.name)
        else:
            return params

    def get_startdate(self, uid_format=False):
        if uid_format:
            return obj_fname.get_uid_from_datetime(self.start_date)
        else:
            return self.start_date

    def get_enddate(self, uid_format=False):
        if uid_format:
            return obj_fname.get_uid_from_datetime(self.end_date)
        else:
            return self.end_date

    def get_policy(self):
        return self.policy if self.policy is not None else 'last'

class set_of_rules(object):
    align_calendar = True
    delete_files = False
    dir_to_archive_files = False
    start_yesterday = False # True : start applying rules from yesterday, False: start from le youngest file
    period_name = {'dayly': 'day', 'weekly': 'week', 'monthly': 'month', 'yearly': 'year'}
    inv_period_name = dict([val, key] for key, val in period_name.items())

    def __init__(self, _file_name):
        self.file_name = _file_name
        self.dictOfRules = {}

    def check_args(self, rule=None, type=False, name=False):
        if rule != None:
            if not isinstance(rule, rule):
                raise TypeError("An instancied object of class 'rule' need to be passed")
        if type:
            if type not in self.period_name.values():
                raise ValueError("erreur de type de regle \n must be in ['day', 'week', 'month', 'year']")
        if name:
            if name not in self.dictOfRules[type].keys():
                raise ValueError("Le nom de la règle est inconnu dans " & self.dictOfRules[type].keys())
        return True

    def add_rule(self, _name, _type, _rule=None):
        if _name == False:
            _name = set_of_rules.period_name[_type]
        elif self.check_args(type=_type):
            _rule = _rule if _rule != None else rule(_name, _type)
            self.dictOfRules[_rule.type] = {_rule.name: _rule}
            print(self.dictOfRules)


    def get_rule(self, _type, _name):
        if self.check_args(type=_type, name=_name):
            return self.dictOfRules[_type][_name]

    def get_list_of_rules(self):
        # return [rule_dict.values() for rule_dict in list(self.dictOfRules.values())]
        return [list(self.dictOfRules[key].values())[0] for key in self.dictOfRules.keys()]

    def get_type_dict(self, _type):
        if self.check_args(type=_type):
            return self.dictOfRules[_type]

    def modify_rule(self, _type, _name, _rule, _newname=False):
        if self.check_args(type=_type, name=_name, rule=_rule):
            if _newname != False:
                self.dictOfRules[_type][_newname] = self.dictOfRules[_type].pop(_name)
            else:
                self.dictOfRules[_type][_name] = _rule

    def delete_rule(self, _type, _name):
        if self.check_args(type=_type, rule=_rule):
            self.dictOfRules[_type].pop(_name)



def get_file_list(path=False):
    return os.listdir(path) if path else False


def printlist(l, enum=False):
    if enum:
        numformat = '{:0' + str(len(str(len(l)))) + 'd}'
        for i, e in enumerate(l):
            print(numformat.format(i), e)
    else:
        for e in l:
            print(e)

def get_default_conf():
    parser = configparser.ConfigParser()
    defaultConfig = """
[params]
default_basename = 
align_calendar = True
delete_files = False
dir_to_archive_files = ./archived
log_file = gestion_backup_file.log
start_yesterday = False 
each_ended_month = True
each_ended_year = True
     
[dayly]
keep = 7
policy = all

[weekly] 
keep = 4
policy = last

[monthly]
keep = 12
keep_max = 11
keep_min = 6
policy = last

[yearly]
keep = 10
policy = last
"""
    parser.read_string(defaultConfig)
    return parser

def read_config(filename='check_bckp_file.conf', setOfRules='Rules'):
    def get_val_typed(section, option):
        try:
            to_check = eval(config.get(section, option))
        except:
            return config.get(section, option)
        else:
            return to_check

    config = configparser.ConfigParser()
    config.read(filename)
    s_of_r = set_of_rules(setOfRules)
    # type = {'params':'param', 'dayly': 'day', 'weekly': 'week', 'monthly': 'month', 'yearly': 'year'}
    for section in config.sections():
        if section == 'params':
            for option in config[section]:
                setattr(s_of_r, option, get_val_typed(section, option))
                print(option + ':', s_of_r.__getattribute__(option))
        else:
            s_of_r.add_rule(_name=section, _type=s_of_r.period_name[section])
            cur_rule = s_of_r.get_rule(_name=section, _type=s_of_r.period_name[section])
            for option in config[section]:
                setattr(cur_rule, option, get_val_typed(section, option))
                print('rule:', cur_rule.name, 'option:', option + ':', cur_rule.__getattribute__(option))

    # print('defined rules:', s_of_r.setname, '\n day :', s_of_r.day_define, \
    #                         '\n week :', s_of_r.week_define, \
    #                         '\n month :', s_of_r.month_define, \
    #                         '\n year :', s_of_r.year_define)
    return s_of_r


def write_defaultconfig(filename='check_bckp_file.conf'):
    config = get_default_conf()
    with open(filename, 'w') as configfile:
        config.write(configfile)


def compute_start_end_dates(sor: set_of_rules, first_objfn: obj_fname):
    """ Compute first more prox date and last far date from begining"""
    isocal = {'curyear': 0, 'weeknum': 1, 'weekday': 2}
    if sor.start_yesterday:
        td = datetime.today()
        _StartDate = datetime(td.year, td.month, td.day - 1, 23,59,59)
    else:
        _StartDate = first_objfn.datetime
    cur_start_date = cur_end_date = _StartDate
    _minus_One = timedelta(days=1)

    print("--------calcul des dates et des regles------------")

    sor_list = sor.get_list_of_rules()
    for cur_rule in sor_list:
        cur_start_date = cur_end_date
        cur_rule.start_date = cur_start_date
        cur_rule.keep_iso = cur_rule.keep
        if cur_rule.type == 'day':
            td = timedelta(days=int(cur_rule.keep))
            cur_end_date = cur_start_date - td
            if sor.align_calendar:
                # set end_date to the first day of current week
                dt = timedelta(days=cur_end_date.isocalendar()[isocal['weekday']])
                cur_end_date -= dt
                cur_rule.keep_iso = int(cur_rule.keep) + dt.days

            cur_rule.last_list = []
            for i in range(0,int(cur_rule.keep_iso)):
                dt = timedelta(days=i)
                cur_rule.last_list.append((cur_start_date - dt).replace(hour=23, minute=59, second=59))
                print('last_days: \t', cur_rule.last_list[-1])
            cur_rule.first_list = []
            for i in range(0,int(cur_rule.keep_iso)):
                dt = timedelta(days=i+1)
                cur_rule.first_list.append((cur_end_date + dt).replace(hour=0, minute=0, second=0))
                print('first_days: \t', cur_rule.first_list[-1])

        elif cur_rule.type == 'week':
            td = timedelta(weeks=int(cur_rule.keep))
            cur_end_date = cur_start_date - td
            if sor.align_calendar:
                # set end_date to the first day of current week
                dt = timedelta(days=cur_end_date.isocalendar()[isocal['weekday']])
                cur_date_week_number = datetime.date(cur_end_date).isocalendar()[1]
                cur_end_date -= dt
                cur_rule.keep_iso = int(cur_rule.keep) - datetime.date(cur_end_date).isocalendar()[1] \
                                                       + cur_date_week_number

            cur_rule.last_list = []
            for i in range(0,int(cur_rule.keep_iso)):
                dt = timedelta(days=i*7)
                cur_rule.last_list.append((cur_start_date - dt).replace(hour=23, minute=59, second=59))
                print('last_weeks: \t', cur_rule.last_list[-1])
            cur_rule.first_list = []
            for i in range(0,int(cur_rule.keep_iso)):
                dt = timedelta(days=i*7)
                cur_rule.first_list.append((cur_end_date + dt).replace(hour=0, minute=0, second=0))
                print('first_weeks: \t', cur_rule.first_list[-1])

        elif cur_rule.type == 'month':
            if sor.each_ended_month:
                # this option assume that _StartDate is the last day of previous month
                # even if weeks keeping go more further than this date
                # note the use of '_StartDate' instead of 'cur_start_date'
                td = relativedelta(day=1) + relativedelta(days=1)
                cur_start_date = _StartDate - td  # go to the last day of previous month
            td = relativedelta(months=int(cur_rule.keep), day=1)
            cur_end_date -= td

            cur_rule.last_list = []
            for i in range(0,int(cur_rule.keep_iso)):
                dt = relativedelta(months=i-1, day=1) + relativedelta(days=1)
                cur_rule.last_list.append((cur_start_date - dt).replace(hour=23, minute=59, second=59))
                print('last monthes: \t', cur_rule.last_list[-1])
            cur_rule.first_list = []
            for i in range(0,int(cur_rule.keep_iso)):
                dt = relativedelta(months=i+1, day=1)
                cur_rule.first_list.append((cur_end_date + dt).replace(hour=0, minute=0, second=0))
                print('first_monthes: \t', cur_rule.first_list[-1])

        elif cur_rule.type == 'year':
            if sor.each_ended_year:
                # this option assume that _StartDate is the last day of previous month
                # even if weeks keeping go more further than this date
                # note the use of '_StartDate' instead of 'cur_start_date'
                td = relativedelta(month=1, day=1) + relativedelta(days=1)
                cur_start_date = _StartDate - td  # go to the last day of previous year
            td = relativedelta(years=int(cur_rule.keep), day=1)
            cur_end_date -= td

            cur_rule.last_list = []
            for i in range(0,int(cur_rule.keep_iso)):
                dt = relativedelta(years=i)
                cur_rule.last_list.append((cur_start_date - dt).replace(hour=23, minute=59, second=59))
                print('last years: \t', cur_rule.last_list[-1])
            cur_rule.first_list = []
            for i in range(0,int(cur_rule.keep_iso)):
                dt = relativedelta(year=i, month=1, day=1)
                cur_rule.first_list.append((cur_end_date + dt).replace(hour=0, minute=0, second=0))
                print('first years: \t', cur_rule.first_list[-1])

        cur_rule.start_date, cur_rule.end_date = cur_start_date, cur_end_date

        print('-' * 100)
        print('Type={0} \t keep={1} \t debut={2} \t fin={3} \t (Policy={4})'.format(cur_rule.type,
                                                                                 cur_rule.keep,
                                                                                 cur_rule.start_date,
                                                                                 cur_rule.end_date,
                                                                                 cur_rule.get_policy())
              )
        print('=' * 100)
        cur_end_date -= _minus_One  # go to day before to start next cur_rule



def test_create_file_list(flist, path=False):
    if path and not os.path.exists(path):
        try:
            os.mkdir(path)
        except:
            sys.exit(1)
    for filename in flist:
        corpus = 'This is a file test for file: ' + filename + '\n'
        with open(os.path.join(path,filename), 'w') as test_file:
            if os.fstat(test_file.fileno()).st_size == 0:
                test_file.write(corpus)

def generate_test_list(nombase, dayh=[0,12], nbdays=500, sd=datetime.today()):
    def strf(l):
        return ['{:02d}'.format(i) for i in l]
    dstart = datetime(sd.year, sd.month, sd.day, 0,0,0)
    sorted(dayh) # , reverse=True)
    flist = []
    for day in range(0, nbdays):
        for h in dayh:
            dt = timedelta(days=day, hours=24-h)
            cd = dstart - dt
            fname = "_".join(strf([cd.year, cd.month, cd.day, cd.hour, cd.minute, cd.second]) + [nombase + ".zip"])
            flist.append(fname)
    return flist

def set_2keep_2del(olist, setofrule):
    """"prendre un jeu de regeles l'une après l'autre,
    pour :  - traiter la règle journalière, puis semaine, mois, etc"""

    def is_date_in(d, l):
        return d in [d.date() for d in l]

    s_o_r = setofrule.get_list_of_rules()
    res_dict = {}

    for rule in s_o_r:
        if rule.type not in res_dict.keys():
            res_dict[rule.type] = []
        cur_startdate, cur_enddate = rule.get_startdate(uid_format=False), rule.get_enddate(uid_format=False)
        # setting the right list of date to use, regarding rule's policy
        if rule.get_policy() == 'first':
            date_list = rule.last_list
        elif rule.get_policy() == 'last':
            date_list = rule.last_list
        elif rule.get_policy() == 'first_last':
            date_list = rule.first_list + rule.last_list
        else:  # mean 'all'
            date_list = []

        # creating a dict of keys on each date, values list of file found at that date
        # cur_list = {k.date():[] for k in date_list} if rule.get_policy() != 'all' else {'all':[]}

        if rule.get_policy() == 'all':
            cur_obj_list = [o.objfn for o in olist]
        else:  # Filtering full list of file from date in current rule
            cur_obj_list = [o.objfn for o in olist if o.objfn.datetime in date_list]
        _all = rule.get_policy() == 'all'  # set outside the loop for accessing once only
        cur_obj_list = [o.objfn for o in olist if (o.objfn.datetime in date_list) \
                        and (cur_startdate >= o.objfn.datetime >= cur_enddate) or _all]

        # then, as the list by dates is done, keeping only files in accordance with policy rule
        for key, dl in cur_obj_list.items():
            # sorting list for current date newer (last) in first place
            dl.sort(key=lambda o: o.datetime, reverse=True)
            #apply final cut
            cur_obj = False
            if len(dl) == 0:
                continue
            elif rule.get_policy() == 'first':
                cur_obj = dl[-1]  # keeping older
            elif rule.get_policy() == 'last':
                cur_obj = dl[0]  # keeping newer
            elif rule.get_policy() == 'first_last':
                cur_obj = [dl[-1], dl[-1]]  # keeping both
            if cur_obj:
                cur_list[key] = cur_obj

        # to_keep[rule.type] = [fn for fn in [el for el in cur_list.values()]]
        ll = list(flatten(cur_list.values()))
        keeped.append(ll)
    # tobj = collections.namedtuple('tupleobj','uid objfn')
    # return [tobj(int(o.uid), o) for o in list(flatten(keeped))]
    return [int(o.uid) for o in list(flatten(keeped))]




# def set_2keep_2del(olist, setofrule):
#     def is_date_in(d, l):
#         return d in [d.date() for d in l]
#     keeped = []
#     s_o_r = setofrule.get_list_of_rules()
#     for rule in s_o_r:
#         cur_startdate, cur_enddate = rule.get_startdate(uid_format=False), rule.get_enddate(uid_format=False)
#         pol = rule.policy
#         pol = rule.get_policy()
#         date_list = rule.first_list if rule.get_policy() == 'first' else rule.last_list
#         date_list += rule.first_list if rule.get_policy() == 'first_last' else []
#         # creating a dict keys on eache dates, values list of file found at that date
#         cur_list = {k.date():[] for k in date_list} if rule.get_policy() != 'all' else {'all':[]}
#         for o in olist: # then feeding the dict
#             # take care that end date is smaller than start date
#             if cur_startdate >= o.objfn.datetime >= cur_enddate:
#                 #calculate option paramters
#                 if rule.get_policy() == 'all':
#                     cur_list['all'].append(o.objfn)
#                 elif rule.get_policy() == 'first':
#                     if is_date_in(o.objfn.date, rule.first_list):
#                         cur_list[o.objfn.date].append(o.objfn)
#                 elif rule.get_policy() == 'last':
#                     if is_date_in(o.objfn.date, rule.last_list):
#                         cur_list[o.objfn.date].append(o.objfn)
#                 elif rule.get_policy() == 'first_last':
#                     if is_date_in(o.objfn.date, rule.first_list) or\
#                             is_date_in(o.objfn.date, rule.last_list):
#                         cur_list[o.objfn.date].append(o.objfn)
#         # then, as the list by dates is done, keeping only files in accordance with policy rule
#         for key, dl in cur_list.items():
#             # sorting list for current date newer (last) in first place
#             dl.sort(key=lambda o: o.datetime, reverse=True)
#             #apply final cut
#             cur_obj = False
#             if len(dl) == 0:
#                 continue
#             elif rule.get_policy() == 'first':
#                 cur_obj = dl[-1]  # keeping older
#             elif rule.get_policy() == 'last':
#                 cur_obj = dl[0]  # keeping newer
#             elif rule.get_policy() == 'first_last':
#                 cur_obj = [dl[-1], dl[-1]]  # keeping both
#             if cur_obj:
#                 cur_list[key] = cur_obj
#
#         # to_keep[rule.type] = [fn for fn in [el for el in cur_list.values()]]
#         ll = list(flatten(cur_list.values()))
#         keeped.append(ll)
#     # tobj = collections.namedtuple('tupleobj','uid objfn')
#     # return [tobj(int(o.uid), o) for o in list(flatten(keeped))]
#     return [int(o.uid) for o in list(flatten(keeped))]

def flatten(items):
    """Yield items from any nested iterable; see Reference."""
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            for sub_x in flatten(x):
                yield sub_x
        else:
            yield x




def main(arguments):

    parser = argparse.ArgumentParser(
        description="Apply some rules (default or stored) to manage rolling backup file on days,weeks,monthes ans years",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('Invariant', help="Name of the file backuped inside all current backup file's name")
    parser.add_argument('Bckpfiles_path', help="Path to directory containing the files bakuped")
    parser.add_argument('-r', '--rulesFile', help="Name of the file containing rules", type=argparse.FileType('r'))
    parser.add_argument('-d', '--defaultrules', help="Display default rules")
    parser.add_argument('-a', '--archdir', help="set archive path for non deletion option", default='./archived')
    parser.add_argument('-l', '--logfile', help="log file",
                        default=sys.stdout, type=argparse.FileType('w'))
    args = parser.parse_args(arguments)
    if os.path.exists(os.path.realpath(args.Bckpfiles_path)):
        wrkdir = os.path.realpath(args.Bckpfiles_path)
    else:
        write_defaultconfig()
        raise ValueError(args.Bckpfiles_path + " does'nt exist.\nDefault example config file have been written")

    if args.rulesFile != None and os.path.exists(os.path.realpath(args.rulesFile.name)):
        SetOfRules = read_config(os.path.realpath(args.rulesFile.name), setOfRules='MyRules')
    else:
        raise ValueError(' option -r or RuleFile does not exist')

    archdir = os.path.realpath(SetOfRules.dir_to_archive_files if SetOfRules.dir_to_archive_files else args.archdir)
    if not os.path.exists(os.path.realpath(archdir)):
        try:
            os.mkdir(archdir)
        except:
            raise ValueError("archive dir : " + archdir + " cannot find or create dir")

    basename = args.Invariant
    print("args namespace=", args)
    print('working dir=', wrkdir, "\narchive dir=", archdir)
    print('basename=', basename)
    flist = generate_test_list(basename, nbdays=1000,dayh=[2,10,13, 16])
    test_create_file_list(flist, wrkdir)
    flist_from_dir = get_file_list(wrkdir)
    # printlist(flist_from_dir, enum=True)
    flist_basename = [e for e in flist_from_dir if not (re.search(basename, e)==None)]
    # printlist(flist_basename)
    tobj = collections.namedtuple('tupleobj','uid objfn')
    # then making a sorted list of tuple (uid, ojb) to be manipulated easily
    olist = [tobj(int(o.uid), o) for o in [obj_fname(fn, basename) for fn in flist_basename]]
    olist = sorted(olist, key=lambda tuplefn: tuplefn.uid, reverse=True)
    # printlist(olist, enum=True)
    compute_start_end_dates(SetOfRules, olist[0].objfn)
    uids2keep = set_2keep_2del(olist, SetOfRules)
    printlist(uids2keep, True)
    logging.info("keeped list of {0} file(s) : {1}".format( len(uids2keep), ', '.join([str(uid) for uid in uids2keep])))
    # doing last job
    # uids2keep = [o.uid for o in keeped]
    failed = done = []
    for fileobj in olist:
        if fileobj.uid not in uids2keep:
            _filename = os.path.join(wrkdir, fileobj.objfn.filename)
            if SetOfRules.delete_files:
                try:
                    os.remove(_filename)
                except PermissionError:
                    failed.append(fileobj)
                    raise ErrorValue("permission denied")
                else:
                    done.append(_filename)
            elif SetOfRules.dir_to_archive_files:
                # if
                try:
                    shutil.move(os.path.join(os.getcwd(),_filename),
                              os.path.join(os.getcwd(),
                                           SetOfRules.dir_to_archive_files,
                                           os.path.basename(_filename)
                                           )
                             )
                except PermissionError:
                    failed.append(_filename)
                    return "permission denied"
                else:
                    done.append(_filename)


    print("="*10 + " Job finished " + "="*10)
    printlist(done)
    if len(failed) > 0:
        logging.warning(failed)
        print("="*10 + "failed" + "="*10)
        printlist(failed)





if __name__ == '__main__':

    # sys.exit(main(sys.argv[1:]))
    sys.exit(main("E12_LABSED2019-04-prod ./test_dir -l log.log -r ./check_bckp_file.conf".split(" ")))

