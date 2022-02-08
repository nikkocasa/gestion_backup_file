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
 - dayly_rule(day_number, default=all_items) : 
    --all_items: toutes les sauvegardes des "day_number" derniers jour  (current - day_number)
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
- Dayly: -d <int> <--first | --last | --all_items>

"""
flist = [
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


def verbose(what, enum=False, log=False, recur=False):
    if not log:
        if enum and isinstance(what, (list, tuple)):
            printlist(what, enum, recur)
        else:
            print(what)
    else:
        pass


class Obj_Fname(object):

    def __init__(self, fname=False, basename=False):
        self.filename = fname
        self.datetime = datetime(2000, 1, 1, 0, 0, 0)
        self.date = date(2000, 1, 1)
        self.time = time(0, 0, 0)
        self.y, self.m, self.d = 0, 0, 0
        self.h, self.mm, self.s = 0, 0, 0
        self.dbname = basename
        self.uid = False
        self.eow = self.is_end_of_week(self.date)
        self.eom = self.is_end_of_month(self.date)
        self.eoy = self.is_end_of_year(self.date)
        if fname and basename:
            self.set_extract_values(fname, basename)

    def set_extract_values(self, str=False, dbn=False):
        str_fname = self.filename if not str else str
        str_basename = self.dbname if not dbn else dbn
        if str_fname and str_basename and not (re.search(str_basename, str_fname) == None):
            datetime_list = re.findall('\d+', re.sub(str_basename, '', str_fname))
            self.y, self.m, self.d, self.h, self.mm, self.s = [int(x) for x in datetime_list]
            self.date = date(self.y, self.m, self.d)
            self.time = time(self.h, self.mm, self.s)
            self.datetime = datetime(self.y, self.m, self.d, self.h, self.mm, self.s)
            # self.date_str = ''.join(datetime_list[0:3])
            # self.time_str = ''.join(datetime_list[3:])
            self.uid = int(self.datetime.strftime('%Y%m%d%H%M%S'))

    def get_uid_from_datetime(dt):
        return int(dt.strftime('%Y%m%d%H%M%S')) if dt else False

    def is_end_of_week(self, dt):
        return dt.isoweekday() == 7 if dt else False

    def is_end_of_month(self, dt):
        return dt.month != (dt + timedelta(days=1)).month if dt else False

    def is_end_of_year(self, dt):
        return dt.year != (dt + timedelta(days=1)).year if dt else False


class Rule(object):

    def __init__(self, _name=False, _type=False, _number=False, _params=False):
        self.name = _name
        self.type = self.set_type(_type)
        self.keep = self.set_number(_number)
        self.keep_iso = self.set_number(_number)
        self.keep_min = self.set_number(_number)
        self.keep_max = self.set_number(_number)
        self.policy = self.set_params(_params) if _params else 'all_items'
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
        # if params and params not in ['last', 'first', 'first_last', 'all_items']:
        if params not in ['last', 'first', 'first_last', 'all_items']:
            raise ValueError("erreur de params \n 'policy' param must be in : ['last', 'first', 'first_last', "
                             "'all_items'] in Rule: " + self.name)
        else:
            return params

    def get_startdate(self, uid_format=False):
        if uid_format:
            return Obj_Fname.get_uid_from_datetime(self.start_date)
        else:
            return self.start_date

    def get_enddate(self, uid_format=False):
        if uid_format:
            return Obj_Fname.get_uid_from_datetime(self.end_date)
        else:
            return self.end_date

    def get_policy(self):
        return self.policy if self.policy is not None else 'last'


class Set_of_Rules(object):
    align_calendar = True
    delete_files = False
    dir_to_archive_files = False
    start_yesterday = False  # True : start applying rules from yesterday, False: start from le youngest file
    period_name = {'dayly': 'day', 'weekly': 'week', 'monthly': 'month', 'yearly': 'year'}
    inv_period_name = dict([val, key] for key, val in period_name.items())

    def __init__(self, _file_name):
        self.file_name = _file_name
        self.dictOfRules = {}

    def check_args(self, rule=None, type=False, name=False):
        if rule != None:
            if not isinstance(rule, rule):
                raise TypeError("An instancied object of class 'Rule' need to be passed")
        if type:
            if type not in self.period_name.values():
                raise ValueError("erreur de type de regle \n must be in ['day', 'week', 'month', 'year']")
        if name:
            if name not in self.dictOfRules[type].keys():
                raise ValueError("Le nom de la règle est inconnu dans " & self.dictOfRules[type].keys())
        return True

    def add_rule(self, _name, _type, _rule=None):
        if _name == False:
            _name = Set_of_Rules.period_name[_type]
        elif self.check_args(type=_type):
            _rule = _rule if _rule != None else Rule(_name, _type)
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


def printlist(l, enum=False, recur=False):
    if enum:
        numformat = '{:0' + str(len(str(len(l)))) + 'd}'
        for i, e in enumerate(l):
            if recur > 0 and isinstance(e, (list, tuple)):
                printlist(e, True, recur + 1)
            else:
                print(chr(9) * recur + numformat.format(i), e)
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
policy = all_items

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
    s_of_r = Set_of_Rules(setOfRules)
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
                print('Rule:', cur_rule.name, 'option:', option + ':', cur_rule.__getattribute__(option))

    # print('defined rules:', s_of_r.setname, '\n day :', s_of_r.day_define, \
    #                         '\n week :', s_of_r.week_define, \
    #                         '\n month :', s_of_r.month_define, \
    #                         '\n year :', s_of_r.year_define)
    return s_of_r


def write_defaultconfig(filename='check_bckp_file.conf'):
    config = get_default_conf()
    with open(filename, 'w') as configfile:
        config.write(configfile)


def compute_start_end_dates(sor: Set_of_Rules, first_objfn: Obj_Fname):
    """ Compute first more prox date and last far date from begining"""
    isocal = {'curyear': 0, 'weeknum': 1, 'weekday': 2}
    if sor.start_yesterday:
        td = datetime.today()
        _StartDate = datetime(td.year, td.month, td.day - 1, 23, 59, 59)
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
            for i in range(0, int(cur_rule.keep_iso)):
                dt = timedelta(days=i)
                cur_rule.last_list.append((cur_start_date - dt).replace(hour=23, minute=59, second=59))
                print('last_days: \t', cur_rule.last_list[-1])
            cur_rule.first_list = []
            for i in range(0, int(cur_rule.keep_iso)):
                dt = timedelta(days=i + 1)
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
            for i in range(0, int(cur_rule.keep_iso)):
                dt = timedelta(days=i * 7)
                cur_rule.last_list.append((cur_start_date - dt).replace(hour=23, minute=59, second=59))
                print('last_weeks: \t', cur_rule.last_list[-1])
            cur_rule.first_list = []
            for i in range(0, int(cur_rule.keep_iso)):
                dt = timedelta(days=i * 7)
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
            for i in range(0, int(cur_rule.keep_iso)):
                dt = relativedelta(months=i - 1, day=1) + relativedelta(days=1)
                cur_rule.last_list.append((cur_start_date - dt).replace(hour=23, minute=59, second=59))
                print('last monthes: \t', cur_rule.last_list[-1])
            cur_rule.first_list = []
            for i in range(0, int(cur_rule.keep_iso)):
                dt = relativedelta(months=i + 1, day=1)
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
            for i in range(0, int(cur_rule.keep_iso)):
                cur_rule.last_list.append(datetime(cur_start_date.year - i, 12, 31, 23, 59, 59))
                # dt = relativedelta(years=i)
                # cur_rule.last_list.append((cur_start_date - dt).replace(hour=23, minute=59, second=59))
                print('last years: \t', cur_rule.last_list[-1])
            cur_rule.first_list = []
            for i in range(0, int(cur_rule.keep_iso) - 1):
                cur_rule.first_list.append(datetime(cur_start_date.year - i, 1, 1, 0,0,0))
                # dt = relativedelta(year=i, month=1, day=1)
                # cur_rule.first_list.append((cur_end_date + dt).replace(hour=0, minute=0, second=0))
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
        with open(os.path.join(path, filename), 'w') as test_file:
            if os.fstat(test_file.fileno()).st_size == 0:
                test_file.write(corpus)


def generate_test_list(nombase, dayh=[0, 12], nbdays=500, sd=datetime.today()):
    def strf(l):
        return ['{:02d}'.format(i) for i in l]

    dstart = datetime(sd.year, sd.month, sd.day, 0, 0, 0)
    sorted(dayh)  # , reverse=True)
    flist = []
    for day in range(0, nbdays):
        for h in dayh:
            dt = timedelta(days=day, hours=24 - h)
            cd = dstart - dt
            fname = "_".join(strf([cd.year, cd.month, cd.day, cd.hour, cd.minute, cd.second]) + [nombase + ".zip"])
            flist.append(fname)
    return flist


def set_2keep_2del(olist, setofrule):
    """"prendre un jeu de regeles l'une après l'autre,
    pour :  - traiter la règle journalière, puis semaine, mois, etc
    it is important to note that the loop if on all the olist list,
    some file may be used in more tha one Rule : i.e. the las 'dayly' Rule's file
    may be also the first of weekly Rule. If dayly computing take thos fileof the original olist,
    then another one will be abnormally kept in its place.
    retourne un tuple de set"""

    def is_date_in(d, l):
        return d in [d.date() for d in l]

    s_o_r = setofrule.get_list_of_rules()

    # compute the 2keep dict = {key=Rule.type(i.e. : day, week,..), value=list of obj_file}
    to_keep = {}  #
    for rule in s_o_r:
        # 1) adding the new list if key doesn't exist in dict
        # then : computing the intervals of date and the Rule's policy on which one to keep (first, last, ..)
        # then : for each Rule , buiding a dict of {'rule_type': set(of uids of onj_file)}
        if rule.type not in to_keep.keys():
            to_keep[rule.type] = []

        cur_startdate, cur_enddate = rule.get_startdate(uid_format=False), rule.get_enddate(uid_format=False)
        # cur_obj_set = set of list of [uid] filtering on all obj-file 'olist'
        # keep all the files between dates
        cur_rule_list = sorted([o for o in olist if cur_startdate >= o.objfn.datetime >= cur_enddate],
                               key=lambda o: o.uid)
        # on crée une liste des période unique en fonction de la regle (jour pour daylyn semaine pour week ..)
        d_period = {}
        if rule.type == 'day':
            days = sorted(list(set([o.objfn.datetime.day for o in cur_rule_list])))
            for day in days:
                d_period[day] = sorted([o for o in cur_rule_list if o.objfn.datetime.day == day], key=lambda o: o.uid)

        elif rule.type == 'week':  # isocalendar()[1] return the week number in the year
            weeks = sorted(list(set([o.objfn.datetime.isocalendar()[1] for o in cur_rule_list])))
            for week in weeks:
                d_period[week] = sorted([o for o in cur_rule_list if o.objfn.datetime.isocalendar()[1] == week],
                                        key=lambda o: o.uid)

        elif rule.type == 'month':
            monthes = sorted(list(set([o.objfn.datetime.month for o in cur_rule_list])))
            for month in monthes:
                d_period[month] = sorted([o for o in cur_rule_list if o.objfn.datetime.month == month],
                                         key=lambda o: o.uid)

        elif rule.type == 'year':
            years = sorted(list(set([o.objfn.datetime.year for o in cur_rule_list])))
            for year in years:
                d_period[year] = sorted([o for o in cur_rule_list if o.objfn.datetime.year == year],
                                        key=lambda o: o.uid)

        # applying the policy, regarding Rule's policy
        rule_policy = rule.get_policy()
        to_keep[rule.type] = []
        if rule_policy == 'first':
            for period in d_period.keys():  # make the list of all the first
                to_keep[rule.type] += [d_period[period][0].uid]
        elif rule_policy == 'last':
            for period in d_period.keys():  # make the list of all the last
                to_keep[rule.type] += [d_period[period][-1].uid]
        elif rule_policy == 'first_last':
            for period in d_period.keys():  # make the list of all the last
                to_keep[rule.type] += [d_period[period][0].uid] + [d_period[period][-1].uid]
        else:  # mean 'all_items'
            for period in d_period.keys():  # make the list of all
                to_keep[rule.type] += [o.uid for o in d_period[period]]

        print_report_header("File to keep for " + rule.type)
        printlist(to_keep[rule.type], True)
    # end of for Rule in s_o_r:

    # then making to set of uids : 2keep, 2del.
    # 2 keep = union(set in the dict)
    # 2 del = olist - 2 keep
    set_2keep = set([uid for uid in flatten(to_keep.values())])  # return the union of all the sets of all the rules
    set_2del = set([o.objfn.uid for o in olist]) - set_2keep
    return (set_2keep, set_2del)


def flatten(items):
    """Yield items from any nested iterable; see Reference."""
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            for sub_x in flatten(x):
                yield sub_x
        else:
            yield x


def print_report_header(message, length=40):
    print('*' * length)
    print(message)
    print('*' * length)


def parseargs(args):
    parser = argparse.ArgumentParser(
        description="Apply some rules (default or stored) to manage rolling backup file on days,weeks,monthes ans years",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-n', '--invariant', help="Name of the file backuped inside all current backup file's name",
                        nargs=1)
    parser.add_argument('-b', '--files_path', help="Path to directory containing the files backuped", nargs=1)
    parser.add_argument('-r', '--config_rules', help="Name of the file containing rules", type=argparse.FileType('r'),
                        nargs=1)
    parser.add_argument('-d', '--defaultrules', help="Display default rules", action='store_true')
    parser.add_argument('-t', '--dryrun', help="Dry run : test rules and display result on console",
                        action='store_true')
    parser.add_argument('-a', '--archdir', help="set archive path for non deletion option", nargs=1,
                        default='./archived')
    parser.add_argument('-l', '--logfile', help="log file",
                        default=sys.stdout, type=argparse.FileType('w'), nargs=1)
    parser.add_argument('-v', '--verbose', help="log file", action='store_true')
    parser.add_argument('-s', '--silent', help="do not ask validation for actions", action='store_false')
    parser.add_argument('--yes', help="Accept all action (not recommended)", action='store_false')
    parser.add_argument('-f', '--file_list', help="file name of the file containing files name to treat")
    args = parser.parse_args(args)
    args.config_rules = args.config_rules[0]
    args.files_path = args.files_path[0]
    args.invariant = args.invariant[0]
    args.logfile = args.logfile[0]
    return args


def do_action(parsed_args, func, *args):
    arg_name = func.__name__
    act_past_name = arg_name + 'ed' if arg_name[-1] != 'e' else arg_name + 'd'
    act_prog_name = arg_name + 'ing' if arg_name[-1] != 'e' else arg_name[0:-1] + 'ing'
    if parsed_args.dryrun:
        print("DryRun : file to be {} : {}".format(act_past_name, arg_name))
    else:
        remove = input("{} file '{}' ? : ".format(act_prog_name, arg_name)) in (
            'o', 'O', 'y', 'Y') if not parsed_args.yes else parsed_args.yes
        if remove:
            if parsed_args.verbose:
                verbose("{} file : ", arg_name)
            func(*args)


def main(arguments):
    parsed_args = parseargs(arguments)
    # mean if set to slient mode, never print verbose / just print logs
    debug = parsed_args.verbose and not parsed_args.silent
    ################################
    # Check config / parameters file
    if os.path.isdir(os.path.realpath(parsed_args.files_path)):
        wrkdir = os.path.realpath(parsed_args.files_path)
    else:
        write_defaultconfig()
        raise ValueError(parsed_args.files_path + " does'nt exist.\nDefault example config file have been written")
    if parsed_args.config_rules != None and os.path.exists(os.path.realpath(parsed_args.config_rules.name)):
        SetOfRules = read_config(os.path.realpath(parsed_args.config_rules.name), setOfRules='MyRules')
    else:
        raise ValueError('config_Rules File does not exist')

    # get archive directory if required
    archdir = os.path.realpath(
        SetOfRules.dir_to_archive_files if SetOfRules.dir_to_archive_files else parsed_args.archdir)
    if not os.path.exists(os.path.realpath(archdir)):
        try:
            do_action(parsed_args, os.mkdir, archdir)
        except:
            raise ValueError("archive dir : " + archdir + " cannot find or create dir")

    # set the pattern of the file name to check-out
    basename = parsed_args.invariant
    verbose(("parsed_args namespace=", parsed_args), True, debug)
    verbose(('working dir=', wrkdir, "\narchive dir=", archdir), True, debug)
    verbose(('basename=', basename), True, debug)

    # for testing purpose ...
    if parsed_args.file_list:
        try:
            with open(parsed_args.file_list, 'r') as file_2_list:
                flist = [line.strip().split('/')[-1] for line in file_2_list]
                # read all the lines, skip \n at end of line, and take only last part of the path
        except:
            raise ValueError("File list of files name : " + parsed_args.file_list + " cannot be found")
    else:
        flist = generate_test_list(basename, nbdays=1000, dayh=[2, 10, 13, 16])
    test_create_file_list(flist, wrkdir)

    ##################################
    # Get full list of file to check
    flist_from_dir = get_file_list(wrkdir)
    if len(flist_from_dir) == 0:
        verbose(wrkdir + " is empty -> Nothing to do !", False, debug)
        exit(0)
    else:
        verbose(flist_from_dir, True, debug)
    # filtering files corresponding to given file pattern
    flist_basename = [e for e in flist_from_dir if not (re.search(basename, e) == None)]
    verbose(flist_basename, True, debug)
    # generate list of file's object for each filered filename
    tobj = collections.namedtuple('tupleobj', 'uid objfn')
    # then making a sorted list of tuple (uid, ojb) to be manipulated easily
    olist = [tobj(int(o.uid), o) for o in [Obj_Fname(fn, basename) for fn in flist_basename]]
    olist = sorted(olist, key=lambda tuplefn: tuplefn.uid, reverse=True)
    verbose(olist, True, debug)

    # 1) check all the rules to apply and calculate the stat and period of time of the Rule ..
    # i.e. for daily Rule, if keep = 10, means that first date is le proxies one and last date will be 10 days before
    # doing the same for every rules
    compute_start_end_dates(SetOfRules, olist[0].objfn)
    # applying to Set_of_Rules : returning a tuple ao set of uid
    uids2keep, uids2del = set_2keep_2del(olist, SetOfRules)

    print_report_header("IUDS  to keep")
    printlist(uids2keep, True)
    print_report_header("IUDS  to del or archive")
    printlist(uids2del, True)
    logging.info("keeped list of {0} file(s) : {1}".format(len(uids2keep), ', '.join([str(uid) for uid in uids2keep])))

    # doing last job : moving the files
    if not parsed_args.dryrun:
        failed = done = []
        d_olist = dict(olist)
        for uid in uids2keep:
            print(os.system('lsattr '+ os.path.join(wrkdir, d_olist[uid].filename)))
            os.system('sudo chattr +u ' + os.path.join(wrkdir, d_olist[uid].filename))
        for uid in uids2del:
            _filename = os.path.join(wrkdir, d_olist[uid].filename)
            if SetOfRules.delete_files:
                try:
                    do_action(parsed_args, os.remove, _filename, _filename, 'remove', 'removed', 'removing')
                except PermissionError:
                    failed.append(fileobj)
                    raise ErrorValue("permission denied")
                else:
                    done.append(_filename)
            elif SetOfRules.dir_to_archive_files:
                try:
                    do_action(parsed_args, shutil.move,
                              os.path.join(os.getcwd(), _filename),
                              os.path.join(os.getcwd(),
                                           SetOfRules.dir_to_archive_files,
                                           os.path.basename(_filename)
                                           )
                except PermissionError:
                    failed.append(_filename)
                    return "permission denied"
                else:
                    done.append(_filename)

        print_report_header("Job finished ")
        printlist(done)
        if len(failed) > 0:
            logging.warning(failed)
            print_report_header("failed")
            printlist(failed)


if __name__ != '__main__':
    sys.exit(main(
        "-r ./check_bckp_file.conf -l log.log -n E12_LABSED2019-04-prod.zip -b ./test_dir -f   lsdirlistbkp.txt".split()))
    # sys.exit(main("-t -r ./check_bckp_file.conf -l log.log -n E12_LABSED2019-04-prod.zip -b ./test_dir".split()))
else:
    args = " ".join(sys.argv[1:]).split()
    sys.exit(main(args if args != [] else ['-h']))
    # sys.exit(main(" ".join(sys.argv[1:]).split()))

