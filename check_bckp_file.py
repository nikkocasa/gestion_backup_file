#!/usr/bin/env python
# -*- coding: utf-8 -*-

# from __future__ import print_function
import os, re
import sys
import argparse
from datetime import date, time, datetime, timedelta
import collections
import configparser

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

class obj_fname(object):
    def __init__(self, fname=False, basename=False):
        self.string = fname
        self.datetime = datetime(1900,1,1,0,0,0)
        self.date = date(1900,1,1)
        self.date_str = False
        self.time_str = False
        self.hour = time(0,0,0)
        self.y, self.m, self.d = 0,0,0
        self.h, self.mm, self.s = 0,0,0
        self.dbname = basename
        self.dayly_keep = False
        self.weekly_keep = False
        self.monthly_keep = False
        self.yearly_keep = False
        self.uid = False
        if fname and basename:
            self.set_extract_values(fname, basename)

    def set_extract_values(self, str=False, dbn=False):
        str_fname = self.string if not str else str
        str_basename = self.dbname if not dbn else dbn
        if str_fname and str_basename and not (re.search(str_basename, str_fname)==None):
            datetime_list = re.findall('\d+',re.sub(str_basename, '', str_fname))
            self.y, self.m, self.d, self.h, self.mm, self.s = [int(x) for x in datetime_list]
            self.date = date(self.y, self.m, self.d)
            self.time = time(self.h, self.mm, self.s)
            self.datetime = datetime(self.y, self.m, self.d, self.h, self.mm, self.s)
            self.date_str = ''.join(datetime_list[0:3])
            self.time_str = ''.join(datetime_list[3:])
            self.uid = self.date_str + self.time_str

class rule(object):
    def __init__(self, _name=False, _type=False, _number=False, _params=False):
        self.name = _name
        self.type = self.set_type(_type)
        self.keep = self.set_number(_number)
        self.keep_min = self.set_number(_number)
        self.keep_max = self.set_number(_number)
        self.policy = self.set_params(_params)

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
        if params and params not in ['last', 'first', 'first_last', 'all']:
            raise ValueError("erreur de params \n must be in : ['last', 'first', 'first_last', 'all']")
        else:
            return params

class set_of_rules(object):
    day_define = False
    week_define = False
    month_define = False
    year_define = False
    align_calendar = True
    ending_deleting_files = False
    dir_to_archive_files = False

    def __init__(self, name=False, _day=None, _week=None, _month=None, _year=None):
        self.setname = name
        self.day = None # self.set_rule('day', _day)
        self.week = None # self.set_rule('week', _week)
        self.month = None # self.set_rule('month', _month)
        self.year = None # self.set_rule('year', _year)

    def set_rule(self, _type, _rule):
        if set_of_rules.year_define == False:
            if not isinstance(_rule, rule):
                raise TypeError("An instancied object of class 'rule' need to be passed")
            elif _type not in ['day', 'week', 'month', 'year']:
                raise ValueError("erreur de type de regle \n must be in ['day', 'week', 'month', 'year']")
            else:
                if _type == 'day':
                    self.day = _rule
                    set_of_rules.day_define = True
                elif _type == 'week':
                    self.week = _rule
                    set_of_rules.week_define = True
                elif _type == 'month':
                    self.month = _rule
                    set_of_rules.month_define = True
                elif _type == 'year':
                    self.year = _rule
                    set_of_rules.year_define = True
        else:
            try:
                raise PermissionError("Rule allreday exisitng .. try modify_rule()")
            except PermissionError:
                print("Exception Object allreday set")
                raise

    def get_rule(self, _type, _param):
        pass

    def modify_rule(self, type, _param):
        pass

    def delete_rule(self, type):
        pass

def get_file_list(path=False):
    if path:
        os.chdir(path)
    flist = os.listdir()
    return flist


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
ending_Deleting_files = False
dir_to_archive_files = ./archived
     
[dayly]
keep = 7
policy = all

[weekly] 
keep = 4
policy = last

[monthly]
keep_max = 11
keep_min = 6
policy = last

[yearly]
keep = 10
policy = last
"""
    parser.read_string(defaultConfig)
    return  parser

def read_config(filename='check_bckp_file.conf', setOfRules='Rules'):
    config = configparser.ConfigParser()
    config.read(filename)
    s_of_r = set_of_rules(setOfRules)
    type = {'params':'param', 'dayly': 'day', 'weekly': 'week', 'monthly': 'month', 'yearly': 'year'}
    for section in config.sections():
        if section == 'params':
            for option in config[section]:
                setattr(s_of_r, option, config.get(section, option))
                print(option + ':', s_of_r.__getattribute__(option))
        else:
            print()
            cur_rule = rule(_name=section, _type = type[section])
            for option in config[section]:
                setattr(cur_rule, option, config.get(section, option))
                print('rule:', cur_rule.name, 'option:', option + ':', cur_rule.__getattribute__(option))
            s_of_r.set_rule(cur_rule.type, cur_rule)
    print('defined rules:', s_of_r.setname, '\n day :', s_of_r.day_define, \
                            '\n week :', s_of_r.week_define, \
                            '\n month :', s_of_r.month_define, \
                            '\n year :', s_of_r.year_define)


def write_defaultconfig(filename='check_bckp_file.conf'):
    config = get_default_conf()
    with open(filename, 'w') as configfile:
        config.write(configfile)


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
            # ltp = strf([cd.year, cd.month, cd.day, cd.hour, cd.minute, cd.second])
            fname = "_".join(strf([cd.year, cd.month, cd.day, cd.hour, cd.minute, cd.second]) + [nombase + ".zip"])
            flist.append(fname)
    return flist



def main(arguments):

    parser = argparse.ArgumentParser(
        description="Apply some rules (default or stored) to manage rolling backup file on days,weeks,monthes ans years",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('Invariant', help="Name of the file bakuped inside all current bachup file's name")
    parser.add_argument('-r', '--rulesFile', help="Name of the file containing rules", type=argparse.FileType('r'))
    parser.add_argument('-d', '--defaultrules', help="Display default rules")
    parser.add_argument('-l', '--logfile', help="log file",
                        default=sys.stdout, type=argparse.FileType('w'))

    args = parser.parse_args(arguments)

    print(args)
    basename = args.Invariant
    print(basename)
    # print(get_file_list())
    # flist = generate_test_list(basename, nbdays=500,dayh=[2,10,13, 16])
    # printlist(flist, enum=True)
    # flist_basename = [e for e in flist if not (re.search(basename, e)==None)]
    # # print(flist_basename)
    # tobj = collections.namedtuple('tupleobj','uid objfn')
    # olist = [tobj(o.uid, o) for o in [obj_fname(fn, basename) for fn in flist_basename]]
    # olist = sorted(olist, key=lambda tuplefn: tuplefn.uid, reverse=True)
    # printlist(olist, enum=True)
    write_defaultconfig()
    read_config(setOfRules='My_RULES')


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

