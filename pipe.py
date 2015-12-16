#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 28 09:11:47 2015

@author: Niuzexiong

fresh info:

"""

import argparse
import os
import yaml
import sys
from get_inputfiles import parseconfig,filesNeedtoDelorAdd
from directory_structure import projdirectory,directory
from create_directories import creatDir,final_path,creatDirect
from software_path import *
from actions import instance_jobs,instance_action_inputs

def mkdirs(apath):
    if os.path.exists(apath):
        pass
    else:
        basepath,dirname = os.path.split(apath)
        if os.path.exists(basepath):
            pass
        else:
            mkdirs(basepath)
        os.mkdir(apath)

def update_dict(dict1,dict2):
    '''
      add dict2 to dict1, the same key will use values in dict2
      when velues are not dict.if values are dict,just fresh keys in next level, not delete keys.
    '''

    for akey in dict2:
        if akey not in dict1:
            dict1[akey] = dict2[akey]
        else:
            if type(dict2[akey]) == dict and type(dict1[akey]) == dict:
                update_dict(dict1[akey],dict2[akey])
            else:
                dict1[akey] = dict2[akey]

    return dict1

def check_flow(jobs):
    files = {}
    for ajob in jobs:
        for afile in jobs[ajob]['input']:
            if afile not in files:
                files[afile] = [[ajob],[]]
            else:
                files[afile][0].append(ajob)
        for afile in jobs[ajob]['output']:
            if afile not in files:
                files[afile] = [[],[ajob]]
            else:
                files[afile][1].append(ajob)
    for afile in files:
        if len(files[afile][1]) == 0:
            sys.exit('BAD flow: there is no {afile} input for {JOBs}'.format(afile=afile,JOBs=','.join(files[afile][0])))
    return 'Good flow! Congratulations :)'


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=''' ''')
    parser.add_argument('--jobcodes',help="JOB codes you want to execute",default="")
    parser.add_argument('--config',help="config file with seq data info")
    parser.add_argument('--yml',help="New jobs you can define in it",default="")
    parser.add_argument('--projdir',help="work dir for the project")
    parser.add_argument('--somatic',type=bool,default=True)
    parser.add_argument('--panel',type=str)

    args = parser.parse_args()

    jobcodes = args.jobcodes.split(',')
    config_file = args.config
    yml_file = args.yml
    projdir = args.projdir
    somatic = args.somatic
    panel = args.panel

    mkdirs(projdir)

    #panel
    if panel == 'panel88':
        panelBed = panel88_bed
    elif panel == 'panel203':
        panelBed = panel203_bed
    elif panel == 'panel154':
        panelBed = panel154_bed
    elif panel == 'exome':
        panelBed = agilent_exome_bed



    default_yml = ""
    ##parsing yaml files,get all jobs were defined
    default_ymlh = open(default_yml,'r')
    default_jobs = yaml.load(default_ymlh)

    new_jobs = yaml.load(open(yml_file,'r'))

    all_jobs = update_dict(default_jobs,new_jobs)

    ##check the probability that jobs in jobcodes constructure a workflow
    wanted_jobs = {}
    for ajobcode in jobcodes:
        if not somatic:
            if 'somatic' in ajobcode:
                continue
        if ajobcode not in all_jobs:
            print "{acode} not defined in yaml files".format(acode=ajobcode)
            sys.exit(1)
        else:
            wanted_jobs[ajobcode] = all_jobs[ajobcode]
    if somatic:
        for ajob in all_jobs:
            if 'somatic' in ajob:
                wanted_jobs[ajob] = all_jobs[ajob]

    print check_flow(wanted_jobs)

    ##parse config file
    datas,samples,filesneedtoAdd = parseconfig(config_file)

    ##infiles creat
    infiles = {}
    for aperson in datas:
        for TorN in datas[aperson]:
            for asample in datas[aperson][TorN]:
                for alib in datas[aperson][TorN][asample]:
                    for lane in datas[aperson][TorN][asample][alib]:
                        if datas[aperson][TorN][asample][alib][lane]['rawdata'] != '-':
                            if '.raw.fq.gz' not in infiles:
                                infiles['.raw.fq.gz'] = [datas[aperson][TorN][asample][alib][lane]['rawdata'].split(',')]
                            else:
                                infiles['.raw.fq.gz'].append(datas[aperson][TorN][asample][alib][lane]['rawdata'].split(','))
                        elif datas[aperson][TorN][asample][alib][lane]['cleandata'] != '-':
                            if '.clean.fq.gz' not in infiles:
                                infiles['.clean.fq.gz'] = [datas[aperson][TorN][asample][alib][lane]['cleandata'].split(',')]
                            else:
                                infiles['.clean.fq.gz'].append(datas[aperson][TorN][asample][alib][lane]['cleandata'].split(','))

    ##make dirs, and check the completeness of result files that were exsited
    p = projdirectory(projdir,directory)
    # for k in p:
    #     print '\t'.join([k,p[k]])
    # print '#################################################################'
    processes = set([wanted_jobs[ajob]['outputdir'] for ajob in wanted_jobs])
    pp = final_path(p,processes)
    # for k in pp:
    #     print '\t'.join([k,pp[k]])
    # print '#################################################################'
    dirs = creatDirect(pp,samples)
    # for k in dirs:
    #     for m in dirs[k]:
    #         print '\t'.join([k,m,dirs[k][m]])

    #寻找需要更新的bam文件和需要添加到datas中的bam文件，如以前下机的T，现在下机了N，可以做后面的somatic检测了，那就需要添加
    #需要更新的bam文件是指加测新lane
    filesneedtoDel = filesNeedtoDelorAdd(datas,samples,os.path.dirname(p['aln']),need_add_datas=filesneedtoAdd,somatic=somatic)

    ##instance jobs for every person
    instance_actions = {}
    for aperson in datas:
        instance_jobs(aperson,wanted_jobs,instance_actions=instance_actions,inputfiles=infiles)
        instance_action_inputs(aperson,instance_actions=instance_actions,inputfiiles=infiles)




