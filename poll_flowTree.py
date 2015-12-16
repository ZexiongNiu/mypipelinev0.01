#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import time

def collectfiles(jobs):
    files = {}
    for ajob in jobs:
        inputs = jobs[ajob]['inputs']
        outputs = jobs[ajob]['outputs']
        for ainput in inputs:
            if ainput not in files:
                files[ainput] = {'inputfor':[ajob]}

            elif 'inputfor' not in files[ainput]:
                files[ainput]['inputfor'] = [ajob]
            else:
                files[ainput]['inputfor'].append(ajob)
        for aoutput in outputs:
            if aoutput not in files:
                files[aoutput] = {'outputfor':[ajob]}

            elif 'outputfor' not in files[aoutput]:
                files[aoutput]['outputfor'] = [ajob]
            else:
                files[aoutput]['outputfor'].append(ajob)
    return files

infinite = 3000000000
def modifiedTime(afile):

    if os.path.isfile(afile):
        modifiedtime = infinite
    else:
        modifiedtime = os.path.getmtime(afile)
    return modifiedtime

def fileTime(files):
    filetime = {}
    for afile in files:
        filetime[afile] = modifiedTime(afile)
    return filetime


def job_depend(jobs):
    depend = {}
    for ajob in jobs:
        depend[ajob] = {}
        output = jobs[ajob]['outputs']
        for bjob in jobs:
            if bjob == ajob:
                continue
            binput = jobs[bjob]['inputs']
            for k in output:
                if k in binput:
                    if jobs[bjob]['pe'] or jobs[bjob]['pairs']:
                        depend[ajob][bjob] = 1
                    else:
                        depend[ajob][bjob] = 1/len(binput)
    return depend

def compareTime(timesets1,timesets2):
    timesets1 = sorted(timesets1)
    timesets2 = sorted(timesets2)
    alltimes = sorted(timesets1+timesets2)
    if alltimes.index(timesets1[-1]) != len(timesets1)[-1]:
        return False
    else:
        return True

def pollTree(files,jobs):
    jobdel,filedel = [],[]
    for ajob in jobs:
        inputs = jobs[ajob]['inputs']
        outputs = jobs[ajob]['outputs']

        inputtimes,outputtimes = [],[]
        for ainput in inputs:
            inputtimes.append(modifiedTime(ainput))
        for aoutput in outputs:
            outputtimes.append(modifiedTime(aoutput))
        reasonable = compareTime(inputtimes,outputtimes)
        if not reasonable:
            for aoutput in outputs:
                filedel.append(aoutput)
        else:
            if modifiedTime(outputs[0]) != infinite:
                allcomplete = True
                for afile in outputs:

                    #如果文件都是完整的，删掉ajob，如果不完整，删掉文件，保留ajob
                    if not os.path.isfile(afile+'.done'):
                        allcomplete = False
                        break
                        
                if allcomplete:
                    jobdel.append(ajob)
                else:
                    for aoutput in outputs:
                        filedel.append(aoutput)
            else:
                pass
    for ajob in jobdel:
        del jobs[ajob]