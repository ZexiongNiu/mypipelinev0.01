#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import subprocess
import pysam


def check_fileIsComplete(afile):
    if afile.endswith('.bam'):
        if os.path.isfile(afile+'.bai'):
            return True
    elif afile.endswith('.vcf'):
        if os.path.isfile(afile+'.idx'):
            return True
    else:
        return True

def check_fileIsComplete1(afile):
    if os.path.isfile(afile+'.done'):
        return True
    else:
        return False

def TraverseDir(indir,suffix):
    alltargets = []
    for dirpath,dirnames,filenames in os.walk(indir):
        for afile in filenames:
            if afile.endswith(suffix):
                fullpath = os.path.join(dirpath,afile)
                alltargets.append(fullpath)
    return alltargets

def parseconfig(cfgfile):
    datas,samples,filesneedtoadd = {},{},{}
    with open(cfgfile,'r') as cfg:
        for eachline in cfg:
            if eachline.startswith('#'):
                continue
            items = eachline.strip().split('\t')
            person,sample,TorN,library,lane,rawdata,cleandata,bam = items[:-1]

            if lane == '.':
                if person not in filesneedtoadd:
                    filesneedtoadd[person] = {TorN:{sample:bam}}
                elif TorN not in filesneedtoadd[person]:
                    filesneedtoadd[person][TorN] = {sample:bam}
                else:
                    filesneedtoadd[person][TorN][sample] = bam
                continue

            if person not in datas:
                datas[person] = {'T':{},'N':{}}
                datas[person][TorN][sample] = {library:{lane:{'rawdata'   :rawdata,
                                                              'cleandata' :cleandata,
                                                              'bam'       :bam}}}
            elif sample not in datas[person][TorN]:
                datas[person][TorN][sample] = {library:{lane:{'rawdata'   :rawdata,
                                                              'cleandata' :cleandata,
                                                              'bam'       :bam}}}
            elif library not in datas[person][TorN][sample]:
                datas[person][TorN][sample][library] = {lane:{'rawdata'   :rawdata,
                                                              'cleandata' :cleandata,
                                                              'bam'       :bam}}
            elif lane not in datas[person][TorN][sample][library]:
                datas[person][TorN][sample][library][lane] = {'rawdata'   :rawdata,
                                                              'cleandata' :cleandata,
                                                              'bam'       :bam}
            else:
                if rawdata != '.':
                    datas[person][TorN][sample][library][lane]['rawdata'] = rawdata
                if cleandata != '.':
                    datas[person][TorN][sample][library][lane]['cleandata'] = cleandata
                if bam != '.':
                    datas[person][TorN][sample][library][lane]['bam'] = bam

            if sample not in samples:
                samples[sample] = TorN
    return datas,samples,filesneedtoadd

def rgidneed(datas):
    idneed = {}
    for aperson in datas:
        for TorN in datas[aperson]:
            for asample in datas[aperson][TorN]:
                if asample not in idneed:
                    idneed[asample] = []
                for alibrary in datas[aperson][TorN][asample]:
                    for alane in datas[aperson][TorN][asample][alibrary]:
                        if alane != '.':
                            idneed[asample].append('_'.join([asample,alane]))
    return idneed

#查找config涉及person的bam文件，添加到datas和filesneedtoadd
def TraverseDir(indir,suffix):
    alltargets = []
    for dirpath,dirnames,filenames in os.walk(indir):
        for afile in filenames:
            if afile.endswith(suffix):
                fullpath = os.path.join(dirpath,afile)
                alltargets.append(fullpath)
    return alltargets

def fileClassBysuffix(files,suffixes):
    classed = {}
    for afile in files:
        for asuffix in suffixes:
            if afile.endswith(asuffix):
                if asuffix not in classed:
                    classed[asuffix] = [afile]
                else:
                    classed[asuffix].append(afile)
                break
    return classed

def rg_idInbam(bamfile):
    rg = []
    samfile = pysam.AlignmentFile(bamfile,'rb')
    for aRG in samfile.header['RG']:
        rg.append(aRG['ID'])

    return rg


def isfile(afile):
    if os.path.isfile(afile):
        return True
    else:
        return False

if __name__ == "__main__":
    datas,samples,filesneedtoadd = parseconfig('config')
    print "datas========================================================================================================"
    for aperson in datas:
        for TorN in datas[aperson]:
            for asample in datas[aperson][TorN]:
                for alibrary in datas[aperson][TorN][asample]:
                    for alane in datas[aperson][TorN][asample][alibrary]:
                        for akey in datas[aperson][TorN][asample][alibrary][alane]:
                            print '\t'.join([aperson,TorN,asample,alibrary,alane,akey,datas[aperson][TorN][asample][alibrary][alane][akey]])
    print "samples======================================================================================================"
    for asample in samples:
        print '\t'.join([asample,samples[asample]])
    print "filesneedtoadd==============================================================================================="
    for aperson in filesneedtoadd:
        for TorN in filesneedtoadd[aperson]:
            for asample in filesneedtoadd[aperson][TorN]:
                print '\t'.join([aperson,TorN,asample,filesneedtoadd[aperson][TorN][asample]])

def TraverseDir(indir,suffix):
    alltargets = []
    for dirpath,dirnames,filenames in os.walk(indir):
        for afile in filenames:
            if afile.endswith(suffix):
                fullpath = os.path.join(dirpath,afile)
                alltargets.append(fullpath)
    return alltargets

def del_file(afile):
    if os.path.isfile(afile):
        os.remove(afile)

def check_lanes_inbam(abamfile,lanes):
    samfile = pysam.AlignmentFile(mergebam,'rb')
    alllanesinbam = []
    for aRG in samfile.header['RG']:
        alllanesinbam.append(aRG['ID'])
    for k in lanes:
        if k not in alllanesinbam:
            return True

    return False

def filesNeedtoDelorAdd(datas,samples,alndir,need_add_datas={},somatic=True):
    #删除后续的bam文件，并把需要添加的datas里面的bam文件添加进去
    dirorfiles = os.listdir(alndir)
    files_needdel = []
    for aperson in datas:
        dirs = {}
        for aitem in dirorfiles:
            if os.path.isdir(aitem) and aitem.startswith(aperson+'.'):
                person,tissuetype,sample = aitem.split('.')
                if aperson not in dirs:
                    dirs[tissuetype] = {sample:os.path.join(alndir,aitem)}
                else:
                    dirs[tissuetype][sample] = os.path.join(alndir,aitem)
        if len(dirs) == 0:
            break
        for atissuetype in datas[aperson]:
            for asample in datas[aperson][atissuetype]:
                alllanes = []
                for alib in datas[aperson][atissuetype][asample]:
                    for lane in datas[aperson][atissuetype][asample]:
                        alllanes.append('_'.join([alib,lane]))

                #asample 存在于dirs中
                need_delete = False
                if atissuetype in dirs and asample in dirs[atissuetype]:
                    adir = dirs[atissuetype][asample]
                    mergebam = os.path.join(adir,asample+'.merge.bam')
                    if os.path.isfile(mergebam):
                        need_delete = check_lanes_inbam(mergebam,alllanes)

                if not need_delete:
                    #判断这个文件是否完整的
                    if check_fileIsComplete1(mergebam):
                        pass
                    else:
                        files_needdel.append(mergebam)
                else:
                    for asuffix in ['merge.bam','rmdup.bam','realn.bam','recal.bam']:
                        for afile in TraverseDir(adir,asuffix):
                            files_needdel.append(afile)
                    #所有跟这个样本相关的vcf文件也需要删除

                #asample 不存在于dirs中
                if somatic:
                    if atissuetype == 'N':
                        for k in dirs['T']:
                            if k not in datas[aperson][atissuetype]:
                                #添加bam文件到datas中，判断是哪种bam文件存在
                                kdir = os.path.join(alndir,'.'.join([aperson,'T',k]))
                                for asuffix in ['recal.bam','realn.bam','rmdup.bam','merge.bam']:
                                    if len(TraverseDir(kdir,asuffix)) != 0:
                                        targetfile = TraverseDir(kdir,asuffix)[0]
                                        if aperson not in need_add_datas:
                                            need_add_datas[aperson] = {'T':{k:targetfile}}
                                        elif 'T' not in need_add_datas[aperson]:
                                            need_add_datas[aperson]['T'] = {k:targetfile}
                                        else:
                                            need_add_datas[aperson]['T'][k] = targetfile
                    else:
                        if len(dirs['N']) != 0 and len(datas[aperson]['N']) == 0:
                            #添加bam文件到datas中，判断是哪种bam文件存在
                            kdir = os.path.join(alndir,'.'.join([aperson,'N',k]))
                            for asuffix in ['recal.bam','realn.bam','rmdup.bam','merge.bam']:
                                if len(TraverseDir(kdir,asuffix)) != 0:
                                    targetfile = TraverseDir(kdir,asuffix)[0]
                                    if aperson not in need_add_datas:
                                        need_add_datas[aperson] = {'N':{k:targetfile}}
                                    elif 'N' not in need_add_datas[aperson]:
                                        need_add_datas[aperson]['N'] = {k:targetfile}
                                    else:
                                        need_add_datas[aperson]['N'][k] = targetfile
    return files_needdel














