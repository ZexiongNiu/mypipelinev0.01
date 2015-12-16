#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import re
import directory_structure

def creatDir(adir):
    if os.path.exists(adir):
        pass
    else:
        parentpath,subpath = os.path.split(adir)
        if os.path.exists(parentpath):
            os.mkdir(adir)
        else:
            creatDir(parentpath)

def final_path(path_sets,process):
    finalpathes = {}
    for p in path_sets:
        if p in process:
            finalpathes[p] = path_sets[p]
    return finalpathes

def creatDirect(pathes,samples):
    dirs = {}
    p = re.compile(r'sample[TN]')
    for aprocess in pathes:
        for aperson in samples:
            for atype in samples[aperson]:
                for asample in samples[aperson][atype]:
                    dirname = '.'.join([aperson,atype,asample])
                    if dirname not in dirs:
                        dirs[dirname] = {}

                    if 'somatic' not in aprocess:
                        dirs[dirname][aprocess] = re.sub(p,dirname,pathes[aprocess])
                    else:
                        if atype == 'T':
                            dirs[dirname][aprocess] = re.sub(p,dirname,pathes[aprocess])

    return dirs

def excuteCreat(projdir,processes,samples):
    structure_model = directory_structure.projdirectory(projdir,directory_structure.directory)
    filtered_modelbyProcesses = final_path(structure_model,processes)
    real_structure = creatDirect(filtered_modelbyProcesses,samples)
    for adirname in real_structure:
        for aprocess in real_structure[adirname]:
            creatDir(real_structure[adirname][aprocess])
            print '\t'.join([adirname,aprocess,real_structure[adirname][aprocess]])

def write_shell(shell_file,cmds):
    with open(shell_file,'w') as sh:
        sh.write('\n'.join(cmds)+'\n')

if __name__ == "__main__":
    p = directory_structure.projdirectory('d',directory_structure.directory)
    for k in p:
        print '\t'.join([k,p[k]])
    print '#################################################################'
    pp = final_path(p,['rawdata','cleandata','aln','somaticsnv','snv','somaticsv','somaticmsi'])
    for k in pp:
        print '\t'.join([k,pp[k]])
    print '#################################################################'
    dirs = creatDirect(pp,{'PA172':{'T':{'PA172TQ1':{}},'N':{'PA172NB1':{}}},'PA173':{'N':{'PA173NB1':{}}}})
    for k in dirs:
        for m in dirs[k]:
            print '\t'.join([k,m,dirs[k][m]])
