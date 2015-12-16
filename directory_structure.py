#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 26 15:44:33 2015

@author: Niuzexiong

"""

import os

directory = {'00.rawdata_folder'    :{'sampleN_folder':None,
                                      'sampleT_folder':None},
             '01.cleandata_folder'  :{'sampleN_folder':None,
                                      'sampleT_folder':None},
             '02.aln_folder'        :{'sampleN_folder':None,
                                      'sampleT_folder':None},
             '03.var_folder'        :{'sampleN_folder':{'snv_folder'  :None,
                                                        'indel_folder':None,
                                                        'sv_folder'   :None,
                                                        'cnv_folder'  :None},
                                      'sampleT_folder':{'snv_folder'  :None,
                                                        'indel_folder':None,
                                                        'sv_folder'   :None,
                                                        'cnv_folder'  :None}},
             '04.somaticvar_folder' :{'sampleT_folder':{'snv_folder'  :None,
                                                        'indel_folder':None,
                                                        'sv_folder'   :None,
                                                        'cnv_folder'  :None,
                                                        'msi_folder'  :None}},
             'log_folder'           :{'shell_files':None}}


def check_subkey(adict,folders,maindir):
    for k in adict:
        if type(adict[k]) == dict:
            check_subkey(adict[k],folders,os.path.join(maindir,k.replace('_folder','')))
        elif k.endswith('_folder'):
            folders[os.path.join(maindir,k.replace('_folder',''))] = True
        else:
            pass

def grade_path(apath,path_sets):
    parentdir,subdir = os.path.split(apath)
    if parentdir not in path_sets:
        path_sets.append(parentdir)
        grade_path(parentdir,path_sets)
    else:
        pass

def projdirectory(proj_dir,directory_structure):
    checked_folders = [proj_dir]
    folders = {}
    check_subkey(directory_structure,folders,proj_dir)
    pathes = {'log':os.path.join(proj_dir,'log')}
    for k in folders:
        if folders[k]:
            checked_folders.append(k)

    for folder_need in sorted(checked_folders):
        if 'sample' not in folder_need:
            continue
        for aprocess in ['cleandata','rawdata','aln','sv','cnv','snv','indel','msi']:
            if aprocess in folder_need:
                if 'somatic' in folder_need:
                    pathes['somatic'+aprocess] = folder_need
                else:
                    pathes[aprocess] = folder_need

    return pathes

if __name__ == "__main__":
    p = projdirectory('d',directory)
    for k in p:
        print '\t'.join([k,p[k]])
