#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
from get_inputfiles import TraverseDir

#dirs, rawdata, cleandata, aln, snv, indel, sv, cnv, somaticsnv, somaticindel, somaticsv, somaticcnv
jobs = {'lnrawdata':{'cmd'    : 'ln -s {input[0]} {output[0]};ln -s {input[1]} {output[1]}',
                     'sample' : None,
                     'lane'   : 'Y',
                     'pe'     : 'Y',
                     'pairs'  : None,
                     'inputdir': '',
                     'outputdir' : 'rawdata',

                     'output_template': ['{sample}_{lane}_R1.raw.fq.gz','{sample}_{lane}_R2.raw.fq.gz'],
                     'output_suffix'  : '.raw.fq.gz',
                     'input_suffix':['fq.gz'],
                     'memory' : '0.1G',
                     'cpus'   : '1',
                     'suffix' : None,
                     'priority_input': None},
        'trimmomatic':{'cmd'     : 'java -jar {TRIMMOMATIC} -thread 8 -trimlog {sample}_{lane}.log {input[0]} {input[1]} \
                                    {library}_{lane}_R1.clean.fq.gz {library}_{lane}_R2.clean.fq.gz {library}_{lane}_R1.unpaired.fq.gz {sample}_{lane}_R2.unpaired.fq.gz \
                                    ILLUMINACLIP:$ADAPTER:2:30:10 LEADING:3 TRAILING:3 \
                                    SLIDINGWINDOW:4:15 MINLEN:36',
                       'inputdir' : 'rawdata',
                       'outputdir': 'cleandata',
                       'output'  : ['{library}_{lane}_{end}.clean.fq.gz'],
                       'extra_output':['{library}_{lane}_R1.unpaired.fq.gz',
                                    '{sample}_{lane}_R2.unpaired.fq.gz',
                                    '{sample}_{lane}.log'],
                       'input_priority':['{library}_{lane}_{end}.raw.fq.gz','{library}_{lane}_{end}.fastq'],
                       'output_suffix':'clean.fq.gz',
                       'memory'   : '5G',
                       'cpus'     : '8'},
        'bwa'        :{'cmd'     : 'bwa mem -R "@RG\tID:{library}_{lane}\tSM:{sample}\tLB:{library}\tPU:{library}_{lane}\tPL:illumina\tCN:genetronhealth" \
        -t 8 -M {REF} {input1} {input2} > {outputdir}/{sample}_{library}_{lane}.bam',
                       'lane':'Y',
                       'pe':'Y',
                       'pairs':None,
                       'inputdir': 'cleandata',
                       'outputdir': 'aln',
                       'input_suffix':['clean.fq.gz'],
                       'output'  : ['{sample}_{library}_{lane}.bam'],
                       'output_suffix':'.bam',
                       'memory'  : '12G',
                       'cpus'    : '8'},
        'somaticsnv' :{'cmd'     : 'java -Xmx32G -Djava.io.tmpdir={outputdir} -jar {MUTECT} -T MuTect -rf BadCigar -dt NONE --reference_sequence {REF} --cosmic \
         {COSMIC} --dbsnp {DBSNP} --intervals {MutectBed} --input_file:normal {input1} --input_file:tumor {input2} --vcf {outputdir}/{sampleT}.{sampleN}.mutect.snv.vcf \
         --out {outputdir}/{sampleT}.{sampleN}.mutect.call_stats.out --coverage_file {outputdir}/{sampleT}.{sampleN}.mutect.coverage.wig.txt',
                       'lane':None,
                       'pe':None,
                       'pairs':'Y',
                       'inputdir': 'aln',
                       'outputdir': 'somaticsnv',
                       'input_suffix':['.rmdup.realn.recal.bam','.rmdup.bam'],
                       'output'  : ['{sampleT}.{sampleN}.mutect.snv.vcf'],
                       'output_suffix':'.mutect.snv.vcf',
                       'memory'  : '32G',
                       'cpus'    : '8'},
        'sort_bam'   :{'cmd'     :'',
                       'inputdir':'aln',
                       'outputdir':'aln',
                       'input'   :[]}}

files = {'raw.fq.gz':'{library}_{lane}_R1.fq.gz',
         'clean.fq.gz':'{library}_{lane}_R1.fq.gz',
         'bam':'{sample}.{library}_{lane}.bam',
         'sort.bam':'{sample}.{library}_{lane}.sort.bam',
         'merge.bam':'{sample}.merge.bam',
         ''}


def sampled_jobs():


class Action:
    '''

    '''

    def __init__(self,action_name,action_structure,inputdir,outputdir,person,sample,tissuetype,matchN,library=None,lane=None,pe=None,pairs=None,cpu=1,memory='1G',outputs=[],inputs_priority_queue=[]):
        self.cmds = action_structure
        self.inputdir = inputdir
        self.outputdir = outputdir
        self.cpus = cpu
        self.person = person
        self.sample = sample
        self.tissuetp = tissuetype
        self.matchN = matchN
        self.library = library
        self.lane = lane
        self.pe = pe
        self.pairs = pairs
        self.mem = memory
        self.output_template = outputs
        self.input_priority_queue = inputs_priority_queue
        self.inputs = []
        self.output = []
        self.action_name = action_name

    def excute_func(self):

        self._confirm_output()
        self.define_actionName()

    #get input files using input priority, from inputfiles set
    def _confirm_input(self,inputfiles={}):
        for atemplate in self.input_priority_queue:
            if atemplate in inputfiles:
                all_fix_files = inputfiles[asuffix]
                if self.lane:
                    if self.pe:
                        file1,file2 = atemplate.format(library=self.library,lane=self.lane,end='R1'),atemplate.format(library=self.library,lane=self.lane,end='R2')
                        real_file1 = self.check_file(all_fix_files,file1)
                        real_file2 = self.check_file(all_fix_files,file2)

                        if real_file1 and real_file2:
                            self.inputs = [real_file1,real_file2]
                            break

                    elif atemplate.endswith('sort.bam'):
                        #merge sort.bam怎么处理，前面都是单个文件输入，而merge是要以sample开头的所有sort.bam文件作为输入
                        real_files = self.check_file(all_fix_files,'',mode='startswith')
                        sortbamsindir = TraverseDir(os.path.join(self.inputdir,'.'.join([self.person,self.tissuetp,self.sample])))
                        self.inputs = set(real_files+sortbamsindir)
                        break

                    else:
                        file1 = atemplate.format(library=self.library,lane=self.lane)
                        real_file1 = self.check_file(all_fix_files,file1)
                        if real_file1:
                            self.inputs = [real_file1]
                            break

                elif self.pairs:
                    file1,file2 = atemplate.format(sample=self.sample),atemplate.format(sample=self.matchN)

                    #判断是否存在这个文件
                    real_file1 = self.check_file(all_fix_files,file1)
                    real_file2 = self.check_file(all_fix_files,file2)
                    if real_file1 and real_file2:
                        self.inputs = [real_file1,real_file2]
                        break

                else:
                    file1 = atemplate.format(sample=self.sample)
                    real_file1 = self.check_file(all_fix_files,file1)
                    if real_file1:
                        self.inputs = [real_file1]
                        break
        #这个判断是不是要放在类里面，还是在实例化之后做检查？在后面做检查更好
        #if len(self.inputs) == 0:
        #    sys.exit('We need a input for {sample} at {act_name}'.format(sample=self.sample,act_name=self.action_name))

    #check the inputfile we need is in infiles set or not
    def check_file(self,files,afile,mode='equal'):
        if mode == 'startswith':
            targetfiles = []
        for k in files:
            filename = os.path.split(k)[1]
            if mode == 'equal':
                if afile == filename:
                    return k
                elif afile in os.listdir(self.inputdir):
                    return os.path.join(self.inputdir,afile)
            elif mode == 'startswith':
                if filename.startswith(self.sample):
                    targetfiles.append(k)
        if mode != 'startswith':
            return None
        else:
            return targetfiles

    #generate output files
    def _confirm_output(self):
        for atemplate in self.output_template:
            if self.lane:
                if self.pe and '{end}' in atemplate:
                    self.output.append(os.path.join(self.outputdir,'.'.join([self.person,self.tissuetp,self.sample])+'/'+atemplate.format(library=self.library,lane=self.lane,end='R1')))
                    self.output.append(os.path.join(self.outputdir,'.'.join([self.person,self.tissuetp,self.sample])+'/'+atemplate.format(library=self.library,lane=self.lane,end='R2')))
                else:
                    self.output.append(os.path.join(self.outputdir,'.'.join([self.person,self.tissuetp,self.sample])+'/'+atemplate.format(sample=self.sample,library=self.library,lane=self.lane)))

            else:
                self.output.append(os.path.join(self.outputdir,'.'.join([self.person,self.tissuetp,self.sample])+'/'+atemplate.format(sample=self.sample)))

    def define_actionName(self):
        if self.lane:
            self.action_name = '_'.join([self.sample,self.library,self.lane,self.action_name])
        else:
            self.action_name = '_'.join([self.sample,self.action_name])


def instance_jobs(patient,wantedjobs,instance_actions={},inputfiles={}):
    instance_actions[patient] = {}
    for asample in patient:
        for atype in patient[asample]:
            for alib in patient[asample][atype]:
                for alane in patient[asample][atype][alib]:
                    for ajob in wantedjobs:
                        if wantedjobs[ajob]['lane']:
                            #instance action
                            aaction = Action(ajob,wantedjobs[ajob]['cmd'],wantedjobs[ajob]['inputdir'],wantedjobs[ajob]['outputdir'],
                                             alib,lane=alane,cpu=wantedjobs[ajob]['cpus'],memory=wantedjobs[ajob]['memory'],
                                             outputs=wantedjobs[ajob]['output_template'],inputs_priority_queue=wantedjobs[ajob]['input_suffix'])
                            instance_actions[patient][aaction.action_name] = aaction
                            if wantedjobs[ajob]['output_suffix'] not in inputfiles:
                                inputfiles[wantedjobs[ajob]['output_suffix']] = [aaction.output]
                            else:
                                inputfiles[wantedjobs[ajob]['output_suffix']].append(aaction.output)
                        elif wantedjobs[ajob]['pairs']:
                            if atype == 'N':
                                pass
                            else:
                                aaction = Action(ajob,wantedjobs[ajob]['cmd'],wantedjobs[ajob]['inputdir'],wantedjobs[ajob]['outputdir'],
                                             alib,cpu=wantedjobs[ajob]['cpus'],memory=wantedjobs[ajob]['memory'],
                                             outputs=wantedjobs[ajob]['output_template'],inputs_priority_queue=wantedjobs[ajob]['input_suffix'])
                                instance_actions[patient][aaction.action_name] = aaction
                                if wantedjobs[ajob]['output_suffix'] not in inputfiles:
                                    inputfiles[wantedjobs[ajob]['output_suffix']] = [aaction.output]
                                else:
                                    inputfiles[wantedjobs[ajob]['output_suffix']].append(aaction.output)
                        else:
                            aaction = Action(ajob,wantedjobs[ajob]['cmd'],wantedjobs[ajob]['inputdir'],wantedjobs[ajob]['outputdir'],
                                             alib,cpu=wantedjobs[ajob]['cpus'],memory=wantedjobs[ajob]['memory'],
                                             outputs=wantedjobs[ajob]['output_template'],inputs_priority_queue=wantedjobs[ajob]['input_suffix'])
                            instance_actions[patient][aaction.action_name] = aaction
                            if wantedjobs[ajob]['output_suffix'] not in inputfiles:
                                inputfiles[wantedjobs[ajob]['output_suffix']] = [aaction.output]
                            else:
                                inputfiles[wantedjobs[ajob]['output_suffix']].append(aaction.output)

def instance_action_inputs(aperson,instance_actions={},inputfiiles={}):
    all_inputs,all_outputs = {},{}
    for aaction in instance_actions[aperson]:
        instance_actions[aperson][aaction]._confirm_input(inputfiiles)
        for afile in aaction.output:
            if afile not in all_outputs:
                all_outputs[afile] = [aaction.action_name]
            else:
                all_outputs[afile].append(aaction.action_name)
        for afile in aaction.input:
            if afile not in all_outputs:
                all_outputs[afile] = [aaction.action_name]
            else:
                all_outputs[afile].append(aaction.action_name)
    return all_outputs,all_inputs





if __name__ == "__main__":
    instance_jobs()
