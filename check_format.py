import os


class BioFile:
    '''
     define file format basing on suffix
    '''

    def __init__(self,formmat,suffixes=[]):
        self.suffixes = suffixes
        self.formmat = formmat

    def suffix_check(self, afile):
        for suffix in self.suffixes:
            if afile.endswith(suffix):
                return True
        return False


fq_suffixes = ['fastq','fq','fastq.gz','fq.gz']
bam_suffixes = ['bam']
vcf_suffixes = ['vcf']

fq_file = BioFile('fq',fq_suffixes)
bam_file = BioFile('bam',bam_suffixes)
vcf_file = BioFile('vcf',vcf_suffixes)

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
            return False,'there is no {afile} input for {JOBs}'.format(afile=afile,JOBs=','.join(files[afile][0]))
    return True,'good flow'

if __name__ == "__main__":
    print BioFile.__doc__
