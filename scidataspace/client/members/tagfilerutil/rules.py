# 
# Copyright 2010 University of Southern California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""The rule processor and supporting class definitions."""

from globusonline.catalog.client.models import RERule, LineRule, DicomRule, NiftiRule, VcfRule, Tag
import re
import csv


class RERuleProcessor(object):
    """Processes a rerule object into tags."""
    
    def __init__(self, rerule):
        """Constructor
        
        Keyword arguments:
        rerule -- rerule object
        
        """
        self._rerule = rerule
        self.prepattern_processor = None
        self.prepattern = None
        if self._rerule.prepattern is not None:
            self.prepattern_processor = RERuleProcessor(self._rerule.prepattern)
        self.pattern = re.compile(rerule.pattern)

        self.apply_func = dict(match=self.apply_match,
                          search=self.apply_search,
                          finditer=self.apply_finditer)[rerule.apply]

        self.tester_func = dict(match=re.match,
                           search=re.search,
                           finditer=re.search)[rerule.apply]

        self.extract_func = dict(constants=self.extract_constant,
                            single=self.extract_single, 
                            positional=self.extract_positional,
                            named=self.extract_named,
                            template=self.extract_template)[rerule.extract]

        self.rewrites = [ (re.compile(r.get_rewrite_pattern()), r.get_rewrite_template()) for r in rerule.rewrites ]

        self.constants = rerule.constants

        self.tags = rerule.tags

        self.templates = rerule.templates
        
    def test(self, string):
        if self.prepattern and not self.prepattern.test(string):
            return False
        if self.tester_func(self.pattern, string):
            return True
        else:
            return False

    def analyze(self, string):
        if self.prepattern_processor and not self.prepattern_processor.test(string):
            return dict()
        return self.apply_func(string)

    def rewrite(self, valuestring):
        for pattern, template in self.rewrites:
            valuestring = re.sub(pattern, template, valuestring)
        return valuestring

    def extract_constant(self, match):
        if match:
            return self.constants
        else:
            return dict()

    def extract_single(self, match):
        if match:
            return { self.tags[0].get_tag_name() : set([ self.rewrite(match.group(0)) ]) }
        else:
            return dict()

    def extract_positional(self, match):
        if match:
            return dict([ (self.tags[i], set( [self.rewrite(match.group(i+1))] )) for i in range(0, len(self.tags))
                          if self.tags[i] and self.rewrite(match.group(i+1)) ])
        else:
            return dict()

    def extract_named(self, match):
        if match:
            return dict([ (key, set([ self.rewrite(value) ]) ) for key, value in match.groupdict().items() if self.rewrite(value) ])
        else:
            return dict()

    def extract_template(self, match):
        if match:
            return dict([ (self.tags[i], set([self.rewrite(match.expand(self.templates[i]))]) ) for i in range(0, len(self.tags)) ])
        else:
            return dict()

    def apply_match(self, string):
        return self.extract_func( re.match(self.pattern, string) )

    def apply_search(self, string):
        return self.extract_func( re.search(self.pattern, string) )

    def apply_finditer(self, string):
        def dictmerge(tags, newtags):
            for tag, valset in newtags.items():
                if type(valset) != set:
                    if type(valset) == list:
                        valset = set(valset)
                    else:
                        valset = set([valset])
                if not tags.has_key(tag):
                    tags[tag] = valset.copy()
                else:
                    tags[tag].update(valset)

        tags = dict()
        for match in re.finditer(self.pattern, string):
            dictmerge(tags, self.extract_func(match))
        return tags


class PathRuleProcessor(RERuleProcessor):
    def analyze(self, file_path):
        return super(PathRuleProcessor, self).analyze(file_path)


class LineRuleProcessor(object):
    def __init__(self, linerule):
        """Constructor
        
        Keyword arguments:
        linerule -- LineRule object
        
        """
        self.namefield = linerule.namefield
        
        if linerule.prepattern:
            self.prepattern = re.compile(linerule.prepattern)
        else:
            self.prepattern = None
    
    def analyze(self, string):
        if self.prepattern and not re.match(self.prepattern, string):
            return dict()
        dict_list = []
        with open(string, 'rU') as csvfile:
            r = csv.DictReader(csvfile)
            for row in r:
                nameval = row[self.namefield]
                row['name'] = string + ":" + nameval
                dict_list.append(row)
        return dict_list


class DicomRuleProcessor(object):
    def __init__(self, dicomrule):
        """Constructor
        
        Keyword arguments:
        dicomrule -- DicomRule object
        
        """
        if dicomrule.prepattern:
            self.prepattern = re.compile(dicomrule.prepattern)
        else:
            self.prepattern = None
            
        self.tagnames = dicomrule.tagnames
    
    def analyze(self, string):
        if self.prepattern and not re.match(self.prepattern, string):
            return dict()
        tag_dict = dict()
        
        import dicom    #@UnresolvedImport
        dcm = dicom.read_file(string)
        for tagname in self.tagnames:
            if tagname and not (tagname == '') and tagname in dcm:
                value = dcm.get(tagname)
                if not value or value == '':
                    continue
                value = value.replace('\x00', '')
                tag_dict[tagname] = [value]
                #TODO: need to handle multiple values returned from dicom object
                
        return tag_dict


class NiftiRuleProcessor(object):
    def __init__(self, niftirule):
        """Constructor
        
        Keyword arguments:
        Niftirule -- NiftiRule object
        
        """
        if niftirule.prepattern:
            self.prepattern = re.compile(niftirule.prepattern)
        else:
            self.prepattern = None
            
        self.tagnames = niftirule.tagnames
    
    def analyze(self, string):
        if self.prepattern and not re.match(self.prepattern, string):
            return dict()
        tag_dict = dict()
        
        import nibabel as nib   #@UnresolvedImport
        img = nib.load(string)
        hdr = img.get_header()
        for tagname in self.tagnames:
            if tagname and not (tagname == '') and tagname in hdr:
                value = str(hdr[tagname])
                if not value or value == '':
                    continue
                value = value.replace('\x00', '')
                tag_dict[tagname] = [value]
                #TODO: need to handle multiple values returned from image header
                
        return tag_dict


class VcfRuleProcessor(object):
    def __init__(self, vcfrule):
        """Constructor
        
        Keyword arguments:
        vcfrule -- DicomRule object
        
        """
        if vcfrule.prepattern:
            self.prepattern = re.compile(vcfrule.prepattern)
        else:
            self.prepattern = None
         
        self.tagnames = vcfrule.tagnames
    
    def analyze(self, string):
        if self.prepattern and not re.match(self.prepattern, string):
            return dict()
        tag_dict = dict()

	import vcf
	vcf_reader = vcf.Reader(open(string, 'rb'))

	for tagname in self.tagnames:
		# check first if the tag is in the metadata
    		if tagname in vcf_reader.metadata:
			if isinstance(vcf_reader.metadata[tagname], basestring):
				tag_dict[tagname] =[vcf_reader.metadata[tagname]]
			else:
				tag_dict[tagname] =[vcf_reader.metadata[tagname][0]]
		elif tagname == "ChromPos":
			# read only first 2 records 
			tag_dict[tagname] = []
			for x in range(0, 2):
				record = vcf_reader.next()
				tag_dict[tagname].append("%s-%s" %(record.CHROM, record.POS))

  
        return tag_dict


class TagDirector(object):
    def tag_registered_file(self, rules, fileobj):
        for rule in rules:
            tag_dict = self.get_rule_processor(rule).analyze(fileobj.filename)
            for k,v_list in tag_dict.iteritems():
                if not k or k == '':
                    continue
                for v in v_list:
                    if not v or v == '':
                        continue
                    t = Tag(name=k, value=v)
                    fileobj.tags.append(t)
                    
    def tag_file_contents(self, rules, fileobj):
        for rule in rules:
            tag_dict_list = self.get_rule_processor(rule).analyze(fileobj.filename)
            for tag_dict in tag_dict_list:
                content_tags = []
                for k,v in tag_dict.iteritems():
                    if not k or not v or k == '' or v == '':
                        continue
                    t = Tag(name=k, value=v)
                    content_tags.append(t)
                fileobj.content_tags.append(content_tags)

    def get_rule_processor(self, rule):
        if isinstance(rule, RERule):
            return PathRuleProcessor(rule)
        elif isinstance(rule, LineRule):
            return LineRuleProcessor(rule)
        elif isinstance(rule, DicomRule):
            return DicomRuleProcessor(rule)
        elif isinstance(rule, NiftiRule):
            return NiftiRuleProcessor(rule)
	elif isinstance(rule, VcfRule):
            return VcfRuleProcessor(rule)
        else:
            raise TypeError("Unsupported rule type for %s" % unicode(rule))

