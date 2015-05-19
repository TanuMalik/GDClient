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
"""
Models for Outbox configuration and local state.
"""

class Outbox(object):
    """Represents the Outbox configuration."""

    def __init__(self, **kwargs):
        self.name = kwargs.get("name")
        self.state_db = kwargs.get("state_db")
        self.bulk_ops_max = kwargs.get("bulk_ops_max")
        self.endpoint_name = kwargs.get("endpoint_name")
        self.url = kwargs.get("url")
        self.username = kwargs.get("username")
        self.password = kwargs.get("password")
        self.roots = kwargs.get("roots", [])
        self.includes = kwargs.get("includes", [])
        self.excludes = kwargs.get("excludes", [])
        self.path_rules = kwargs.get("path_rules", [])
        self.line_rules = kwargs.get("line_rules", [])
        self.dicom_rules = kwargs.get("dicom_rules", [])
        self.nifti_rules = kwargs.get("nifti_rules", [])
        self.vcf_rules = kwargs.get("vcf_rules", [])

class RERule(object):
    """A regular expression rule used for tagging."""
    
    def __init__(self, **kwargs):
        
        self.pattern = kwargs.get("pattern")
        self.apply = kwargs.get("apply", "match")
        self.extract = kwargs.get("extract", "single")
        self.rewrites = kwargs.get("rewrites", [])
        self.constants = kwargs.get("constants", [])
        
        self.tags = kwargs.get("tags", [])
        tag = kwargs.get("tag")
        if tag:
            self.tags.append(tag)
            
        self.templates = kwargs.get("templates", [])
        template = kwargs.get("template")
        if template:
            self.templates.append(template)
        
        prepatternstr = kwargs.get("prepattern")
        if prepatternstr:
            self.prepattern = RERule(**{"pattern": prepatternstr})
        else:
            self.prepattern = None


class LineRule(object):
    """A regular expression rule used for tagging based on file contents."""
    def __init__(self, **kwargs):
        #Note: the following code was in here originally, from dsmith or karlcz.
        #    I am just keeping it in case it was to be used somehow...
        #self.path_rule = kwargs.get("path_rule")
        #self.rerules = kwargs.get("rerules", [])
        
        self.prepattern = kwargs.get("prepattern")
        self.namefield = kwargs.get("namefield")

class DicomRule(object):
    """A regular expression rule used for tagging based on dicom format."""
    def __init__(self, **kwargs):
        self.prepattern = kwargs.get("prepattern")
        self.tagnames = kwargs.get("tagnames", [])

class NiftiRule(object):
    """A regular expression rule used for tagging based on nifti format."""
    def __init__(self, **kwargs):
        self.prepattern = kwargs.get("prepattern")
        self.tagnames = kwargs.get("tagnames", [])

class VcfRule(object):
    """A regular expression rule used for tagging based on vcf format."""
    def __init__(self, **kwargs):
        self.prepattern = kwargs.get("prepattern")
        self.tagnames = kwargs.get("tagnames", [])

class RERuleConstant(object):
    """A constant assigned to a rerule."""
    def __init__(self, **kwargs):
        self.name = kwargs.get("name")
        self.value = kwargs.get("value")        


class RERuleRewrite(object):
    """A rewrite pattern and template for a rerule."""
    def __init__(self, **kwargs):
        self.pattern = kwargs.get("pattern")
        self.template = kwargs.get("template")


class File(object):
    """Represents a File."""
    
    # File status flag values
    COMPUTE     = 0
    COMPARE     = 1
    REGISTER    = 2
    
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.filename = kwargs.get("filename")
        self.mtime = kwargs.get("mtime")
        self.rtime = kwargs.get("rtime")
        self.size = kwargs.get("size")
        self.checksum = kwargs.get("checksum")
        self.username = kwargs.get("username")
        self.groupname = kwargs.get("groupname")
        self.tags = kwargs.get("tags", [])
        self.content_tags = kwargs.get("content_tags", [])
        self.status = kwargs.get("status")
        
    def filter_tags(self, name):
        return [tag for tag in self.tags if tag.name == name]

    def __str__(self):
        s = self.filename
        s += " <%s> " % self.id
        s += " (%s %s %s %s %s) [" % (self.mtime, self.rtime, self.size, 
                                      self.username, self.groupname)
        for t in self.tags:
            s += "%s, " % t
        s += "]"
        return s


class Tag(object):
    """Represents a Tag."""
    
    def __init__(self, name=None, value=None, **kwargs):
        self.name = name or kwargs.get("name")
        self.value = value or kwargs.get("value")

    def __str__(self):
        s = "%s=%s" % (self.name, self.value)
        return s


def create_default_name_path_rule(endpoint):
    """Creates the path rule for the required 'name' tag."""
    path_rule = RERule(**{})
    path_rule.pattern = '^(?P<path>.*)'
    path_rule.extract = 'template'
    path_rule.templates.append('%s\g<path>' % endpoint)
    path_rule.tags.append('name')
    return path_rule
