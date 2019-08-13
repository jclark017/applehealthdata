# -*- coding: utf-8 -*-
"""
applehealthdata.py: Extract data from Apple Health App's export.xml.

Copyright (c) 2016 Nicholas J. Radcliffe
Licence: MIT
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import re
import sys
import sqlite3

from xml.etree import ElementTree
from collections import Counter, OrderedDict

__version__ = '1.3'

RECORD_FIELDS = OrderedDict((
    ('sourceName', 's'),
    ('sourceVersion', 's'),
    ('device', 's'),
    ('type', 's'),
    ('unit', 's'),
    ('creationDate', 'd'),
    ('startDate', 'd'),
    ('endDate', 'd'),
    ('value', 'n'),
))

ACTIVITY_SUMMARY_FIELDS = OrderedDict((
    ('dateComponents', 'd'),
    ('activeEnergyBurned', 'n'),
    ('activeEnergyBurnedGoal', 'n'),
    ('activeEnergyBurnedUnit', 's'),
    ('appleExerciseTime', 's'),
    ('appleExerciseTimeGoal', 's'),
    ('appleStandHours', 'n'),
    ('appleStandHoursGoal', 'n'),
))

WORKOUT_FIELDS = OrderedDict((
    ('sourceName', 's'),
    ('sourceVersion', 's'),
    ('device', 's'),
    ('creationDate', 'd'),
    ('startDate', 'd'),
    ('endDate', 'd'),
    ('workoutActivityType', 's'),
    ('duration', 'n'),
    ('durationUnit', 's'),
    ('totalDistance', 'n'),
    ('totalDistanceUnit', 's'),
    ('totalEnergyBurned', 'n'),
    ('totalEnergyBurnedUnit', 's'),
))

FIELDS = {
    'Record': RECORD_FIELDS,
    'ActivitySummary': ACTIVITY_SUMMARY_FIELDS,
    'Workout': WORKOUT_FIELDS,
}

CONSTANTS = {
    'HKCategoryValueAppleStandHourIdle': '0',
    'HKCategoryValueAppleStandHourStood': '1',
    'HKCategoryValueSleepAnalysisInBed': '1',
}


PREFIX_RE = re.compile('^HK.*TypeIdentifier(.+)$')
ABBREVIATE = True
VERBOSE = True

def format_freqs(counter):
    """
    Format a counter object for display.
    """
    return '\n'.join('%s: %d' % (tag, counter[tag])
                     for tag in sorted(counter.keys()))


def format_value(value, datatype):
    """
    Format a value for a CSV file, escaping double quotes and backslashes.

    None maps to empty.

    datatype should be
        's' for string (escaped)
        'n' for number
        'd' for datetime
    """
    if value is None:
        return ''
    elif datatype in ('s', 'd'):  # string
        return '\'%s\'' % value.replace('\\', '\\\\').replace('"', '\\"')
    elif datatype == 'n':  # number or date
        # Handle weird constant value for sleep analysis
        if value in CONSTANTS:
            return CONSTANTS.get(value)
        else:
            if len(value) ==0:
                return '0'
            else:
                return value
    else:
        raise KeyError('Unexpected format value: %s' % datatype)

def dtype(datatype):
    """
    Format a data type in dictionary, return the sqlite datatype.

    None maps to empty.

    datatype should be
        's' for string (escaped)
        'n' for number
        'd' for datetime
    """
    if datatype == 's':  # string
        return 'text'
    elif datatype == 'n':  # number or date
        return 'numeric'
    elif datatype == 'd':  # number or date
        return 'text'
    else:
        raise KeyError('Unexpected format value: %s' % datatype)

def abbreviate(s, enabled=ABBREVIATE):
    """
    Abbreviate particularly verbose strings based on a regular expression
    """
    m = re.match(PREFIX_RE, s)
    return m.group(1) if enabled and m else s


def encode(s):
    """
    Encode string for writing to file.
    In Python 2, this encodes as UTF-8, whereas in Python 3,
    it does nothing
    """
    return s.encode('UTF-8') if sys.version_info.major < 3 else s

class HealthDataExtractorEV(object):
    """
    Extract health data from Apple Health App's XML export, export.xml.

    Inputs:
        path:      Relative or absolute path to export.xml
        verbose:   Set to False for less verbose output

    Outputs:
        Writes a CSV file for each record type found, in the same
        directory as the input export.xml. Reports each file written
        unless verbose has been set to False.
    """
    def __init__(self, path, verbose=VERBOSE):
        self.handles = {}
        self.paths = []
        self.in_path = path
        self.verbose = verbose
        self.directory = os.path.abspath(os.path.split(path)[0])

        conn = sqlite3.connect(os.path.join(self.directory, 'export.sqlite'))
        c = conn.cursor()
        with open(path) as f:
            self.report('Reading data from %s . . . ' % path, end='')
            #self.data = ElementTree.iterparse(f)
            self.report('done')

            # get an iterable
            context = ElementTree.iterparse(f, events=("start", "end"))

            # turn it into an iterator
            context = iter(context)

            # get the root element
            event, root = next(context)
            
            cnt = 1
            for event, element in context:
                # element is a whole element
                if event == "end":
                    self.abbreviate_types(element)
                    self.write_records(element, c)
                    if cnt % 10000 == 0:
                        conn.commit()
                cnt = cnt + 1
                root.clear()

        
        conn.close()
        #self.root = self.data._root
        #self.nodes = self.root.getchildren()
        #self.n_nodes = len(self.nodes)
        #self.abbreviate_types()
    
    def abbreviate_types(self, node):
        """
        Shorten types by removing common boilerplate text.
        """
        if node.tag == 'Record':
            if 'type' in node.attrib:
                node.attrib['type'] = abbreviate(node.attrib['type'])

    
    def report(self, msg, end='\n'):
        if self.verbose:
            print(msg, end=end)
            sys.stdout.flush()

    def write_records(self, node, c):
        kinds = FIELDS.keys()
        if node.tag in kinds:
            attributes = node.attrib
            kind = attributes['type'] if node.tag == 'Record' else node.tag
            values = [format_value(attributes.get(field,''), datatype)
                        for (field, datatype) in FIELDS[node.tag].items()]
            tl = self.table_list(c)
            line = encode(','.join(values))
            if kind in tl:
                self.write_record(kind, line, c)
            else:
                self.open_for_writing(node.tag, kind, c)
                self.write_record(kind, line, c)
    
    def table_list(self, c):
        c.execute('SELECT name FROM sqlite_master WHERE type =\'table\' AND name NOT LIKE \'sqlite_%\';')
        names = [tup[0] for tup in c.fetchall()]
        return names

    def open_for_writing(self, tag, kind, c):
        fl = ', '.join('{} {}'.format(key, dtype(value)) for key, value in FIELDS[tag].items())
        c.execute('CREATE TABLE {} ({})' .format(kind, fl))

    def write_record(self, kind, line, c):
        script = 'INSERT INTO {} VALUES ({})' .format(kind, line)
        c.execute(script)
    
    def close_files(self):
        for (kind, f) in self.handles.items():
            f.close()
            self.report('Written %s data.' % abbreviate(kind))

# class HealthDataExtractor(object):
#     """
#     Extract health data from Apple Health App's XML export, export.xml.

#     Inputs:
#         path:      Relative or absolute path to export.xml
#         verbose:   Set to False for less verbose output

#     Outputs:
#         Writes a CSV file for each record type found, in the same
#         directory as the input export.xml. Reports each file written
#         unless verbose has been set to False.
#     """
#     def __init__(self, path, verbose=VERBOSE):
#         self.in_path = path
#         self.verbose = verbose
#         self.directory = os.path.abspath(os.path.split(path)[0])
#         with open(path) as f:
#             self.report('Reading data from %s . . . ' % path, end='')
#             self.data = ElementTree.parse(f)
#             self.report('done')
#         self.root = self.data._root
#         self.nodes = self.root.getchildren()
#         self.n_nodes = len(self.nodes)
#         self.abbreviate_types()
#         self.collect_stats()

#     def report(self, msg, end='\n'):
#         if self.verbose:
#             print(msg, end=end)
#             sys.stdout.flush()

#     def count_tags_and_fields(self):
#         self.tags = Counter()
#         self.fields = Counter()
#         for record in self.nodes:
#             self.tags[record.tag] += 1
#             for k in record.keys():
#                 self.fields[k] += 1

#     def count_record_types(self):
#         """
#         Counts occurrences of each type of (conceptual) "record" in the data.

#         In the case of nodes of type 'Record', this counts the number of
#         occurrences of each 'type' or record in self.record_types.

#         In the case of nodes of type 'ActivitySummary' and 'Workout',
#         it just counts those in self.other_types.

#         The slightly different handling reflects the fact that 'Record'
#         nodes come in a variety of different subtypes that we want to write
#         to different data files, whereas (for now) we are going to write
#         all Workout entries to a single file, and all ActivitySummary
#         entries to another single file.
#         """
#         self.record_types = Counter()
#         self.other_types = Counter()
#         for record in self.nodes:
#             if record.tag == 'Record':
#                 self.record_types[record.attrib['type']] += 1
#             elif record.tag in ('ActivitySummary', 'Workout'):
#                 self.other_types[record.tag] += 1
#             elif record.tag in ('Export', 'Me'):
#                 pass
#             else:
#                 self.report('Unexpected node of type %s.' % record.tag)

#     def collect_stats(self):
#         self.count_record_types()
#         self.count_tags_and_fields()

#     def open_for_writing(self):
#         self.handles = {}
#         self.paths = []
#         for kind in (list(self.record_types) + list(self.other_types)):
#             path = os.path.join(self.directory, '%s.csv' % abbreviate(kind))
#             f = open(path, 'w')
#             headerType = (kind if kind in ('Workout', 'ActivitySummary')
#                                else 'Record')
#             f.write(','.join(FIELDS[headerType].keys()) + '\n')
#             self.handles[kind] = f
#             self.report('Opening %s for writing' % path)

#     def abbreviate_types(self):
#         """
#         Shorten types by removing common boilerplate text.
#         """
#         for node in self.nodes:
#             if node.tag == 'Record':
#                 if 'type' in node.attrib:
#                     node.attrib['type'] = abbreviate(node.attrib['type'])

#     def write_records(self):
#         kinds = FIELDS.keys()
#         for node in self.nodes:
#             if node.tag in kinds:
#                 attributes = node.attrib
#                 kind = attributes['type'] if node.tag == 'Record' else node.tag
#                 values = [format_value(attributes.get(field), datatype)
#                           for (field, datatype) in FIELDS[node.tag].items()]
#                 line = encode(','.join(values) + '\n')
#                 self.handles[kind].write(line)

#     def close_files(self):
#         for (kind, f) in self.handles.items():
#             f.close()
#             self.report('Written %s data.' % abbreviate(kind))

#     def extract(self):
#         self.open_for_writing()
#         self.write_records()
#         self.close_files()

#     def report_stats(self):
#         print('\nTags:\n%s\n' % format_freqs(self.tags))
#         print('Fields:\n%s\n' % format_freqs(self.fields))
#         print('Record types:\n%s\n' % format_freqs(self.record_types))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('USAGE: python applehealthdata.py /path/to/export.xml',
              file=sys.stderr)
        sys.exit(1)
    data = HealthDataExtractorEV(sys.argv[1])
    data.close_files()
#    data.report_stats()
#    data.extract()
