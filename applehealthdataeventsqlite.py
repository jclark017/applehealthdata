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
from datetime import datetime

__version__ = '1.3'

LOOKUP_FIELDS = OrderedDict((
    ('sourceName', 'sourceName'),
    ('sourceVersion', 'sourceVersion'),
    ('device', 'device'),
    ('type', 'type'),
    ('workoutActivityType', 'type'),
    ('unit', 'unit'),
    ('durationUnit', 'unit'),
    ('totalDistanceUnit', 'unit'),
    ('totalEnergyBurnedUnit', 'unit'),
))

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

RECORD_FIELDS_HR = OrderedDict((
    ('sourceName', 's'),
    ('sourceVersion', 's'),
    ('device', 's'),
    ('type', 's'),
    ('unit', 's'),
    ('creationDate', 'd'),
    ('startDate', 'd'),
    ('endDate', 'd'),
    ('motionContext', 'd'),
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

ACTIVITY_SUMMARY_VERSIONS = {
    '1': ACTIVITY_SUMMARY_FIELDS
}

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

WORKOUT_VERSIONS = {
    '1': WORKOUT_FIELDS
}

RECORD_TYPES = {
    'ActiveEnergyBurned': RECORD_FIELDS,
    'AppleExerciseTime': RECORD_FIELDS,
    'AppleStandHour': RECORD_FIELDS,
    'BasalEnergyBurned': RECORD_FIELDS,
    'BodyMass': RECORD_FIELDS,
    'DistanceWalkingRunning': RECORD_FIELDS,
    'FlightsClimbed': RECORD_FIELDS,
    'HeartRate': RECORD_FIELDS_HR,
    'HeartRateVariabilitySDNN': RECORD_FIELDS,
    'Height': RECORD_FIELDS,
    'MindfulSession': RECORD_FIELDS,
    'SleepAnalysis': RECORD_FIELDS,
    'StepCount': RECORD_FIELDS,
    'VO2Max': RECORD_FIELDS,
    'RestingHeartRate': RECORD_FIELDS,
    'WalkingHeartRateAverage': RECORD_FIELDS
}

FIELDS = {
    'Record': RECORD_TYPES,
    'ActivitySummary': ACTIVITY_SUMMARY_VERSIONS,
    'Workout': WORKOUT_VERSIONS,
}

CONSTANTS = {
    'HKCategoryValueAppleStandHourIdle': '0',
    'HKCategoryValueAppleStandHourStood': '1',
    'HKCategoryValueSleepAnalysisInBed': '1',
}

LOOKUP_VALUES = {}

PREFIX_RE = re.compile('^HK.*TypeIdentifier(.+)$')
DEVICE_RE = re.compile('^<<HK.*>, (.+)>$')
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

def abbreviate(s, reg, enabled=ABBREVIATE):
    """
    Abbreviate particularly verbose strings based on a regular expression
    """
    m = re.match(reg, s)
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
        self.tl = []

        conn = sqlite3.connect(os.path.join(self.directory, 'export.sqlite'))
        c = conn.cursor()
        starttime = datetime.now()
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
                    for elem in element:
                        if elem.tag == 'MetadataEntry' and elem.attrib['key'] == "HKMetadataKeyHeartRateMotionContext":
                            element.attrib['motionContext'] = elem.attrib['value']
                    self.abbreviate_types(element)
                    self.write_records(element, c)
                    # commit every 10,000 rows
                    if cnt % 10000 == 0:
                        conn.commit()
                        diff = datetime.now() - starttime
                        self.report(str(cnt) + "-" + str(diff.seconds) + ": " + str(round(diff.seconds/cnt)))

                cnt = cnt + 1
                root.clear()

            # dump the lookup lists to tables
            self.lookup_output(c)
            conn.commit()
        
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
                node.attrib['type'] = abbreviate(node.attrib['type'], PREFIX_RE)
            if 'device' in node.attrib:
                node.attrib['device'] = abbreviate(node.attrib['device'], DEVICE_RE)

    
    def report(self, msg, end='\n'):
        if self.verbose:
            print(msg, end=end)
            sys.stdout.flush()

    def write_records(self, node, c):
        kinds = FIELDS.keys()
        if node.tag in kinds:
            attributes = node.attrib
            kind = attributes['type'] if node.tag == 'Record' else node.tag
            version = attributes['type'] if node.tag == 'Record' else "1"

            values = [self.lookup(field,format_value(attributes.get(field,''), datatype),c)
                        for (field, datatype) in FIELDS[node.tag][version].items()]
 
            line = encode(','.join(values))
            if kind in self.tl:
                self.write_record(kind, line, c)
            else:
                self.open_for_writing(node.tag, version, kind, c)
                self.tl = self.table_list(c)
                self.write_record(kind, line, c)
    
    def lookup(self, field, value, c):
        if LOOKUP_FIELDS.get(field) is None:
            return value
        else:
            if LOOKUP_VALUES.get(LOOKUP_FIELDS[field]) is None:
                LOOKUP_VALUES[LOOKUP_FIELDS[field]] = []

            if value.replace("'","") in LOOKUP_VALUES[LOOKUP_FIELDS[field]]:
                return format_value(str(LOOKUP_VALUES[LOOKUP_FIELDS[field]].index(value.replace("'",""))),'s')
            else:
                LOOKUP_VALUES[LOOKUP_FIELDS[field]].append(value.replace("'",""))
                return format_value(str(LOOKUP_VALUES[LOOKUP_FIELDS[field]].index(value.replace("'",""))),'s')

#            names = self.table_list(c)
#            if 'lookup' + field in names:
#                value = self.lookup_lov('lookup' + field, value, c)
#            else:
#                self.lookup_create('lookup' + field, c)
#                value = self.lookup_lov('lookup' + field, value, c)
#            return format_value(str(value),'s')

    def lookup_create(self, table, c):
        c.execute('CREATE TABLE {} (value TEXT, name TEXT)' .format(table))

    def lookup_output(self, c):
        for lst in LOOKUP_VALUES:
            self.lookup_create('z' + lst,c)
        
            for value in LOOKUP_VALUES[lst]:
                # Insert the missing value
                script = 'INSERT INTO {} (value, name) VALUES ({}, {})' .format('z' + lst, format_value(str(LOOKUP_VALUES[lst].index(value)),'s'), format_value(value,'s'))
                c.execute(script)

    # def lookup_table(self, table, value, c):
    #     script = 'SELECT value, name FROM {} WHERE name = {}' .format(table, value)
    #     c.execute(script)
    #     rows = c.fetchall()
    #     for row in rows:
    #         if row[1] == value.replace("'",""):
    #             return row[0]
        
    #     # Insert the missing value
    #     script = 'INSERT INTO {} (name) VALUES ({})' .format(table, value)
    #     c.execute(script)
    #     return self.lookup_lov(table, value, c)

    def table_list(self, c):
        c.execute('SELECT name FROM sqlite_master WHERE type =\'table\' AND name NOT LIKE \'sqlite_%\';')
        names = [tup[0] for tup in c.fetchall()]
        return names

    def open_for_writing(self, tag, version, kind, c):
        fl = ', '.join('{} {}'.format(key, dtype(value)) for key, value in FIELDS[tag][version].items())
        c.execute('CREATE TABLE {} ({})' .format(kind, fl))

    def write_record(self, kind, line, c):
        script = 'INSERT INTO {} VALUES ({})' .format(kind, line)
        c.execute(script)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('USAGE: python applehealthdata.py /path/to/export.xml',
              file=sys.stderr)
        sys.exit(1)
    data = HealthDataExtractorEV(sys.argv[1])
#    data.report_stats()
#    data.extract()
