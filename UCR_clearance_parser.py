#! /usr/bin/env python
#-----------------------------------------------------------------------------------

# Author: https://github.com/bclifton

#-----------------------------------------------------------------------------------
import glob
import logging
import datetime
from collections import OrderedDict

import pandas as pd
import numpy as np

from sqlalchemy import create_engine

#-----------------------------------------------------------------------------------

logger = logging.getLogger()
hdlr = logging.FileHandler('{}.error.log.clearance'.format(datetime.date.today()))
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)

engine = create_engine('postgresql://localhost/', encoding='utf-8')

#-----------------------------------------------------------------------------------

def str_int(s):
    if s.isdigit():
        return int(s)
    else:
        try:
            print int(s,0)
        except:
            s = s.replace('0', '')
            if s[-1] in neg_num:
                return -int(s[:-1] + neg_num[s[-1]], 0	)
            else:
                # intentionally returning the error
                # These are most likely data entry errors, for example: '000J4' instead of '0004J'
                return s

def check_int(s):
    if s.isdigit():
        return int(s)
    else:
        return s.strip()


def remove_escape_chars(s):
    return filter(lambda x: ord(x)<128, s)


def full_year(s):
    last_two = s[-2:]
    if int(last_two) <= 30:
        return int('20{}'.format(last_two))
    else:
        return int('19{}'.format(last_two))


#-----------------------------------------------------------------------------------
state_code = {
'50': 'AK - Alaska',
'01': 'AL - Alabama',
'03': 'AR - Arkansas',
'54': 'AS - American Samoa',
'02': 'AZ - Arizona',
'04': 'CA - California',
'05': 'CO - Colorado',
'06': 'CT - Connecticut',
'52': 'CZ - Canal Zone',
'08': 'DC - District of Columbia',
'07': 'DE - Delaware',
'09': 'FL - Florida',
'10': 'GA - Georgia',
'55': 'GM - Guam',
'51': 'HI - Hawaii',
'14': 'IA - Iowa',
'11': 'ID - Idaho',
'12': 'IL - Illinois',
'13': 'IN - Indiana',
'15': 'KS - Kansas',
'16': 'KY - Kentucky',
'17': 'LA - Louisiana',
'20': 'MA - Massachusetts',
'19': 'MD - Maryland',
'18': 'ME - Maine',
'21': 'MI - Michigan',
'22': 'MN - Minnesota',
'24': 'MO - Missouri',
'23': 'MS - Mississippi',
'25': 'MT - Montana',
'26': 'NB - Nebraska',
'32': 'NC - North Carolina',
'33': 'ND - North Dakota',
'28': 'NH - New Hampshire',
'29': 'NJ - New Jersey',
'30': 'NM - New Mexico',
'27': 'NV - Nevada',
'31': 'NY - New York',
'34': 'OH - Ohio',
'35': 'OK - Oklahoma',
'36': 'OR - Oregon',
'37': 'PA - Pennsylvania',
'53': 'PR - Puerto Rico',
'38': 'RI - Rhode Island',
'39': 'SC - South Carolina',
'40': 'SD - South Dakota',
'41': 'TN - Tennessee',
'42': 'TX - Texas',
'43': 'UT - Utah',
'62': 'VI - Virgin Islands',
'45': 'VA - Virginia',
'44': 'VT - Vermont',
'46': 'WA - Washington',
'48': 'WI - Wisconsin',
'47': 'WV - West Virginia',
'49': 'WY - Wyoming',
' ': '',
'': ''
}

group = {
'0':  'Possessions [Puerto Rico, Guam, Canal Zone, Virgin Islands, and American Samoa]',
'1':  'All cities 250,000 or over',
'1A': 'Cities 1,000,000 or over',
'1B': 'Cities from 500,000 thru 999,999',
'1C': 'Cities from 250,000 thru 499,999',
'2':  'Cities from 100,000 thru 249,000',
'3':  'Cities from 50,000 thru 99,000',
'4':  'Cities from 25,000 thru 49,999',
'5':  'Cities from 10,000 thru 24,999',
'6':  'Cities from 2,500 thru 9,999',
'7':  'Cities under 2,500',
'8':  'Non-MSA Counties',
'8A': 'Non-MSA counties 100,000 or over',
'8B': 'Non-MSA counties from 25,000 thru 99,999',
'8C': 'Non-MSA counties from 10,000 thru 24,999',
'8D': 'Non-MSA counties under 10,000',
'8E': 'Non-MSA State Police',
'9':  'MSA Counties',
'9A': 'MSA counties 100,000 or over',
'9B': 'MSA counties from 25,000 thru 99,999',
'9C': 'MSA counties from 10,000 thru 24,999',
'9D': 'MSA counties under 10,000',
'9E': 'MSA State Police',
' ': '',
'': ''
}

division = {
'0': 'Possessions',
'1': 'New England',
'2': 'Middle Atlantic',
'3': 'East North Central',
'4': 'West North Central',
'5': 'South Atlantic',
'6': 'East South Central',
'7': 'West South Central',
'8': 'Mountain',
'9': 'Pacific',
' ': '',
'': ''
}

core = {
'Y': 'agency is core city of an MSA',
'N': 'agency is not a core city of an MSA',
' ': '',
'': ''
}

follow_up = {
'Y': 'Send a Follow-Up',
'N': 'Do Not Send a Follow-Up',
' ': '',
'': ''
}

smg = {
'0': 'If not a special mailing group agency.',
'1': 'If the return is to be sent to another agency.',
'2': 'Small city [groups 5 - 7] to be sent a large city [groups 1 - 4] form.',
'7': 'Agency is a "Non-Contributor", it is not sent forms.',
'9': 'Agency is a Contributor but not on the mailing list, they are not sent forms.',
' ': '',
'': ''
}

sma = {
'Y': 'Special mailing address',
'N': 'Not a special mailing address',
' ': '',
'': ''
}

card_type = {
'0': 'Not updated',
'2': 'Adjustment',
'4': 'Not available',
'5': 'Normal Return',
'8': 'Information is included but is unusable',
' ': '',
'': ''
}

card_data = {
'P': 'Breakdown of Offenses',
'T': 'Totals',
'0': 'No Return',
'2': 'Unknown Code',
' ': '',
'': ''
}

neg_num = {
'}': '0',
'J': '1',
'K': '2',
'L': '3',
'M': '4',
'N': '5',
'O': '6',
'P': '7',
'Q': '8',
'R': '9'
}


#-----------------------------------------------------------------------------------

def main():
    files = glob.glob('/returnA/*.txt')

    for f in files:
        print f
        lines = open(f, 'rb').read().splitlines()
        row_data = []

        for i, line in enumerate(lines):
            try:
                line = '_{}'.format(line) # Add a '_' so indexes match the char positions in the summary.

                # ----------- HEADER ----------- #
                data = OrderedDict()

                # identifier = line[1]                                              # Identifier code for the ASR master file
                data['state'] = state_code[line[2:4]]                               # Numeric State Code. Range is 01-62.  Data  records are in order by ORI code within numeric state code.
                data['ori_code'] = line[4:11]                                       # ORI Code. Originating Agency Identifier.
                data['group'] = group[line[11:13].strip()]                          # Group. Group 0 is possessions;  1-7 are cities, 8-9 are counties. All populations are inclusive. 
                data['division'] = division[line[13]]                               # Division. Geographic division in which the state is located (from 1 thru 9).  Possessions are coded "0".
                                                                                    #    The states comprising each division are as follows.           
                data['year'] = line[14:16]                                          # Year. Last two digits of the year the data reflects, e.g., "85" = 1985, "90" = 1990, etc.
                data['seq_number'] = line[16:21]                                    # Sequence Number. A five-digit number which places all cities in alphabetic order, regardless of state.  
                                                                                    #     This field is blank for groups 0, 8, and 9.
                data['juvenile_age'] = line[21:23]                                  # Juvenile Age. The juvenile age limit in the state in which the agency is located.
                data['core_city'] = line[23]                                        # Core City Indication.    
                data['covered_by'] = check_int(line[24:31])                         # Covered By. The ORI of the agency that submits crime data for the agency represented by the header. 
                                                                                    #     For example, a county will often submit a return which includes the crime data for a city within that county. 
                                                                                    #     This field is blank if the agency is not a "covered-by."   
                data['covered_group'] = check_int(line[31])                         # Covered By Group. This is the group of the "covered-by" ORI above.    
                data['last_update'] = line[32:38]                                   # Last Update. The date the heading or mailing list information was last updated (MMDDYY).   
                data['field_office'] = line[38:42]                                  # Field Office. The four-digit numeric code for the FBI field office whose territory covers the agency.   
                data['months_reported'] = line[42:44]                               # Number of Months Reported. highest "valued" month that was reported for the year by the submitting agency. 
                                                                                    #    For example, if October was the last month submitted or is the only month submitted, "10" would be the value. 
                                                                                    #    This value also can be used to stop processing once October ("10") has been processed. Again, the value does not 
                                                                                    #    mean that there are necessarily 10 full months reported. It does mean that the tenth month is the last month reported 
                                                                                    #    on the return for the year.    
                data['agency_count'] = line[44].strip()                             # Agency Count. Used to accumulate "agencies used" totals in various tabulations. This field is normally "1" but will be "0" 
                                                                                    #     for the U.S. Park Police and all State Police agencies whose ORI code ends in "SP" (or "99" in  California).    
                data['population_city'] = check_int(line[45:54])                    # Population Data - 1 - Population. This is the population of the City in the County below.
                data['county'] = check_int(line[54:57])                             # Population Data - 1 - County. This is the county in which the city is in.
                data['msa'] = check_int(line[57:60])                                # Population Data - 1 - MSA. If present, it is the code of the MSA in which it is located.
                data['population_group_1'] = int(line[60:75])                       # Population Data - 2 - Group. If city resides in two counties, this is the second largest county population.
                data['population_group_2'] = int(line[75:90])                       # Population Data - 3 - Group.   If city resides in three counties, this is the third largest county population.
                data['population_1'] = check_int(line[90:99])                       # Population 1 - Last Census
                data['population_2'] = check_int(line[99:108])                      # Population 2 - Last Census
                data['population_3'] = check_int(line[108:117])                     # Population 3 - Last Census
                                                                                    #    These are the populations taken from the previous census. The fields correspond to the meaning of the previous three 
                                                                                    #    Population Data Groups.     
                data['population_source'] = line[117]                               # Population Source. No documentation exists that reflects the year to year code changes. This was the source for the current 
                                                                                    #    population. This can be from certain commercial publications, from a special census or extra population based on the data 
                                                                                    #    from prior years or from the census. If unused, it should be blank. The code values are 1-9, but meanings of the codes are 
                                                                                    #    not available.   
                data['follow_up_indication'] = follow_up[line[118]]                 # Follow-Up Indication. Periodically all agencies submitting RETURN-A's are checked to see if they have submitted a return for 
                                                                                    #    the preceding months.  If not they are sent a "Follow-Up" return.  This indication is used to show if this particular agency 
                                                                                    #    should be sent a "Follow-Up".   
                data['special_mailing_group'] = smg[line[119]]                      # Special Mailing Group. When addressing RETURN-A's, the tape is sorted by Zip Code so that the forms can be mailed by geographic 
                                                                                    #    area.  Special Mailing Group agencies are excluded   
                data['special_mailing_address'] = sma[line[120]]                    # Special Mailing Address. This indication is used when the first line of the mailing address is other than "Chief of Police" or "Sheriff".   
                data['agency_name'] = line[121:145].strip()                         # Agency Name.   
                data['agency_state'] = line[145:151].strip()                        # Agency State Name.   
                data['agency_address_1'] = remove_escape_chars(line[151:181].strip())   # First Line of Mailing Address.   
                data['agency_address_2'] = remove_escape_chars(line[181:211].strip())   # Second Line of Mailing Address.   
                data['agency_address_3'] = remove_escape_chars(line[211:241].strip())   # Third Line of Mailing Address.   
                data['agency_address_4'] = remove_escape_chars(line[241:271].strip())   # Fourth Line of Mailing Address.   
                data['agency_zip_code'] = remove_escape_chars(line[271:276].strip())    # Zip Code.   
                data['old_population_group'] = line[276]                            # Old Population Group. The population group the agency was in the previous year.   
                # # line[277:306]                                                   # Unused  - Blanks.

                data['key'] = '{}-{}'.format(data['ori_code'], data['year'])
                data['state_full'] =  data['state'].split('-')[1].strip()
                data['state_abbr'] =  data['state'].split('-')[0].strip()

                # BREAK THE MONTHS INTO SUBSTRINGS OF THE ORIGINAL ROW:
                month_lines = {}
                month_lines['01'] = line[306:896]
                month_lines['02'] = line[896:1486]
                month_lines['03'] = line[1486:2076]
                month_lines['04'] = line[2076:2666]
                month_lines['05'] = line[2666:3256]
                month_lines['06'] = line[3256:3846]
                month_lines['07'] = line[3846:4436]
                month_lines['08'] = line[4436:5026]
                month_lines['09'] = line[5026:5616]
                month_lines['10'] = line[5616:6206]
                month_lines['11'] = line[6206:6796]
                month_lines['12'] = line[6796:7386]     

                try:
                    # FOR EACH MONTH SUBSTRING, CREATE THE CARD HEADERS AND CARD SUBSTRINGS:
                    for month, _line in month_lines.items():
                        # The area represented by positions 306 - 895 occurs once for each month for which a return has been received.  If there are any missing months, the corresponding area will be zeros and blanks, depending on field type.  For example, a return for October will always be in the tenth position, July will always be in the seventh, etc. The number of months received is located in Number of Months Reported in positions 42 - 43.

                        t_card_0 = OrderedDict()
                        t_card_1 = OrderedDict()
                        t_card_2 = OrderedDict()
                        t_card_3 = OrderedDict()
                        t_card_4 = OrderedDict()

                        t_card_0['month'] = month
                        t_card_1['month'] = month
                        t_card_2['month'] = month
                        t_card_3['month'] = month
                        t_card_4['month'] = month

                        t_card_0['type'] = card_type[_line[8]]  # Card 0 Type. The type of data in column 3 of the RETURN-A.
                        t_card_1['type'] = card_type[_line[9]]  # Card 1 Type. Same codes as Card 0, except it is for column 4.
                        t_card_2['type'] = card_type[_line[10]] # Card 2 Type. Same codes as Card 0, except it is for column 5.
                        t_card_3['type'] = card_type[_line[11]] # Card 3 Type. Same codes as Card 0, except it is for column 6.
                        t_card_4['type'] = card_type[_line[12]] # Card 4 Type. Same codes as Card 0, except it is for Police Assault information on the RETURN-A. If value is "8", the information is included but is unusable.

                        t_card_0['data'] = card_data[_line[13]] # Card 0 P/T. Shows whether the data for column 3 includes the "breakdown" offenses (P), or shows only the totals (T).  
                                                                #     Field is blank if no return has been received.
                        t_card_1['data'] = card_data[_line[14]] # Card 1 P/T. As noted above but for column 4.
                        t_card_2['data'] = card_data[_line[15]] # Card 2 P/T. As noted above but for column 5.
                        t_card_3['data'] = card_data[_line[16]] # Card 3 P/T. As noted above but for column 6.

                        t_card_4['officers_killed_felonious'] = str_int(_line[577:580])
                        t_card_4['officers_killed_accident'] = str_int(_line[580:583])
                        t_card_4['officers_assaulted'] = str_int(_line[583:590])

                        cards = OrderedDict()
                        cards['unfounded'] = _line[17:157]
                        cards['actual'] = _line[157:297]
                        cards['cleared'] = _line[297:437]
                        cards['under18'] = _line[437:577]

                        card_entries = OrderedDict()
                        
                        for card_name, card_line in cards.items():

                            card_entries[card_name] = {}

                            card_entries[card_name]['{}_month_included'.format(card_name)] = _line[0:2]                        # Month Included In. Used only if an agency does not submit a return, say for January, 
                                                                                                                               #    but indicates on the February return that it includes the January data. In this case, 
                                                                                                                               #    the January area would have "02" in this field with the remainder of the month data 
                                                                                                                               #    initialized to field defaults of zeros and blanks, if applicable.
                            card_entries[card_name]['{}_last_update'.format(card_name)] = _line[2:8]                           # Date of Last Update. The date this month area was updated, in the form of MMDDYY.
                            card_entries[card_name]['{}_murder_total'.format(card_name)] = str_int(card_line[0:5])             # 1 - Murder
                            card_entries[card_name]['{}_manslaughter_total'.format(card_name)] = str_int(card_line[5:10])      # 2 - Manslaughter
                            card_entries[card_name]['{}_rape_total'.format(card_name)] = str_int(card_line[10:15])             # 3 - Rape Total
                            card_entries[card_name]['{}_rape_forcible'.format(card_name)] = str_int(card_line[15:20])          # 4 - Rape by Force  
                            card_entries[card_name]['{}_rape_attempted'.format(card_name)] = str_int(card_line[20:25])         # 5 - Attempted Rape  
                            card_entries[card_name]['{}_robbery_total'.format(card_name)] = str_int(card_line[25:30])          # 6 - Robbery Total  
                            card_entries[card_name]['{}_robbery_wgun'.format(card_name)] = str_int(card_line[30:35])           # 7 - Robbery With A Gun  
                            card_entries[card_name]['{}_robbery_wknife'.format(card_name)] = str_int(card_line[35:40])         # 8 - Robbery With a Knife (only collected 1974 and later, will be zero for any prior years)  
                            card_entries[card_name]['{}_robbery_other'.format(card_name)] = str_int(card_line[40:45])          # 9 - Robbery - Other Weapon (only collected 1974 and later, will be zero for any prior years)  
                            card_entries[card_name]['{}_robbery_strongarm'.format(card_name)] = str_int(card_line[45:50])      # 10 - Strong-Arm Robbery  
                            card_entries[card_name]['{}_assault_total'.format(card_name)] = str_int(card_line[50:55])          # 11 - Assault Total  
                            card_entries[card_name]['{}_assault_wgun'.format(card_name)] = str_int(card_line[55:60])           # 12 - Assault With a Gun  
                            card_entries[card_name]['{}_assault_wknife'.format(card_name)] = str_int(card_line[60:65])         # 13 - Assault With a Knife  
                            card_entries[card_name]['{}_assault_other'.format(card_name)] = str_int(card_line[65:70])          # 14 - Assault - Other Weapon  
                            card_entries[card_name]['{}_assault_whands'.format(card_name)] = str_int(card_line[70:75])         # 15 - Assault With Hands, Feet, etc.  
                            card_entries[card_name]['{}_assault_simple'.format(card_name)] = str_int(card_line[75:80])         # 16 - Simple Assault 
                            card_entries[card_name]['{}_burglary_total'.format(card_name)] = str_int(card_line[80:85])         # 17 - Burglary Total  
                            card_entries[card_name]['{}_burglary_forcible'.format(card_name)] = str_int(card_line[85:90])      # 18 - Burglary - Forcible Entry  
                            card_entries[card_name]['{}_burglary_notforcible'.format(card_name)] = str_int(card_line[90:95])   # 19 - Burglary - No Forcible Entry  
                            card_entries[card_name]['{}_burglary_attempted'.format(card_name)] = str_int(card_line[95:100])    # 20 - Attempted Burglary
                            card_entries[card_name]['{}_larceny_total'.format(card_name)] = str_int(card_line[100:105])        # 21 - Larceny Total
                            card_entries[card_name]['{}_larceny_motor'.format(card_name)] = str_int(card_line[105:110])        # 22 - Motor Vehicle Theft Total
                            card_entries[card_name]['{}_larceny_auto'.format(card_name)] = str_int(card_line[110:115])         # 23 - Auto Theft
                            card_entries[card_name]['{}_larceny_truck'.format(card_name)] = str_int(card_line[115:120])        # 24 - Truck, Bus Theft
                            card_entries[card_name]['{}_larceny_other'.format(card_name)] = str_int(card_line[120:125])        # 25 - Other Vehicle Theft
                            card_entries[card_name]['{}_grand_total'.format(card_name)] = str_int(card_line[125:130])          # 26 - Grand Total of All Fields
                            card_entries[card_name]['{}_larceny_lt_50'.format(card_name)] = str_int(card_line[130:135])        # 27 - Larceny Under $50.00
                            # card_line[135:140]                                                                               # 28-UNUSED

                        t_card_0.update(card_entries['unfounded'])
                        t_card_1.update(card_entries['actual'])
                        t_card_2.update(card_entries['cleared'])
                        t_card_3.update(card_entries['under18'])
                        
                        data.update(t_card_0)
                        data.update(t_card_1)
                        data.update(t_card_2)
                        data.update(t_card_3)
                        data.update(t_card_4)

                except Exception as e1:
                    print e1
                    logging.exception('Details-Error--{}--{}--'.format(f, i))

                row_data.append(data)

            except IndexError as e2:
                print e2
                logging.exception('Index-Error--{}--{}--{}'.format(f, i, line))
                
            except Exception as e3:
                print e3
                logging.exception('Header-Error--{}--{}--'.format(f, i))

        
        column_names = ['key', 'ori_code', 'year', 'group', 'division', 'state', 'state_full', 'state_abbr', 'county', 'seq_number', 'juvenile_age', 'core_city', 'covered_by', 'covered_group', 'field_office', 'last_update', 'months_reported', 'agency_count', 'population_city', 'msa', 'population_group_1', 'population_group_2', 'population_1', 'population_2', 'population_3', 'population_source', 'follow_up_indication', 'special_mailing_address', 'special_mailing_group', 'agency_name', 'agency_state', 'agency_address_1', 'agency_address_2', 'agency_address_3', 'agency_address_4', 'agency_zip_code', 'old_population_group', 'actual_last_update', 'actual_month_included', 'actual_grand_total', 'actual_murder_total', 'actual_manslaughter_total', 'actual_rape_total', 'actual_rape_forcible', 'actual_rape_attempted', 'actual_robbery_total', 'actual_robbery_wgun', 'actual_robbery_wknife', 'actual_robbery_other', 'actual_robbery_strongarm', 'actual_assault_total', 'actual_assault_wgun', 'actual_assault_wknife', 'actual_assault_other', 'actual_assault_whands', 'actual_assault_simple', 'actual_burglary_total', 'actual_burglary_forcible', 'actual_burglary_notforcible', 'actual_burglary_attempted', 'actual_larceny_total', 'actual_larceny_motor', 'actual_larceny_auto', 'actual_larceny_truck', 'actual_larceny_other', 'actual_larceny_lt_50', 'cleared_last_update', 'cleared_month_included', 'cleared_grand_total', 'cleared_murder_total', 'cleared_manslaughter_total', 'cleared_rape_total', 'cleared_rape_forcible', 'cleared_rape_attempted', 'cleared_robbery_total', 'cleared_robbery_wgun', 'cleared_robbery_wknife', 'cleared_robbery_other', 'cleared_robbery_strongarm', 'cleared_assault_total', 'cleared_assault_wgun', 'cleared_assault_wknife', 'cleared_assault_other', 'cleared_assault_whands', 'cleared_assault_simple', 'cleared_burglary_total', 'cleared_burglary_forcible', 'cleared_burglary_notforcible', 'cleared_burglary_attempted', 'cleared_larceny_total', 'cleared_larceny_motor', 'cleared_larceny_auto', 'cleared_larceny_truck', 'cleared_larceny_other', 'cleared_larceny_lt_50', 'unfounded_last_update', 'unfounded_month_included', 'unfounded_grand_total', 'unfounded_murder_total', 'unfounded_manslaughter_total', 'unfounded_rape_total', 'unfounded_rape_forcible', 'unfounded_rape_attempted', 'unfounded_robbery_total', 'unfounded_robbery_wgun', 'unfounded_robbery_wknife', 'unfounded_robbery_other', 'unfounded_robbery_strongarm', 'unfounded_assault_total', 'unfounded_assault_wgun', 'unfounded_assault_wknife', 'unfounded_assault_other', 'unfounded_assault_whands', 'unfounded_assault_simple', 'unfounded_burglary_total', 'unfounded_burglary_forcible', 'unfounded_burglary_notforcible', 'unfounded_burglary_attempted', 'unfounded_larceny_total', 'unfounded_larceny_motor', 'unfounded_larceny_auto', 'unfounded_larceny_truck', 'unfounded_larceny_other', 'unfounded_larceny_lt_50', 'under18_last_update', 'under18_month_included', 'under18_grand_total', 'under18_murder_total', 'under18_manslaughter_total', 'under18_rape_total', 'under18_rape_forcible', 'under18_rape_attempted', 'under18_robbery_total', 'under18_robbery_wgun', 'under18_robbery_wknife', 'under18_robbery_other', 'under18_robbery_strongarm', 'under18_assault_total', 'under18_assault_wgun', 'under18_assault_wknife', 'under18_assault_other', 'under18_assault_whands', 'under18_assault_simple', 'under18_burglary_total', 'under18_burglary_forcible', 'under18_burglary_notforcible', 'under18_burglary_attempted', 'under18_larceny_total', 'under18_larceny_motor', 'under18_larceny_auto', 'under18_larceny_truck', 'under18_larceny_other', 'under18_larceny_lt_50', 'officers_assaulted', 'officers_killed_accident', 'officers_killed_felonious']

        df = pd.DataFrame(row_data, columns=column_names)
        
        filename = f.split('/')[-1].split('.')[0]
        df.to_csv('{}.csv'.format(filename), index=False)

        print '[{}] saved successfully. {}'.format(f, df.shape)


if __name__ == '__main__':
    main()
