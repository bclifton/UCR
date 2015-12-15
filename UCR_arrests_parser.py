#! /usr/bin/env python
#-----------------------------------------------------------------------------------

# Author: https://github.com/bclifton

#-----------------------------------------------------------------------------------
import glob
import logging
import datetime

import pandas as pd
import numpy as np

from sqlalchemy import create_engine

#-----------------------------------------------------------------------------------

logger = logging.getLogger()
hdlr = logging.FileHandler('{}.error.log.arrests'.format(datetime.date.today()))
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)

engine = create_engine('postgresql://localhost/')

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
                return -int(s[:-1] + neg_num[s[-1]], 0)
            else:
                # intentionally returning the error
                # These are most likely data entry errors, for example: '000J4' instead of '0004J'
                return s

def full_year(s):
    last_two = s[-2:]
    if int(last_two) <= 30:
        return int('20{}'.format(last_two))
    else:
        return int('19{}'.format(last_two))

#-----------------------------------------------------------------------------------
# Header Dicts:

state_code = {
'50': 'AK-Alaska',
'01': 'AL-Alabama',
'03': 'AR-Arkansas',
'54': 'AS-American Samoa',
'02': 'AZ-Arizona',
'04': 'CA-California',
'05': 'CO-Colorado',
'06': 'CT-Connecticut',
'52': 'CZ-Canal Zone',
'08': 'DC-District of Columbia',
'07': 'DE-Delaware',
'09': 'FL-Florida',
'10': 'GA-Georgia',
'55': 'GM-Guam',
'51': 'HI-Hawaii',
'14': 'IA-Iowa',
'11': 'ID-Idaho',
'12': 'IL-Illinois',
'13': 'IN-Indiana',
'15': 'KS-Kansas',
'16': 'KY-Kentucky',
'17': 'LA-Louisiana',
'20': 'MA-Massachusetts',
'19': 'MD-Maryland',
'18': 'ME-Maine',
'21': 'MI-Michigan',
'22': 'MN-Minnesota',
'24': 'MO-Missouri',
'23': 'MS-Mississippi',
'25': 'MT-Montana',
'26': 'NB-Nebraska',
'32': 'NC-North Carolina',
'33': 'ND-North Dakota',
'28': 'NH-New Hampshire',
'29': 'NJ-New Jersey',
'30': 'NM-New Mexico',
'27': 'NV-Nevada',
'31': 'NY-New York',
'34': 'OH-Ohio',
'35': 'OK-Oklahoma',
'36': 'OR-Oregon',
'37': 'PA-Pennsylvania',
'53': 'PR-Puerto Rico',
'38': 'RI-Rhode Island',
'39': 'SC-South Carolina',
'40': 'SD-South Dakota',
'41': 'TN-Tennessee',
'42': 'TX-Texas',
'43': 'UT-Utah',
'62': 'VI-Virgin Islands',
'45': 'VA-Virginia',
'44': 'VT-Vermont',
'46': 'WA-Washington',
'48': 'WI-Wisconsin',
'47': 'WV-West Virginia',
'49': 'WY-Wyoming',
' ': '',
'': ''
}

group = {
'0':  'Possessions (Puerto Rico, Guam, Canal Zone, Virgin Islands, and American Samoa)',
'1':  'All cities 250,000 or over:',
'1A': 'Cities 1,000,000 or over',
'1B': 'Cities from 500,000 thru 999,999',
'1C': 'Cities from 250,000 thru 499,999',
'2':  'Cities from 100,000 thru 249,000',
'3':  'Cities from 50,000 thru 99,000',
'4':  'Cities from 25,000 thru 49,999',
'5':  'Cities from 10,000 thru 24,999',
'6':  'Cities from 2,500 thru 9,999',
'7':  'Cities under 2,500',
'8':  'Non-MSA Counties:',
'8A': 'Non-MSA counties 100,000 or over',
'8B': 'Non-MSA counties from 25,000 thru 99,999',
'8C': 'Non-MSA counties from 10,000 thru 24,999',
'8D': 'Non-MSA counties under 10,000',
'8E': 'Non-MSA State Police',
'9':  'MSA Counties:',
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

suburban = {
'1': 'Suburban',
'0': 'Non-Suburban',
' ': '',
'': ''
}

indication = {
'0': 'Juvenile and Adult Reported',
'1': 'Juvenile Only Reported',
'2': 'Adult Only Reported',
'3': 'Not Reported',
' ': '',
'': ''
}

adjustment_h = {
'0': 'Age, Race, and Ethnic Origin Reported',
'1': 'No Age Reported',
'2': 'No Race Reported',
'3': 'No Ethnic Origin Reported',
'4': 'No Race or Ethnic Origin Reported',
'5': 'No Age or Ethnic Origin Reported',
'6': 'No Age or Race Reported',
' ': '',
'': ''
}

core = {
'Y': 'agency is core city of an MSA',
'N': 'agency is not a core city of an MSA',
' ': '',
'': ''
}

#-----------------------------------------------------------------------------------
# Details Dicts:

card1 = {
'0': False,
'1': True}

card2 = {
'0': False,
'1': True}

card3 = {
'0': False,
'1': True}

adjustment_d = {
'0': 'Age, Race, and Ethnic Origin Reported',
'1': 'No Age Reported',
'2': 'No Race Reported',
'3': 'No Ethnic Origin Reported',
'4': 'No Race or Ethnic Origin Reported',
'5': 'No Age or Ethnic Origin Reported',
'6': 'No Age or Race Reported',
' ': '',
'': ''
}

offense_code = {
'011': 'Murder and Non-Negligent Manslaughter',
'012': 'Manslaughter by Negligence',
'020': 'Forcible Rape',
'030': 'Robbery',
'040': 'Aggravated Assault',
'050': 'Burglary - Breaking or Entering',
'060': 'Larceny - Theft  (except motor vehicle)',
'070': 'Motor Vehicle Theft',
'080': 'Other Assaults',
'090': 'Arson',
'100': 'Forgery and Counterfeiting',
'110': 'Fraud',
'120': 'Embezzlement',
'130': 'Stolen property - Buying, Receiving, Poss.',
'140': 'Vandalism',
'150': 'Weapons - Carrying, Possessing, etc.',
'160': 'Prostitution and Commercialized Vice',
'170': 'Sex Offenses (except forcible rape and prostitution)',
'18' : 'Drug Abuse Violations (Total)',
'180': 'Sale/Manufacturing (Subtotal)',
'181': 'Sale/Manufacturing: Opium and Cocaine, and their derivatives (Morphine, Heroin)',
'182': 'Sale/Manufacturing: Marijuana',
'183': 'Sale/Manufacturing: Synthetic Narcotics - Manufactured Narcotics which can cause true drug addiction (Demerol, Methadones)',
'184': 'Sale/Manufacturing: Other Dangerous Non-Narcotic Drugs (Barbiturates, Benzedrine)',
'185': 'Possession (Subtotal)',
'186': 'Possession: Opium and Cocaine, and their derivatives (Morphine, Heroin)',
'187': 'Possession: Marijuana',
'188': 'Possession: Synthetic Narcotics - Manufactured Narcotics which can cause true drug addiction (Demerol, Methadones)',
'189': 'Possession: Other Dangerous Non-Narcotic Drugs (Barbiturates, Benzedrine)',
'190': 'Gambling (Total)',
'191': 'Gambling: Bookmaking (Horse and Sport Book)',
'192': 'Gambling: Number and Lottery',
'193': 'Gambling: All Other Gambling',
'200': 'Offenses Against Family and Children',
'210': 'Driving Under the Influence',
'220': 'Liquor Laws',
'230': 'Drunkenness',
'240': 'Disorderly Conduct',
'250': 'Vagrancy',
'260': 'All Other Offenses (except traffic)',
'270': 'Suspicion',
'280': 'Curfew and Loitering Law Violations',
'290': 'Runaways',
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
    files = glob.glob('arrests/*.txt')

    for f in files:
        print f

        lines = open(f, 'rb').read().splitlines()
        data_headers = []
        data_details = []
        
        for i, line in enumerate(lines):
            line = '_{}'.format(line) # Add a '_' so indexes match the char positions in the summary.
            entry = {}

            try:
                if line[23:26] == '000':
                    # ----------- HEADER ----------- #
                    # identifier = line[1]                                            # Identifier code for the ASR master file
                    entry['state'] = state_code[line[2:4]]                            # Numeric State Code. Range is 01-62.  Data  records are in order by ORI code within numeric state code.
                    entry['ori_code'] = line[4:11]                                    # ORI Code. Originating Agency Identifier.
                    entry['group'] = group[line[11:13].strip()]                       # Group. Group 0 is possessions;  1-7 are cities, 8-9 are counties. All populations are inclusive. 
                    entry['division'] = division[line[13]]                            # Division. Geographic division in which the state is located (from 1 thru 9).  Possessions are coded "0". 
                                                                                      #     The states comprising each division are as follows.  
                    entry['year'] = line[14:16]                                       # Year. Last two digits of the year the data reflects, e.g., "85" = 1985, "90" = 1990, etc.
                    entry['msa'] = line[16:19]                                        # MSA. Metropolitan Statistical Area (MSA) number in which the city is located, if any.  Blank if not used.
                    entry['suburban'] = suburban[line[19]]                            # Suburban. A "suburban" agency is an MSA city with less than 50,000 population (groups 4 - 7) together with MSA counties (group 9).
                    # line[20]                                                        # --Not Used.
                    entry['indication'] = indication[line[21]]                        # Report Indication.
                    entry['adjustment'] = adjustment_h[line[22].replace('\x00', '')]  # Adjustment.
                    entry['offense'] = line[23:26]                                    # Offense. If value is "000", it is the "header" record currently being defined, else it is the "detail" record.
                    # line[26:28]                                                     # --Not Used.    
                    entry['seq_number'] = line[28:33]                                 # Sequence Number. A five-digit number which places all cities in alphabetic order, regardless of state.  
                                                                                      #     This field is blank for groups 0, 8, and 9.
                    entry['county'] = line[33:36].strip()                             # County. Three-digit numeric code for the county in which the agency is located.
                    # line[36:40]                                                     # --Not Used.    
                    entry['core_city'] = core[line[40]]                               # Core City.
                    entry['population'] = int(line[41:51])                            # Current Population. Total population for the agency for the year reported.
                    # line[51:110]                                                    # --Not Used. NOTE:  FBI uses positions 51 through 100 to store the first previous year's population through the fifth 
                                                                                      #     previous year's population.
                    entry['agency_count'] = int(line[110])                            # Agency Count. Used to accumulate "agencies used" totals in various tabulations.  This field is normally "1" but will 
                                                                                      #     be "0" for the U.S. Park Police and all State Police agencies whose ORI code ends in "SP" (or "99" in California).
                    entry['agency_name'] = line[111:136].strip()                      # Agency Name.
                    entry['state_name'] = line[136:142].strip()                       # State Name.
                    # line[142:565]                                                   # --Not Used. NOTE: FBI uses positions 151 - 230 to store the populations from the sixth previous year's population through the thirteenth previous year's population.

                    entry['key'] = '{}-{}'.format(entry['ori_code'], entry['year'])

                    data_headers.append(entry)

                else:    
                    # ----------- DETAIL ----------- #
                    # identifier = line[1]                                # Identifier code for the ASR master file
                    entry['state'] = state_code[line[2:4]]                # Numeric State Code. Range is 01-62.  Data  records are in order by ORI code within numeric state code.
                    entry['ori_code'] = line[4:11]                        # ORI Code. Originating Agency Identifier.
                    entry['group'] = group[line[11:13].strip()]           # Group. Group 0 is possessions;  1-7 are cities, 8-9 are counties. All populations are inclusive. 
                    entry['division'] = division[line[13]]                # Division. Geographic division in which the state is located (from 1 thru 9).  Possessions are coded "0".The states comprising each division are as follows.
                    entry['msa'] = line[16:19]                            # MSA. Metropolitan Statistical Area (MSA) number in which the city is located, if any.  Blank if not used.
                    entry['male_involved'] = card1[line[19]]              # Card 1 Indicator.
                    entry['female_involved'] = card2[line[20]]            # Card 2 Indicator.
                    entry['juvenile_involved'] = card3[line[21]]          # Card 3 Indicator.
                    entry['adjustment'] = adjustment_d[line[22]]          # Adjustment.
                    entry['offense'] = offense_code[line[23:26].strip()]  # Offense.
                    # line[26:40]                                         # Not Used. Set to blanks. FBI uses these positions for internal use as follows:
                    
                    # Male Totals by Age:
                    entry['m_lt_10'] = str_int(line[41:50])          # Under 10
                    entry['m_10_12'] = str_int(line[50:59])          # 10-12
                    entry['m_13_14'] = str_int(line[59:68])          # 13-14
                    entry['m_15'] = str_int(line[68:77])             # 15
                    entry['m_16'] = str_int(line[77:86])             # 16
                    entry['m_17'] = str_int(line[86:95])             # 17
                    entry['m_18'] = str_int(line[95:104])            # 18
                    entry['m_19'] = str_int(line[104:113])           # 19
                    entry['m_20'] = str_int(line[113:122])           # 20
                    entry['m_21'] = str_int(line[122:131])           # 21
                    entry['m_22'] = str_int(line[131:140])           # 22
                    entry['m_23'] = str_int(line[140:149])           # 23
                    entry['m_24'] = str_int(line[149:158])           # 24
                    entry['m_25_29'] = str_int(line[158:167])        # 25-29
                    entry['m_30_34'] = str_int(line[167:176])        # 30-34
                    entry['m_35_39'] = str_int(line[176:185])        # 35-39
                    entry['m_40_44'] = str_int(line[185:194])        # 40-44
                    entry['m_45_49'] = str_int(line[194:203])        # 45-49
                    entry['m_50_54'] = str_int(line[203:212])        # 50-54
                    entry['m_55_59'] = str_int(line[212:221])        # 55-59
                    entry['m_60_64'] = str_int(line[221:230])        # 60-64
                    entry['m_mt_64'] = str_int(line[230:239])        # Over 64   

                    # Female Totals by Age:
                    entry['f_lt_10'] = str_int(line[239:248])        # Under 10
                    entry['f_10_12'] = str_int(line[248:257])        # 10-12
                    entry['f_13_14'] = str_int(line[257:266])        # 13-14
                    entry['f_15'] = str_int(line[266:275])           # 15
                    entry['f_16'] = str_int(line[275:284])           # 16
                    entry['f_17'] = str_int(line[284:293])           # 17
                    entry['f_18'] = str_int(line[293:302])           # 18
                    entry['f_19'] = str_int(line[302:311])           # 19
                    entry['f_20'] = str_int(line[311:320])           # 20
                    entry['f_21'] = str_int(line[320:329])           # 21
                    entry['f_22'] = str_int(line[329:338])           # 22
                    entry['f_23'] = str_int(line[338:347])           # 23
                    entry['f_24'] = str_int(line[347:356])           # 24
                    entry['f_25_29'] = str_int(line[356:365])        # 25-29
                    entry['f_30_34'] = str_int(line[365:374])        # 30-34
                    entry['f_35_39'] = str_int(line[374:383])        # 35-39
                    entry['f_40_44'] = str_int(line[383:392])        # 40-44
                    entry['f_45_49'] = str_int(line[392:401])        # 45-49
                    entry['f_50_54'] = str_int(line[401:410])        # 50-54
                    entry['f_55_59'] = str_int(line[410:419])        # 55-59
                    entry['f_60_64'] = str_int(line[419:428])        # 60-64
                    entry['f_mt_64'] = str_int(line[428:437])        # Over 64

                    # Juvenile Totals by Race and Ethnic Origin.
                    entry['j_white'] = str_int(line[437:446])
                    entry['j_black'] = str_int(line[446:455])
                    entry['j_indian'] = str_int(line[455:464])
                    entry['j_asian'] = str_int(line[464:473])
                    entry['j_hispanic'] = str_int(line[473:482])
                    entry['j_nonhispanic'] = str_int(line[482:491])

                    # Adult Totals by Race and Ethnic Origin.
                    entry['a_white'] = str_int(line[491:500])
                    entry['a_black'] = str_int(line[500:509])
                    entry['a_indian'] = str_int(line[509:518])
                    entry['a_asian'] = str_int(line[518:527])
                    entry['a_hispanic'] = str_int(line[527:536])
                    entry['a_nonhispanic'] = str_int(line[536:545])

                    # line[545:565]                               # Unused.

                    data_details.append(entry)

            except Exception as e:
                logging.exception('--{}--{}--:{}'.format(f, i, line))



        header_columns = ['key','state','ori_code','group','division','year','msa','suburban','indication','adjustment','offense','seq_number','county','core_city','population','agency_count','agency_name','state_name']
        header_df = pd.DataFrame(data_headers, columns=header_columns)

        details_columns = ['state','ori_code','group','division','msa','male_involved','female_involved','juvenile_involved','adjustment','offense','m_lt_10','m_10_12','m_13_14','m_15','m_16','m_17','m_18','m_19','m_20','m_21','m_22','m_23','m_24','m_25_29','m_30_34','m_35_39','m_40_44','m_45_49','m_50_54','m_55_59','m_60_64','m_mt_64','f_lt_10','f_10_12','f_13_14','f_15','f_16','f_17','f_18','f_19','f_20','f_21','f_22','f_23','f_24','f_25_29','f_30_34','f_35_39','f_40_44','f_45_49','f_50_54','f_55_59','f_60_64','f_mt_64','j_white','j_black','j_indian','j_asian','j_hispanic','j_nonhispanic','a_white','a_black','a_indian','a_asian','a_hispanic','a_nonhispanic']
        details_df = pd.DataFrame(data_details, columns=details_columns)


        # Merge the data
        df = pd.merge(header_df, details_df, on=['ori_code'], how='inner')
        df.drop(['state_y','group_y','division_y','msa_y','offense_x', 'adjustment_x', 'state_name'], axis=1, inplace=True)
        df.rename(columns={'offense_y': 'offense',
                           'state_x': 'state',
                           'group_x': 'group', 
                           'division_x': 'division',
                           'msa_x': 'msa',
                           'adjustment_y': 'adjustment'}, inplace=True)

        df['state_full'] =  df['state'].apply(lambda x: x.split('-')[1])
        df['state_abbr'] =  df['state'].apply(lambda x: x.split('-')[0])
        df['year']       =  df['year'].apply(lambda x: full_year(x))
        


        df = df[['key','year','ori_code','state_full','state_abbr','division','county','agency_name','agency_count','seq_number','msa','suburban','core_city','group','population','indication','adjustment','offense','male_involved','female_involved','juvenile_involved','m_lt_10','m_10_12','m_13_14','m_15','m_16','m_17','m_18','m_19','m_20','m_21','m_22','m_23','m_24','m_25_29','m_30_34','m_35_39','m_40_44','m_45_49','m_50_54','m_55_59','m_60_64','m_mt_64','f_lt_10','f_10_12','f_13_14','f_15','f_16','f_17','f_18','f_19','f_20','f_21','f_22','f_23','f_24','f_25_29','f_30_34','f_35_39','f_40_44','f_45_49','f_50_54','f_55_59','f_60_64','f_mt_64','j_white','j_black','j_indian','j_asian','j_hispanic','j_nonhispanic','a_white','a_black','a_indian','a_asian','a_hispanic','a_nonhispanic']]


        # Compress data:
        df['year'] =         df['year'].astype('category')
        df['ori_code'] =     df['ori_code'].astype('category')
        df['state_full'] =   df['state_full'].astype('category')
        df['state_abbr'] =   df['state_abbr'].astype('category')
        df['division'] =     df['division'].astype('category')
        df['county'] =       df['county'].astype('category')
        df['agency_name'] =  df['agency_name'].astype('category')
        df['indication'] =   df['indication'].astype('category')
        df['suburban'] =     df['suburban'].astype('category')
        df['core_city'] =    df['core_city'].astype('category')
        df['group'] =        df['group'].astype('category')
        df['indication'] =   df['indication'].astype('category')
        df['adjustment'] =   df['adjustment'].astype('category')
        df['offense'] =      df['offense'].astype('category')

        filename = f.split('/')[-1].split('.')[0]
        df.to_csv('{}.csv'.format(filename), index=False)

        print '[{}] saved successfully. {}'.format(f, df.shape)


if __name__ == '__main__':
    main()