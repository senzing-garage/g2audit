#! /usr/bin/env python3

import os
import sys
import argparse
import signal
import csv
import json
from datetime import datetime
import time
import random


# ----------------------------------------
def pause(question='PRESS ENTER TO CONTINUE ...'):
    """ pause for debug purposes """
    try:
        input(question)
    except KeyboardInterrupt:
        global shutDown
        shutDown = True
    except:
        pass


# ----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shutDown
    shutDown = True


# ----------------------------------------
def makeKeytable(fileName, tableName):
    print(f'loading {fileName} ...')
    try:
        with open(fileName, 'r') as f:
            headerLine = f.readline()
    except IOError as err:
        print(err)
        return None
    csvDialect = csv.Sniffer().sniff(headerLine)
    columnNames = next(csv.reader([headerLine], dialect=csvDialect))
    columnNames = [x.upper() for x in columnNames]

    fileMap = {}
    fileMap['algorithmName'] = '<name of the algorthm that produced the entity map>'
    fileMap['clusterField'] = '<csvFieldName> for unique ID'
    fileMap['recordField'] = '<csvFieldName> for the record ID'
    fileMap['sourceField'] = '<csvFieldName> for the data source (only required if multiple)'
    fileMap['sourceValue'] = 'hard coded value that matches Senzing data source source'
    fileMap['scoreField'] = '<csvFieldName> for the matching score (optional)'

    if 'RESOLVED_ENTITY_ID' in columnNames and 'DATA_SOURCE' in columnNames and 'RECORD_ID' in columnNames:
        fileMap['algorithmName'] = 'Senzing'
        fileMap['clusterField'] = 'RESOLVED_ENTITY_ID'
        fileMap['recordField'] = 'RECORD_ID'
        fileMap['sourceField'] = 'DATA_SOURCE'
        fileMap['scoreField'] = 'MATCH_KEY'
    elif 'CLUSTER_ID' in columnNames and 'RECORD_ID' in columnNames:
        fileMap['algorithmName'] = 'Other'
        fileMap['clusterField'] = 'CLUSTER_ID'
        fileMap['recordField'] = 'RECORD_ID'
        if 'DATA_SOURCE' in columnNames:
            fileMap['sourceField'] = 'DATA_SOURCE'
        else:
            del fileMap['sourceField']
            print()
            fileMap['sourceValue'] = input('What did you name the data_source? ')
            print()
            if not fileMap['sourceValue']:
                print('Unfortunately a data source name is required. process aborted.')
                print()
                return None
        if 'SCORE' in columnNames:
            fileMap['scoreField'] = 'SCORE'
        else:
            del fileMap['scoreField']
    else:
        if not os.path.exists(fileName + '.map'):
            print('')
            print('please describe the fields for ' + fileName + ' as follows in a file named ' + fileName + '.map')
            print(json.dumps(fileMap, indent=4))
            print('')
            return None
        try:
            fileMap = json.load(open(fileName + '.map'))
        except ValueError as err:
            print('error opening %s' % (fileName + '.map'))
            print(err)
            return None
        if 'clusterField' not in fileMap:
            print('clusterField missing from file map')
            return None
        if 'recordField' not in fileMap:
            print('recordField missing from file map')
            return None
        if 'sourceField' not in fileMap and 'sourceValue' not in fileMap:
            print('either a sourceField or sourceValue must be specified in the file map')
            return None

    fileMap['fileName'] = fileName
    fileMap['tableName'] = tableName
    fileMap['columnHeaders'] = columnNames
    if fileMap['clusterField'] not in fileMap['columnHeaders']:
        print('column %s not in %s' % (fileMap['clusterField'], fileMap['fileName']))
        return 1
    if fileMap['recordField'] not in fileMap['columnHeaders']:
        print('column %s not in %s' % (fileMap['recordField'], fileMap['fileName']))
        return 1

    fileMap['clusters'] = {}
    fileMap['records'] = {}
    fileMap['relationships'] = {}
    nextMissingCluster_id = 0

    with open(fileMap['fileName'], 'r') as csv_file:
        csv_reader = csv.reader(csv_file, dialect=csvDialect)
        next(csv_reader)  # --remove header
        for row in csv_reader:
            rowData = dict(zip(columnNames, row))
            if fileMap['algorithmName'] == 'Senzing' and 'RELATED_ENTITY_ID' in rowData and rowData['RELATED_ENTITY_ID'] != '0':
                ent1str = str(rowData['RESOLVED_ENTITY_ID'])
                ent2str = str(rowData['RELATED_ENTITY_ID'])
                relKey = ent1str + '-' + ent2str if ent1str < ent2str else ent2str + '-' + ent1str
                if relKey not in fileMap['relationships']:
                    fileMap['relationships'][relKey] = rowData['MATCH_KEY']
                continue
            if 'sourceField' in fileMap:
                sourceValue = rowData[fileMap['sourceField']]
            else:
                sourceValue = fileMap['sourceValue']
            if 'scoreField' in fileMap:
                scoreValue = rowData[fileMap['scoreField']]
            else:
                scoreValue = None

            rowData[fileMap['recordField']] = str(rowData[fileMap['recordField']]) + '|DS=' + str(sourceValue)
            if not rowData[fileMap['clusterField']]:
                nextMissingCluster_id += 1
                rowData[fileMap['clusterField']] = '(sic) ' + str(nextMissingCluster_id)
            else:
                rowData[fileMap['clusterField']] = str(rowData[fileMap['clusterField']])
            fileMap['records'][rowData[fileMap['recordField']]] = rowData[fileMap['clusterField']]
            if rowData[fileMap['clusterField']] not in fileMap['clusters']:
                fileMap['clusters'][rowData[fileMap['clusterField']]] = {}
            fileMap['clusters'][rowData[fileMap['clusterField']]][rowData[fileMap['recordField']]] = scoreValue

    return fileMap


def erCompare(fileName1, fileName2, outputRoot):

    # --load the second file into a database table (this is the prior run or prior ground truth)
    priorMap = makeKeytable(fileName2, 'prior')
    if not priorMap:
        return 1

    # --load the first file into a database table (this is the newer run or candidate for adoption)
    newerMap = makeKeytable(fileName1, 'newer')
    if not newerMap:
        return 1

    # --set output files and columns
    outputCsvFile = outputRoot + '.csv'
    outputJsonFile = outputRoot + '.json'
    try:
        csvHandle = open(outputCsvFile, 'w')
    except IOError as err:
        print(err)
        print('could not open output file %s' % outputCsvFile)
        return 1

    csvHeaders = []
    csvHeaders.append('audit_id')
    csvHeaders.append('audit_category')
    csvHeaders.append('audit_result')
    csvHeaders.append('data_source')
    csvHeaders.append('record_id')
    csvHeaders.append('prior_id')
    csvHeaders.append('prior_score')
    csvHeaders.append('newer_id')
    csvHeaders.append('newer_score')
    try:
        csvHandle.write(','.join(csvHeaders) + '\n')
    except IOError as err:
        print(err)
        print('could not write to output file %s' % outputCsvFile)
        return 1
    nextAuditID = 0

    # --initialize stats
    statpack = {}
    statpack['SOURCE'] = 'G2Audit'

    statpack['ENTITY'] = {}
    statpack['ENTITY']['PRIOR_COUNT'] = 0
    statpack['ENTITY']['NEWER_COUNT'] = 0
    statpack['ENTITY']['COMMON_COUNT'] = 0

    statpack['CLUSTERS'] = {}
    statpack['CLUSTERS']['PRIOR_COUNT'] = 0
    statpack['CLUSTERS']['NEWER_COUNT'] = 0
    statpack['CLUSTERS']['COMMON_COUNT'] = 0

    statpack['RECORDS'] = {}
    statpack['RECORDS']['PRIOR_POSITIVE'] = 0
    statpack['RECORDS']['SAME_POSITIVE'] = 0
    statpack['RECORDS']['NEW_POSITIVE'] = 0
    statpack['RECORDS']['NEW_NEGATIVE'] = 0

    statpack['PAIRS'] = {}
    statpack['PAIRS']['NEWER_COUNT'] = 0
    statpack['PAIRS']['PRIOR_COUNT'] = 0
    statpack['PAIRS']['COMMON_COUNT'] = 0

    statpack['AUDIT'] = {}
    statpack['MISSING_RECORD_COUNT'] = 0

    # --go through each cluster in the second file
    batchStartTime = time.time()
    entityCnt = 0
    for priorClusterID in priorMap['clusters']:

        # --progress display
        entityCnt += 1
        if entityCnt % 10000 == 0:
            now = datetime.now().strftime('%I:%M%p').lower()
            eps = int(float(sqlCommitSize) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))
            batchStartTime = time.time()
            print(' %s entities processed at %s, %s per second' % (entityCnt, now, eps))

        # --store the side2 cluster
        statpack['ENTITY']['PRIOR_COUNT'] += 1
        priorRecordIDs = priorMap['clusters'][priorClusterID]
        priorRecordCnt = len(priorRecordIDs)
        if debugOn:
            print('-' * 50)
            print('prior cluster [%s] has %s records (%s)' % (priorClusterID, priorRecordCnt, ','.join(sorted(priorRecordIDs)[:10])))

        # --lookup those records in side1 and see how many clusters they created (ideally one)
        auditRows = []
        missingCnt = 0
        newerRecordCnt = 0
        newerClusterIDs = {}
        for recordID in priorRecordIDs:
            auditData = {}
            auditData['_priorClusterID_'] = priorClusterID
            auditData['_recordID_'] = recordID
            auditData['_priorScore_'] = priorMap['clusters'][priorClusterID][recordID]
            try:
                newerClusterID = newerMap['records'][recordID]
            except:
                missingCnt += 1
                auditData['_auditStatus_'] = 'missing'
                auditData['_newerClusterID_'] = 'unknown'
                auditData['_newerScore_'] = ''
                if debugOn:
                    print('newer run missing record [%s]' % recordID)
            else:
                newerRecordCnt += 1
                auditData['_auditStatus_'] = 'same'  # --default, may get updated later
                auditData['_newerClusterID_'] = newerMap['records'][recordID]
                auditData['_newerScore_'] = newerMap['clusters'][auditData['_newerClusterID_']][recordID]

                if newerMap['records'][recordID] in newerClusterIDs:
                    newerClusterIDs[newerMap['records'][recordID]] += 1
                else:
                    newerClusterIDs[newerMap['records'][recordID]] = 1
            auditRows.append(auditData)
        newerClusterCnt = len(newerClusterIDs)
        statpack['MISSING_RECORD_COUNT'] += missingCnt

        if debugOn:
            print('newer run has those %s records in %s clusters [%s]' % (newerRecordCnt, newerClusterCnt, ','.join(map(str, newerClusterIDs.keys()))))

        # --count as prior positive and see if any new negatives
        largestnewerClusterID = list(newerClusterIDs.keys())[0]
        newNegativeCnt = 0
        if priorRecordCnt > 1:
            statpack['CLUSTERS']['PRIOR_COUNT'] += 1
            statpack['PAIRS']['PRIOR_COUNT'] += ((priorRecordCnt * (priorRecordCnt - 1)) / 2)
            statpack['RECORDS']['PRIOR_POSITIVE'] += priorRecordCnt
            if len(newerClusterIDs) > 1:  # --gonna be some new negatives here

                # --give credit for largest newerCluster
                for clusterID in newerClusterIDs:
                    if newerClusterIDs[clusterID] > newerClusterIDs[largestnewerClusterID]:
                        largestnewerClusterID = clusterID
                statpack['PAIRS']['COMMON_COUNT'] += ((newerClusterIDs[largestnewerClusterID] * (newerClusterIDs[largestnewerClusterID] - 1)) / 2)

                # --mark the smaller clusters as new negatives
                for i in range(len(auditRows)):
                    if auditRows[i]['_newerClusterID_'] != largestnewerClusterID:
                        newNegativeCnt += 1
                        auditRows[i]['_auditStatus_'] = 'new negative'
            else:
                statpack['PAIRS']['COMMON_COUNT'] += ((priorRecordCnt * (priorRecordCnt - 1)) / 2)

        # --now check for new positives in the largest common cluster
        newPositiveCnt = 0
        newerClusterID = largestnewerClusterID
        clusterNewPositiveCnt = 0
        for recordID in newerMap['clusters'][newerClusterID]:
            if recordID not in priorRecordIDs:
                newPositiveCnt += 1
                clusterNewPositiveCnt += 1
                newerRecordCnt += 1
                auditData = {}
                auditData['_recordID_'] = recordID
                auditData['_newerClusterID_'] = newerClusterID
                auditData['_newerScore_'] = newerMap['clusters'][auditData['_newerClusterID_']][recordID]

                # --must lookup the side2 clusterID
                try:
                    priorClusterID2 = priorMap['records'][recordID]
                except:
                    missingCnt += 1
                    auditData['_auditStatus_'] = 'missing'
                    auditData['_priorClusterID_'] = 'unknown'
                    if debugOn:
                        print('side 2 missing record [%s]' % recordID)
                else:
                    auditData['_auditStatus_'] = 'new positive'
                    auditData['_priorClusterID_'] = priorClusterID2
                    auditData['_priorScore_'] = priorMap['clusters'][auditData['_priorClusterID_']][recordID]
                auditRows.append(auditData)

            if clusterNewPositiveCnt > 0:
                if debugOn:
                    print('newer cluster %s has %s more records!' % (newerClusterID, clusterNewPositiveCnt))

        # --if exactly same, note and goto top
        if newerClusterCnt == 1 and newerRecordCnt == priorRecordCnt:
            if debugOn:
                print('RESULT IS SAME!')
            statpack['ENTITY']['COMMON_COUNT'] += 1
            if newerRecordCnt > 1:
                statpack['CLUSTERS']['COMMON_COUNT'] += 1
                statpack['RECORDS']['SAME_POSITIVE'] += newerRecordCnt
            continue

        # --log it to the proper categories
        auditCategory = ''
        if missingCnt:
            auditCategory += '+MISSING'
        if newerClusterCnt > 1:
            auditCategory += '+SPLIT'
        if newerRecordCnt > priorRecordCnt:
            auditCategory += '+MERGE'
        if not auditCategory:
            auditCategory = '+UNKNOWN'
        auditCategory = auditCategory[1:] if auditCategory else auditCategory

        # --only count if current side2 cluster is largest merged
        largerClusterID = None
        lowerClusterID = None
        if 'MERGE' in auditCategory:
            priorClusterCounts = {}
            for auditData in auditRows:
                if auditData['_priorClusterID_'] not in priorClusterCounts:
                    priorClusterCounts[auditData['_priorClusterID_']] = 1
                else:
                    priorClusterCounts[auditData['_priorClusterID_']] += 1

            for clusterID in priorClusterCounts:
                if priorClusterCounts[clusterID] > priorClusterCounts[priorClusterID]:
                    largerClusterID = clusterID
                    break
                if priorClusterCounts[clusterID] == priorClusterCounts[priorClusterID] and clusterID < priorClusterID:
                    lowerClusterID = clusterID

            if debugOn:
                if largerClusterID:
                    print('largerClusterID found! %s' % largerClusterID)
                elif lowerClusterID:
                    print('lowerClusterID if equal size found! %s' % lowerClusterID)

        # --if the largest audit status is not same, wait for the largest to show up
        if largerClusterID or lowerClusterID:
            if debugOn:
                print('AUDIT RESULT BYPASSED!')
                pause()
            continue
        if debugOn:
            print('AUDIT RESULT WILL BE COUNTED!')

        # --initialize audit category
        if auditCategory not in statpack['AUDIT']:
            statpack['AUDIT'][auditCategory] = {}
            statpack['AUDIT'][auditCategory]['COUNT'] = 0
            statpack['AUDIT'][auditCategory]['SUB_CATEGORY'] = {}

        # --adjust the newerScore (match key for senzing)
        clarifyScores = True
        if clarifyScores:

            same_newerClusterID = 0
            newerMatchKeys = {}
            for i in range(len(auditRows)):
                if auditRows[i]['_auditStatus_'] == 'same':
                    same_newerClusterID = auditRows[i]['_newerClusterID_']
                if auditRows[i]['_newerScore_']:
                    if auditRows[i]['_newerClusterID_'] not in newerMatchKeys:
                        newerMatchKeys[auditRows[i]['_newerClusterID_']] = {auditRows[i]['_newerScore_']: True}
                    else:
                        newerMatchKeys[auditRows[i]['_newerClusterID_']][auditRows[i]['_newerScore_']] = True

            # --adjust the new positives/negatives
            for i in range(len(auditRows)):
                # --clear the scores on the records that are the same
                if auditRows[i]['_auditStatus_'] == 'same':
                    auditRows[i]['_priorScore_'] = ''
                    auditRows[i]['_newerScore_'] = ''
                # --use the relationship to see how split rows are related
                elif auditRows[i]['_auditStatus_'] == 'new negative':
                    ent1str = same_newerClusterID
                    ent2str = auditRows[i]['_newerClusterID_']
                    relKey = ent1str + '-' + ent2str if ent1str < ent2str else ent2str + '-' + ent1str
                    if relKey in newerMap['relationships']:
                        auditRows[i]['_newerScore_'] = 'related on: ' + newerMap['relationships'][relKey]
                    else:
                        auditRows[i]['_newerScore_'] = 'not related'
                # -- use the record level match_key
                elif auditRows[i]['_auditStatus_'] == 'new positive':
                    #if not auditRows[i]['_newerScore_']:  # --maybe statisize this
                    if len(newerMatchKeys[auditRows[i]['_newerClusterID_']]) == 1:
                        auditRows[i]['_newerScore_'] = list(newerMatchKeys[auditRows[i]['_newerClusterID_']].keys())[0]
                    else:
                        auditRows[i]['_newerScore_'] = 'multiple'

        # --write the record
        scoreCounts = {}
        statpack['AUDIT'][auditCategory]['COUNT'] += 1
        nextAuditID += 1
        sampleRows = []
        for auditData in auditRows:
            csvRow = []
            csvRow.append(nextAuditID)
            csvRow.append(auditCategory)
            csvRow.append(auditData['_auditStatus_'])
            recordIDsplit = auditData['_recordID_'].split('|DS=')
            auditData['_dataSource_'] = recordIDsplit[1]
            auditData['_recordID_'] = recordIDsplit[0]
            csvRow.append(auditData['_dataSource_'])
            csvRow.append(auditData['_recordID_'])
            csvRow.append(auditData['_priorClusterID_'])
            csvRow.append(auditData['_priorScore_'] if '_priorScore_' in auditData else '')
            csvRow.append(auditData['_newerClusterID_'])
            csvRow.append(auditData['_newerScore_'] if '_newerScore_' in auditData else '')
            if auditData['_auditStatus_'] == 'new negative':
                statpack['RECORDS']['NEW_NEGATIVE'] += 1
            elif auditData['_auditStatus_'] == 'new positive':
                statpack['RECORDS']['NEW_POSITIVE'] += 1
            elif auditData['_auditStatus_'] == 'same':
                statpack['RECORDS']['SAME_POSITIVE'] += 1
            if auditData['_auditStatus_'] in ('new negative', 'new positive') and auditData['_newerScore_']:
                if auditData['_newerScore_'] not in scoreCounts:
                    scoreCounts[auditData['_newerScore_']] = 1
                else:
                    scoreCounts[auditData['_newerScore_']] += 1
            if debugOn:
                print(auditData)
            sampleRows.append(dict(zip(csvHeaders, csvRow)))

            try:
                csvHandle.write(','.join(map(str, csvRow)) + '\n')
            except IOError as err:
                print(err)
                print('could not write to output file %s' % outputCsvFile)
                return 1

        # --assign the best score (most used)
        use_best = False
        if use_best:
            bestScore = 'none'
            bestCount = 0
            for score in scoreCounts:
                if scoreCounts[score] > bestCount:
                    bestScore = score
                    bestCount = scoreCounts[score]
        else: # just say multiple if more than one
            if len(scoreCounts) == 0:
                bestScore = 'none'
            elif len(scoreCounts) == 1:
                bestScore = list(scoreCounts.keys())[0]
            else:
                bestScore = 'multiple'

        # --initialize sub category
        if bestScore not in statpack['AUDIT'][auditCategory]['SUB_CATEGORY']:
            statpack['AUDIT'][auditCategory]['SUB_CATEGORY'][bestScore] = {}
            statpack['AUDIT'][auditCategory]['SUB_CATEGORY'][bestScore]['COUNT'] = 0
            statpack['AUDIT'][auditCategory]['SUB_CATEGORY'][bestScore]['SAMPLE'] = []
        statpack['AUDIT'][auditCategory]['SUB_CATEGORY'][bestScore]['COUNT'] += 1

        # --place in the sample list
        if len(statpack['AUDIT'][auditCategory]['SUB_CATEGORY'][bestScore]['SAMPLE']) < 500:
            statpack['AUDIT'][auditCategory]['SUB_CATEGORY'][bestScore]['SAMPLE'].append(sampleRows)
        else:
            randomSampleI = random.randint(1, 499)
            if randomSampleI % 10 != 0:
                statpack['AUDIT'][auditCategory]['SUB_CATEGORY'][bestScore]['SAMPLE'][randomSampleI] = sampleRows

        if debugOn:
            pause()

    csvHandle.close()

    # --completion display
    now = datetime.now().strftime('%I:%M%p').lower()
    eps = int(float(sqlCommitSize) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))
    batchStartTime = time.time()
    print(' %s entities processed at %s, %s per second, complete!' % (entityCnt, now, eps))

    # --compute the side 1 (result set) cluster and pair count
    print('computing statistics ...')

    # --get all cluster counts for both sides

    # --get cluster and pair counts for side1
    for newerClusterID in newerMap['clusters']:
        statpack['ENTITY']['NEWER_COUNT'] += 1
        newerRecordCnt = len(newerMap['clusters'][newerClusterID])
        if newerRecordCnt == 1:
            continue
        statpack['CLUSTERS']['NEWER_COUNT'] += 1
        statpack['PAIRS']['NEWER_COUNT'] += ((newerRecordCnt * (newerRecordCnt - 1)) / 2)

    # --entity precision and recall
    statpack['ENTITY']['PRECISION'] = 0
    statpack['ENTITY']['RECALL'] = 0
    statpack['ENTITY']['F1-SCORE'] = 0
    if statpack['ENTITY']['NEWER_COUNT'] and statpack['ENTITY']['PRIOR_COUNT']:
        statpack['ENTITY']['PRECISION'] = round((statpack['ENTITY']['COMMON_COUNT'] + .0) / (statpack['ENTITY']['NEWER_COUNT'] + .0), 5)
        statpack['ENTITY']['RECALL'] = round(statpack['ENTITY']['COMMON_COUNT'] / (statpack['ENTITY']['PRIOR_COUNT'] + .0), 5)
        if (statpack['ENTITY']['PRECISION'] + statpack['ENTITY']['RECALL']) != 0:
            statpack['ENTITY']['F1-SCORE'] = round(2 * ((statpack['ENTITY']['PRECISION'] * statpack['ENTITY']['RECALL']) / (statpack['ENTITY']['PRECISION'] + statpack['ENTITY']['RECALL'] + .0)), 5)

    # --cluster precision and recall
    statpack['CLUSTERS']['PRECISION'] = 0
    statpack['CLUSTERS']['RECALL'] = 0
    statpack['CLUSTERS']['F1-SCORE'] = 0
    if statpack['CLUSTERS']['NEWER_COUNT'] and statpack['CLUSTERS']['PRIOR_COUNT']:
        statpack['CLUSTERS']['PRECISION'] = round((statpack['CLUSTERS']['COMMON_COUNT'] + .0) / (statpack['CLUSTERS']['NEWER_COUNT'] + .0), 5)
        statpack['CLUSTERS']['RECALL'] = round(statpack['CLUSTERS']['COMMON_COUNT'] / (statpack['CLUSTERS']['PRIOR_COUNT'] + .0), 5)
        if (statpack['CLUSTERS']['PRECISION'] + statpack['CLUSTERS']['RECALL']) != 0:
            statpack['CLUSTERS']['F1-SCORE'] = round(2 * ((statpack['CLUSTERS']['PRECISION'] * statpack['CLUSTERS']['RECALL']) / (statpack['CLUSTERS']['PRECISION'] + statpack['CLUSTERS']['RECALL'] + .0)), 5)

    # --pairs precision and recall
    statpack['PAIRS']['SAME_POSITIVE'] = statpack['PAIRS']['COMMON_COUNT']
    statpack['PAIRS']['NEW_POSITIVE'] = statpack['PAIRS']['NEWER_COUNT'] - statpack['PAIRS']['COMMON_COUNT'] if statpack['PAIRS']['NEWER_COUNT'] > statpack['PAIRS']['COMMON_COUNT'] else 0
    statpack['PAIRS']['NEW_NEGATIVE'] = statpack['PAIRS']['PRIOR_COUNT'] - statpack['PAIRS']['COMMON_COUNT'] if statpack['PAIRS']['PRIOR_COUNT'] > statpack['PAIRS']['COMMON_COUNT'] else 0

    statpack['PAIRS']['PRECISION'] = 0
    statpack['PAIRS']['RECALL'] = 0
    statpack['PAIRS']['F1-SCORE'] = 0
    if statpack['PAIRS']['SAME_POSITIVE']:
        statpack['PAIRS']['PRECISION'] = round(statpack['PAIRS']['SAME_POSITIVE'] / (statpack['PAIRS']['SAME_POSITIVE'] + statpack['PAIRS']['NEW_POSITIVE'] + .0), 5)
        statpack['PAIRS']['RECALL'] = round(statpack['PAIRS']['SAME_POSITIVE'] / (statpack['PAIRS']['SAME_POSITIVE'] + statpack['PAIRS']['NEW_NEGATIVE'] + .0), 5)
        if (statpack['PAIRS']['PRECISION'] + statpack['PAIRS']['RECALL']) != 0:
            statpack['PAIRS']['F1-SCORE'] = round(2 * ((statpack['PAIRS']['PRECISION'] * statpack['PAIRS']['RECALL']) / (statpack['PAIRS']['PRECISION'] + statpack['PAIRS']['RECALL'] + .0)), 5)

    # --accuracy precision and recall
    statpack['RECORDS']['PRECISION'] = 0
    statpack['RECORDS']['RECALL'] = 0
    statpack['RECORDS']['F1-SCORE'] = 0
    if statpack['RECORDS']['SAME_POSITIVE']:
        statpack['RECORDS']['PRECISION'] = round(statpack['RECORDS']['SAME_POSITIVE'] / (statpack['RECORDS']['SAME_POSITIVE'] + statpack['RECORDS']['NEW_POSITIVE'] + .0), 5)
        statpack['RECORDS']['RECALL'] = round(statpack['RECORDS']['SAME_POSITIVE'] / (statpack['RECORDS']['SAME_POSITIVE'] + statpack['RECORDS']['NEW_NEGATIVE'] + .0), 5)
        if (statpack['RECORDS']['PRECISION'] + statpack['RECORDS']['RECALL']) != 0:
            statpack['RECORDS']['F1-SCORE'] = round(2 * ((statpack['RECORDS']['PRECISION'] * statpack['RECORDS']['RECALL']) / (statpack['RECORDS']['PRECISION'] + statpack['RECORDS']['RECALL'] + .0)), 5)

    # --dump the stats to screen and file
    with open(outputJsonFile, 'w') as outfile:
        json.dump(statpack, outfile)

    # print ('')
    # print ('%s prior positives ' % statpack['RECORDS']['PRIOR_POSITIVE'])
    # print ('%s same positives ' % statpack['RECORDS']['SAME_POSITIVE'])

    # print ('%s new positives ' % statpack['RECORDS']['NEW_POSITIVE'])
    # print ('%s new negatives ' % statpack['RECORDS']['NEW_NEGATIVE'])
    # print ('%s precision ' % statpack['RECORDS']['PRECISION'])
    # print ('%s recall ' % statpack['RECORDS']['RECALL'])
    # print ('%s f1-score ' % statpack['RECORDS']['F1-SCORE'])
    print ('')
    print ('%s prior pairs ' % statpack['PAIRS']['PRIOR_COUNT'])
    print ('%s newer pairs ' % statpack['PAIRS']['NEWER_COUNT'])
    print ('%s common pairs ' % statpack['PAIRS']['COMMON_COUNT'])
    print ('')
    print ('%s same positives ' % statpack['PAIRS']['SAME_POSITIVE'])
    print ('%s new positives ' % statpack['PAIRS']['NEW_POSITIVE'])
    print ('%s new negatives ' % statpack['PAIRS']['NEW_NEGATIVE'])
    print ('%s precision ' % statpack['PAIRS']['PRECISION'])
    print ('%s recall ' % statpack['PAIRS']['RECALL'])
    print ('%s f1-score ' % statpack['PAIRS']['F1-SCORE'])
    print ('')

    print ('%s prior entities ' % statpack['ENTITY']['PRIOR_COUNT'])
    print ('%s new entities ' % statpack['ENTITY']['NEWER_COUNT'])
    print ('%s common entities ' % statpack['ENTITY']['COMMON_COUNT'])
    print ('%s merged entities ' % (statpack['AUDIT']['MERGE']['COUNT'] if 'MERGE' in statpack['AUDIT'] else 0))
    print ('%s split entities ' % (statpack['AUDIT']['SPLIT']['COUNT'] if 'SPLIT' in statpack['AUDIT'] else 0))
    print ('%s split+merge entities ' % (statpack['AUDIT']['SPLIT+MERGE']['COUNT'] if 'SPLIT+MERGE' in statpack['AUDIT'] else 0))
    print ('')
    if statpack['MISSING_RECORD_COUNT']:
        print ('%s ** missing clusters **' % statpack['MISSING_RECORD_COUNT'])
        print('')
    if shutDown:
        print('** process was aborted **')
    else:
        print('process completed successfully!')
    print('')
    return 0


# ===== The main function =====
if __name__ == '__main__':
    shutDown = False
    signal.signal(signal.SIGINT, signal_handler)
    procStartTime = time.time()

    sqlCommitSize = 10000  # -this is really just for stat display

    # --capture the command line arguments
    argParser = argparse.ArgumentParser()
    argParser.add_argument('-n', '--newer_csv_file', dest='newerFile', default=None, help='the latest entity map file')
    argParser.add_argument('-p', '--prior_csv_file', dest='priorFile', default=None, help='the prior entity map file')
    argParser.add_argument('-o', '--output_file_root', dest='outputRoot', default=None, help='the ouputfile root name (both a .csv and a .json file will be created')
    argParser.add_argument('-D', '--debug', dest='debug', action='store_true', default=False, help='print debug statements')
    args = argParser.parse_args()
    newerFile = args.newerFile
    priorFile = args.priorFile
    outputRoot = args.outputRoot
    debugOn = args.debug

    print()
    err_cnt = 0
    if not newerFile:
        print('ERROR: A newer csv file must be specified with -n')
        err_cnt += 1
    elif not os.path.exists(newerFile):
        print('ERROR: The newer csv file was not found!')
        err_cnt += 1

    if not priorFile:
        print('ERROR: A prior csv file must be specified with -p')
        err_cnt += 1
    elif not os.path.exists(priorFile):
        print('ERROR: The prior csv file was not found!')
        err_cnt += 1

    if not outputRoot:
        print('ERROR: An output root must be specified with -o')
        err_cnt += 1
    elif os.path.splitext(outputRoot)[1]:
        print("Please don't use a file extension as both a .json and a .csv file will be created")
        err_cnt += 1

    try:
        csvHandle = open(outputRoot + '.json', 'w')
    except IOError as err:
        print(f"ERROR: Opening output file: {err}")
        err_cnt += 1

    if err_cnt:
        print()
    else:
        err_cnt = erCompare(newerFile, priorFile, outputRoot)


    sys.exit(err_cnt)
