# g2audit

## Overview

The [G2Audit.py](G2Audit.py) utility compares two entity resolution result sets and computes the precision, recall and F1 scores between them. It can be used to compare different runs to a 
truth set to determine which one is best or to determine the full affect configuration change had against prior run of the same data.  There are many articles that describe this including ...
* https://senzing.zendesk.com/hc/en-us/articles/360045624093-Understanding-the-G2Audit-statistics
* https://senzing.zendesk.com/hc/en-us/articles/360050643034-Exploratory-Data-Analysis-4-Comparing-ER-results
* https://senzing.zendesk.com/hc/en-us/articles/360051016033-How-to-create-an-entity-resolution-truth-set

This project is designed to be used along with ...
* https://github.com/Senzing/g2snapshot to extract the entity resolution result set from a Senzing database
* https://github.com/Senzing/g2explorer to explore the audit result statistics and examples (requires the data to be loaded into Senzing)

Usage:

```console
python3 G2Audit.py --help
usage: G2Audit.py [-h] [-n NEWERFILE] [-p PRIORFILE] [-o OUTPUTROOT] [-D]

optional arguments:
  -h, --help            show this help message and exit
  -n NEWERFILE, --newer_csv_file NEWERFILE
                        the latest entity map file
  -p PRIORFILE, --prior_csv_file PRIORFILE
                        the prior entity map file
  -o OUTPUTROOT, --output_file_root OUTPUTROOT
                        the ouputfile root name (both a .csv and a .json file
                        will be created)
  -D, --debug           print debug statements
  ```

## Contents

1. [Prerequisites](#Prerequisites)
2. [Installation](#Installation)
3. [Typical use](#Typical-use)
4. [Output files](#Output-files)

### Prerequisites
- python 3.6 or higher

*Plenty of RAM! This process runs very fast as it loads each data set into memory. This is not a problem if your control or truth set is under a million records.  But if you get into the 
10s or 100s of million records, you may need to run this on a computer with enough RAM to load both sets into memory at the same time.*

### Installation

1. Simply place the the following files in a directory of your choice ...  (Ideally along with G2Snapshot.py and G2Explorer.py)
    - [G2Audit.py](G2Audit.py) 

### Typical use

#### For comparing to a truthset to find the best result ...
```console
python3 G2Audit.py -n /path/to/candidate1-result.csv -p /path/to/truthset.csv -o /path/to/audit1-result

python3 G2Audit.py -n /path/to/candidate2-result.csv -p /path/to/truthset.csv -o /path/to/audit2-result
```
You will find the precision, recall and F1 scores in the audit1-result1.json and audit-result2.json files along with examples of entities that have been split or merged.

#### For analyzing the full effect of a configuration change ...
```console
python3 G2Audit.py -n /path/to/v1-config-test1-result.csv -p /path/to/v2-config-test1-result.csv -o /path/to/audit-result-cfg2-cfg1
```
This would determine the effect the version 2 config changes had on the test1 data set compared to the version 1 config result on the same data.  

Configuration updates are usually made to reduce false positives or negatives on specific examples reported by users.  Performing this kind of an audit can help ensure their examples
were corrected without drastically affecting the overall precision and recall scores.

### Output files

#### json statistics file

![Alt text](images/json-file-screenshot.jpg?raw=true "Screen shot")

#### csv statistics file

![Alt text](images/csv-file-screenshot.jpg?raw=true "Screen shot")

* AUDIT_ID groups the records involved in a split or merged entity.
* AUDIT_CATEGORY is either "SPLIT" or "MERGED" or "SPLIT+MERGED" and applies to the group so is the same for every record in the AUDIT_ID.
* AUDIT_RESULT is either "new_positive", "new_negative", or "same" and shows that particular record's role in the audit result.
* DATA_SOURCE and RECORD_ID indicate the specific record.
* PRIOR_ID indicates the unique entity or cluster ID in the prior or truth set.
* PRIOR_SCORE indicates the reported score for this record in relation to the prior entity reported by the prior or truth set.
* NEWER_ID indicates the unique entity or cluster ID in the prior or truth set.
* NEWER_SCORE indicates the reported score for this record in relation to the newer entity reported by the candidate or newer result set.

*The scores are usually only provided on data run through the Senzing software which even provides scores of records that were not matched.
For instance in the screen shot above, line 8 shows that even though the entity was split, there was still a relationship created on name and 
date of birth.*

