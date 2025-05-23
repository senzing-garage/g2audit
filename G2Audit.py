#! /usr/bin/env python3

import os
import sys
import argparse
import csv
import json
import time
import random
from itertools import groupby
from operator import itemgetter
import logging
import textwrap


def detect_column_names(field_names):
    if "RESOLVED_ENTITY_ID" in field_names:
        cluster_field, source_field, record_field, score_field = (
            "RESOLVED_ENTITY_ID",
            "DATA_SOURCE",
            "RECORD_ID",
            "MATCH_KEY",
        )
    else:
        cluster_field = source_field = record_field = score_field = None
        for field_name in field_names:
            if field_name.upper() in ("ENTITY_ID", "CLUSTER_ID"):
                cluster_field = field_name
            elif field_name.upper() == "DATA_SOURCE":
                source_field = field_name
            elif field_name.upper() == "RECORD_ID":
                record_field = field_name
            elif field_name.upper() == "SCORE":
                score_field = field_name
        if not cluster_field or not source_field or not record_field:
            raise Exception(
                f"Expected fields missing for {file_name}, need at least ENTITY_ID, DATA_SOURCE and RECORD_ID"
            )
    return cluster_field, source_field, record_field, score_field


def load_from_file(file_name, file_type):
    logging.info(f"loading {file_name} ...")
    file_map = {"entities": {}, "records": {}, "relations": {}}
    with open(file_name, "r") as f:
        reader = csv.DictReader(f)
        cluster_field, source_field, record_field, score_field = detect_column_names(
            reader.fieldnames
        )
        progress_cntr = 0
        for record in reader:
            progress_cntr = progress_display(
                progress_cntr, f"{file_type} records loaded", interval=100000
            )
            entity_id = str(record[cluster_field])
            if entity_id not in file_map["entities"]:
                file_map["entities"][entity_id] = {}
            if record.get("RELATED_ENTITY_ID", "0") == "0":
                record_key = compute_record_key(
                    record, cluster_field, source_field, record_field, score_field
                )
                file_map["entities"][entity_id][record_key] = record.get(
                    score_field, ""
                )
                file_map["records"][record_key] = entity_id
            elif file_type == "newer":  # don't need relationships for prior
                rel_key = "|".join(
                    sorted([record["RESOLVED_ENTITY_ID"], record["RELATED_ENTITY_ID"]])
                )
                if rel_key not in file_map["relations"]:
                    file_map["relations"][rel_key] = record.get(score_field, "")
        progress_cntr = progress_display(progress_cntr, "records loaded, complete")
    return file_map


def audit(file_name1, file_name2, output_root, debug):
    try:
        newer_map = load_from_file(file_name1, "newer")
        prior_map = load_from_file(file_name2, "prior")
    except Exception as err:
        logging.error(f"{err} loading files")
        return 1

    csv_file_name = output_root + ".csv"
    json_file_name = output_root + ".json"
    csv_headers = [
        "audit_id",
        "audit_category",
        "audit_result",
        "data_source",
        "record_id",
        "prior_id",
        "prior_score",
        "newer_id",
        "newer_score",
    ]
    try:
        csv_handle = open(csv_file_name, "w")
        csv_writer = csv.writer(csv_handle)
        csv_writer.writerow(csv_headers)
    except Exception as err:
        logging.error(f"{err} opening {csv_file_name}")
        return 1

    newer_pair_count = 0
    prior_entities = {}
    prior_pair_count = 0
    common_entity_count = 0
    common_pair_count = 0
    missing_prior_record_cnt = 0
    missing_newer_record_cnt = 0
    next_audit_id = 0
    audit_stats = {}

    logging.info("auditing newer entities ...")
    progress_cntr = 0
    for newer_entity_id in newer_map["entities"]:
        progress_cntr = progress_display(progress_cntr, "newer entities audited")
        logging.debug("-" * 50)
        logging.debug(
            f"newer entity {newer_entity_id} has {len(newer_map['entities'][newer_entity_id])} records"
        )
        prior_entity_ids = {}
        newer_keys_found = {}
        any_missing = False
        missing_cnt = 0
        for newer_key in newer_map["entities"][newer_entity_id]:
            prior_entity_id = prior_map["records"].get(newer_key, "unknown")
            if prior_entity_id != "unknown":
                newer_keys_found[newer_key] = prior_entity_id
                prior_entity_ids = count_by_key(prior_entity_ids, prior_entity_id)
            else:
                missing_cnt += 1
        newer_pair_count += len(newer_keys_found) * (len(newer_keys_found) - 1) / 2

        if missing_cnt:
            logging.debug(f"prior set is missing {missing_cnt} records!")
            missing_prior_record_cnt += missing_cnt
            any_missing = True
            if len(newer_keys_found) == 0:
                logging.debug(
                    "skipping as prior set does not have any of the newer records!"
                )
                continue

        prior_entity_id = "unknown"
        for entity_id in prior_entity_ids:  # choose the largest matching entity
            logging.debug(
                f"prior entity {entity_id} has {prior_entity_ids[entity_id]} of those records, plus {len(prior_map['entities'][entity_id])-prior_entity_ids[entity_id]} more"
            )
            if prior_entity_ids[entity_id] > prior_entity_ids.get(prior_entity_id, 0):
                prior_entity_id = entity_id
            elif (
                prior_entity_ids[entity_id] == prior_entity_ids.get(prior_entity_id, 0)
                and entity_id < prior_entity_id
            ):
                prior_entity_id = entity_id
        if len(prior_entity_ids) > 1:
            logging.debug(
                f"prior entity {prior_entity_id} selected as it has the most matching records or is the lowest entity_id!"
            )

        same_cnt = new_pos_cnt = 0
        audit_records = []
        for newer_key in newer_map["entities"][newer_entity_id]:
            data_source, record_id = parse_record_key(newer_key)
            audit_record = {
                "data_source": data_source,
                "record_id": record_id,
                "record_key": newer_key,
                "newer_id": newer_entity_id,
                "newer_score": newer_map["entities"][newer_entity_id][newer_key],
                "prior_id": newer_keys_found.get(newer_key, "unknown"),
                "prior_score": "",
            }
            if audit_record["prior_id"] == prior_entity_id:
                audit_record["audit_result"] = "same"
                audit_record["prior_score"] = prior_map["entities"][prior_entity_id][
                    newer_key
                ]
                same_cnt += 1
            elif audit_record["prior_id"] != "unknown":
                audit_record["audit_result"] = "new positive"
                new_pos_cnt += 1
            else:
                audit_record["audit_result"] = "missing"
            audit_records.append(audit_record)

        missing_cnt = 0
        new_neg_cnt = 0
        newer_entity_ids = {}
        for prior_key in prior_map["entities"].get(prior_entity_id, []):
            newer_entity_id2 = newer_map["records"].get(prior_key, "unknown")
            if prior_key not in newer_map["entities"][newer_entity_id]:
                data_source, record_id = parse_record_key(prior_key)
                audit_record = {
                    "data_source": data_source,
                    "record_id": record_id,
                    "record_key": prior_key,
                    "newer_id": newer_entity_id2,
                    "newer_score": "",  # will be replaced by relationship match_key later
                    "audit_result": (
                        "new negative" if newer_entity_id2 != "unknown" else "missing"
                    ),
                    "prior_id": prior_entity_id,
                    "prior_score": prior_map["entities"][prior_entity_id][prior_key],
                }
                if audit_record["audit_result"] == "new negative":
                    new_neg_cnt += 1
                else:
                    missing_cnt += 1
                audit_records.append(audit_record)

            if newer_entity_id2 != "unknown":
                newer_entity_ids = count_by_key(newer_entity_ids, newer_entity_id2)

        if prior_entity_id not in prior_entities:
            prior_entities[prior_entity_id] = True
            prior_entity_record_count = (
                len(prior_map["entities"].get(prior_entity_id, [])) - missing_cnt
            )
            prior_pair_count += (
                prior_entity_record_count * (prior_entity_record_count - 1) / 2
            )

        if missing_cnt:
            logging.debug(f"newer set is missing {missing_cnt} records!")
            missing_newer_record_cnt += missing_cnt
            any_missing = True

        # always get credit for same pairs
        common_pair_count += same_cnt * (same_cnt - 1) / 2

        # skip entity reporting if same
        if new_pos_cnt + new_neg_cnt == 0 and not any_missing:
            common_entity_count += 1
            logging.debug("skipping as result is same!")
            continue

        # skip if another newer entity has more matching records in the prior
        if len(newer_entity_ids) > 1:
            best_newer_entity_id = newer_entity_id
            for newer_entity_id2 in newer_entity_ids:
                if (
                    newer_entity_ids[newer_entity_id2]
                    > newer_entity_ids[best_newer_entity_id]
                ):
                    best_newer_entity_id = newer_entity_id2
                    logging.debug(
                        f"oops, newer entity id {best_newer_entity_id} has {newer_entity_ids[best_newer_entity_id]} matching records for prior_entity {prior_entity_id}"
                    )
                elif (
                    newer_entity_ids[newer_entity_id2]
                    == newer_entity_ids[best_newer_entity_id]
                    and newer_entity_id2 < newer_entity_id
                ):
                    best_newer_entity_id = newer_entity_id2
                    logging.debug(
                        f"oops, newer entity id {best_newer_entity_id} has the same number of matching records for prior_entity {prior_entity_id} and is a lower ID!"
                    )
                    break
            if best_newer_entity_id != newer_entity_id:
                logging.debug(
                    f"skipping as {best_newer_entity_id} is a better match for the selected prior entity!"
                )
                continue

        logging.debug(
            f"logging prior entity {prior_entity_id} with {new_pos_cnt} new positives and {new_neg_cnt} new negatives"
        )

        # log it to the proper categories
        audit_category = ""
        if any_missing:
            audit_category += "+MISSING"
        if new_neg_cnt:
            audit_category += "+SPLIT"
        if new_pos_cnt:
            audit_category += "+MERGE"
        if not audit_category:
            audit_category = "+UNKNOWN"
        audit_category = audit_category[1:]

        if audit_category not in audit_stats:
            audit_stats[audit_category] = {}
            audit_stats[audit_category]["COUNT"] = 0
            audit_stats[audit_category]["SUB_CATEGORY"] = {}
        audit_stats[audit_category]["COUNT"] += 1
        next_audit_id += 1

        newer_match_keys = {}
        for audit_record in audit_records:
            newer_match_keys = list_by_key(
                newer_match_keys, audit_record["newer_id"], audit_record["newer_score"]
            )

        score_counts = {}
        csv_rows = []
        for audit_record in audit_records:
            if audit_record["audit_result"] == "same":
                audit_record["prior_score"] = ""
                audit_record["newer_score"] = ""
            elif (
                audit_record["audit_result"] == "new negative"
            ):  # use relationship score
                rel_key = "|".join(sorted([newer_entity_id, audit_record["newer_id"]]))
                if rel_key in newer_map["relations"]:
                    audit_record["newer_score"] = "related on: " + newer_map[
                        "relations"
                    ].get(rel_key, "unspecified")
                else:
                    audit_record["newer_score"] = "not related"
            elif (
                audit_record["audit_result"] == "new positive"
                and not audit_record["newer_score"]
            ):
                if len(newer_match_keys.get(audit_record["newer_id"], [])) == 1:
                    audit_record["newer_score"] = newer_match_keys[
                        audit_record["newer_id"]
                    ][0]
                else:
                    audit_record["newer_score"] = "multiple"
            score_counts = count_by_key(score_counts, audit_record["newer_score"])

            csv_rows.append(
                [
                    next_audit_id,
                    audit_category,
                    audit_record["audit_result"],
                    audit_record["data_source"],
                    audit_record["record_id"],
                    audit_record["prior_id"],
                    audit_record["prior_score"],
                    audit_record["newer_id"],
                    audit_record["newer_score"],
                ]
            )
            logging.debug(csv_rows[-1])

        csv_writer.writerows(csv_rows)

        audit_sample = [dict(zip(csv_headers, csv_row)) for csv_row in csv_rows]

        if len(score_counts) == 0:
            best_score = "none"
        elif len(score_counts) == 1:
            best_score = list(score_counts.keys())[0]
        else:
            best_score = "multiple"
        logging.debug(f"{audit_category} sub category assigned is {best_score}")

        if best_score not in audit_stats[audit_category]["SUB_CATEGORY"]:
            audit_stats[audit_category]["SUB_CATEGORY"][best_score] = {}
            audit_stats[audit_category]["SUB_CATEGORY"][best_score]["COUNT"] = 0
            audit_stats[audit_category]["SUB_CATEGORY"][best_score]["SAMPLE"] = []
        audit_stats[audit_category]["SUB_CATEGORY"][best_score]["COUNT"] += 1
        if len(audit_stats[audit_category]["SUB_CATEGORY"][best_score]["SAMPLE"]) < 500:
            audit_stats[audit_category]["SUB_CATEGORY"][best_score]["SAMPLE"].append(
                audit_sample
            )
        else:
            random_index = random.randint(1, 499)
            if random_index % 10 != 0:
                audit_stats[audit_category]["SUB_CATEGORY"][best_score]["SAMPLE"][
                    random_index
                ] = audit_sample

        # if debug:
        #    input('press any key to continue')
    progress_cntr = progress_display(progress_cntr, "newer entities audited, complete")
    csv_handle.close()

    prior_entity_count = len(prior_map["entities"])
    newer_entity_count = len(newer_map["entities"])
    entity_precision = (
        round(common_entity_count + 0.0 / newer_entity_count + 0.0, 5)
        if newer_entity_count
        else 0
    )
    entity_recall = (
        round(common_entity_count + 0.0 / newer_entity_count + 0.0, 5)
        if prior_entity_count
        else 0
    )
    entity_f1_score = (
        round(
            2 * (entity_precision * entity_recall) / (entity_precision + entity_recall),
            5,
        )
        if entity_precision or entity_recall
        else 0
    )

    pair_same_positive = common_pair_count
    pair_new_positive = (
        newer_pair_count - common_pair_count
        if newer_pair_count > common_pair_count
        else 0
    )
    pair_new_negative = (
        prior_pair_count - common_pair_count
        if prior_pair_count > common_pair_count
        else 0
    )
    pair_precision = (
        round(pair_same_positive / (pair_same_positive + pair_new_positive), 5)
        if pair_same_positive + pair_new_positive > 0
        else 0
    )
    pair_recall = (
        round(pair_same_positive / (pair_same_positive + pair_new_negative), 5)
        if pair_same_positive + pair_new_negative > 0
        else 0
    )
    pair_f1_score = (
        round((2 * pair_precision * pair_recall) / (pair_precision + pair_recall), 5)
        if pair_precision + pair_recall > 0
        else 0
    )

    stat_pack = {
        "SOURCE": "G2Audit",
        "ENTITY": {
            "PRIOR_COUNT": prior_entity_count,
            "NEWER_COUNT": newer_entity_count,
            "COMMON_COUNT": common_entity_count,
            "PRECISION": entity_precision,
            "RECALL": entity_recall,
            "F1-SCORE": entity_f1_score,
        },
        "PAIRS": {
            "PRIOR_COUNT": prior_pair_count,
            "NEWER_COUNT": newer_pair_count,
            "COMMON_COUNT": common_pair_count,
            "SAME_POSITIVE": pair_same_positive,
            "NEW_POSITIVE": pair_new_positive,
            "NEW_NEGATIVE": pair_new_negative,
            "PRECISION": pair_precision,
            "RECALL": pair_recall,
            "F1-SCORE": pair_f1_score,
        },
        "AUDIT": audit_stats,
    }
    with open(json_file_name, "w") as f:
        json.dump(stat_pack, f)

    print(
        textwrap.dedent(
            f"""\

    {stat_pack['PAIRS']['PRIOR_COUNT']} prior pairs
    {stat_pack['PAIRS']['NEWER_COUNT']} newer pairs
    {stat_pack['PAIRS']['COMMON_COUNT']} common pairs

    {stat_pack['PAIRS']['SAME_POSITIVE']} same positives
    {stat_pack['PAIRS']['NEW_POSITIVE']} new positives
    {stat_pack['PAIRS']['NEW_NEGATIVE']} new negatives
    {stat_pack['PAIRS']['PRECISION']} precision
    {stat_pack['PAIRS']['RECALL']} recall
    {stat_pack['PAIRS']['F1-SCORE']} f1-score

    {stat_pack['ENTITY']['PRIOR_COUNT']} prior entities
    {stat_pack['ENTITY']['NEWER_COUNT']} new entities
    {stat_pack['ENTITY']['COMMON_COUNT']} common entities
    {stat_pack['AUDIT'].get('MERGE', {}).get('COUNT', 0)} merged entities
    {stat_pack['AUDIT'].get('SPLIT', {}).get('COUNT', 0)} split entities
    {stat_pack['AUDIT'].get('SPLIT+MERGE', {}).get('COUNT', 0)} split+merge entities

    """
        )
    )
    if missing_prior_record_cnt or missing_newer_record_cnt:
        print(f"{missing_prior_record_cnt} missing prior records")
        print(f"{missing_newer_record_cnt} missing newer records")
        print()
    return 0


def stat_checker_file_loader(file_name):
    entity_count = 0
    pairs = {}
    with open(file_name, "r") as f:
        reader = csv.DictReader(f)
        cluster_field, source_field, record_field, score_field = detect_column_names(
            reader.fieldnames
        )
        sorted_reader = sorted(
            reader, key=itemgetter(cluster_field)
        )  # can't rely on input being sorted
        progress_cntr = 0
        for entity_group in groupby(sorted_reader, key=itemgetter(cluster_field)):
            progress_cntr = progress_display(progress_cntr, "entities loaded")
            entity_count += 1
            entity_id = entity_group[0]
            entity_records = [
                x
                for x in list(entity_group[1])
                if x.get("RELATED_ENTITY_ID", "0") == "0"
            ]
            for entity_record1 in entity_records:
                record_key1 = compute_record_key(
                    entity_record1,
                    cluster_field,
                    source_field,
                    record_field,
                    score_field,
                )
                for entity_record2 in entity_records:
                    record_key2 = compute_record_key(
                        entity_record2,
                        cluster_field,
                        source_field,
                        record_field,
                        score_field,
                    )
                    if record_key1 < record_key2:
                        pairs[f"{record_key1}|{record_key2}"] = entity_id
        progress_cntr = progress_display(progress_cntr, "entities loaded, complete")
    return entity_count, pairs


def stat_checker(newer_file_name, prior_file_name):
    """simplified statistic checker"""
    try:
        newer_entity_count, newer_pairs = stat_checker_file_loader(newer_file_name)
        prior_entity_count, prior_pairs = stat_checker_file_loader(prior_file_name)
    except Exception as err:
        logging.error(f"{err} loading files")
        return 1

    logging.info("checking newer pairs for true and false positives")
    true_positive_count = 0
    false_positive_count = 0
    progress_cntr = 0
    for newer_pair in newer_pairs:
        progress_cntr = progress_display(progress_cntr, "newer pairs checked")
        if newer_pair in prior_pairs:
            true_positive_count += 1
        else:
            false_positive_count += 1
    progress_cntr = progress_display(progress_cntr, "newer pairs checked, complete")

    progress_cntr = 0
    logging.info("checking prior pairs for false negatives")
    false_negative_count = 0
    for prior_pair in prior_pairs:
        progress_cntr = progress_display(progress_cntr, "prior pairs checked")
        if prior_pair not in newer_pairs:
            false_negative_count += 1
    progress_cntr = progress_display(progress_cntr, "prior pairs checked, complete")

    precision = (
        round(true_positive_count / (true_positive_count + false_positive_count), 5)
        if true_positive_count + false_positive_count > 0
        else 0
    )
    recall = (
        round(true_positive_count / (true_positive_count + false_negative_count), 5)
        if true_positive_count + false_negative_count > 0
        else 0
    )
    f1_score = (
        round((2 * precision * recall) / (precision + recall), 5)
        if precision + recall > 0
        else 0
    )

    print(
        textwrap.dedent(
            f"""\

    {newer_entity_count} newer_entities
    {prior_entity_count} prior_entities

    {len(newer_pairs)} newer_pairs
    {len(prior_pairs)} prior_pairs

    {true_positive_count} true_positives
    {false_positive_count} false_positives
    {false_negative_count} false_negatives

    {precision} precision
    {recall} recall
    {f1_score} f1-score

    """
        )
    )
    return 0


def count_by_key(_dict, _key):
    if _key:
        if _key in _dict:
            _dict[_key] += 1
        else:
            _dict[_key] = 1
    return _dict


def list_by_key(_dict, _key, _item):
    if _key not in _dict:
        _dict[_key] = [_item]
    elif _item and _item not in _dict[_key]:
        _dict[_key].append(_item)
    return _dict


def compute_record_key(record, cluster_field, source_field, record_field, score_field):
    return f"{record[source_field]}||{record[record_field]}"


def parse_record_key(key):
    return key.split("||")


def progress_display(progress_cntr, desc, **kwargs):
    interval = kwargs.get("interval", 100000)
    if "complete" not in desc:
        progress_cntr += 1
    if progress_cntr % interval == 0 or "complete" in desc:
        logging.info(f"{progress_cntr:,} {desc}")
    return progress_cntr


if __name__ == "__main__":

    argParser = argparse.ArgumentParser()
    argParser.add_argument(
        "-n",
        "--newer_csv_file",
        dest="newerFile",
        default=None,
        help="the latest entity map file",
    )
    argParser.add_argument(
        "-p",
        "--prior_csv_file",
        dest="priorFile",
        default=None,
        help="the prior entity map file",
    )
    argParser.add_argument(
        "-o",
        "--output_file_root",
        dest="outputRoot",
        default=None,
        help="the output file root name (both a .csv and a .json file will be created",
    )
    argParser.add_argument(
        "-D",
        "--debug",
        dest="debug",
        action="store_true",
        default=False,
        help="print debug statements",
    )
    argParser.add_argument(
        "-C",
        "--checker",
        dest="checker",
        action="store_true",
        default=False,
        help="run simplified statistic checker",
    )
    args = argParser.parse_args()

    loggingLevel = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%m/%d %I:%M",
        level=loggingLevel,
    )

    if not args.newerFile:
        logging.error("A newer csv file must be specified with -n")
        sys.exit(1)
    elif not os.path.exists(args.newerFile):
        logging.error("The newer csv file was not found!")
        sys.exit(1)

    if not args.priorFile:
        logging.error("A prior csv file must be specified with -p")
        sys.exit(1)
    elif not os.path.exists(args.priorFile):
        logging.error("The prior csv file was not found!")
        sys.exit(1)

    if not args.outputRoot:
        logging.error("An output root must be specified with -o")
        sys.exit(1)

    proc_start_time = time.time()
    if args.checker:
        success = stat_checker(args.newerFile, args.priorFile)
    else:
        success = audit(args.newerFile, args.priorFile, args.outputRoot, args.debug)
    print(
        f"process completed in {round((time.time() - proc_start_time) / 60, 1)} minutes\n"
    )

    sys.exit(success)
