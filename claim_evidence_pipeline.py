#!/usr/bin/env python3
"""SE363 evidence candidate retrieval and human review queue.

This module NEVER guesses truth from lexical similarity. Evidence candidates are
retrieval hints. Gold/prediction scores only use supplied human-reviewed labels.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import unicodedata
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
STOP = {
    'một', 'hai', 'bạn', 'mình', 'cho', 'các', 'của', 'với', 'trong', 'theo',
    'được', 'đến', 'không', 'những', 'cũng', 'thì', 'rằng', 'này', 'thông',
    'qua', 'làm', 'khi', 'còn', 'đang', 'trên', 'đều', 'vào', 'đây', 'như',
    'hơn', 'được', 'tôi', 'người', 'nhà', 'nhiều', 'phần', 'thực', 'dụng'
}


def tokens(s):
    t = unicodedata.normalize('NFC', str(s or '')).casefold()
    return {x for x in re.findall(r'[^\W_]+', t, flags=re.UNICODE)
            if len(x) >= 3 and x not in STOP}


def iter_jsonl(path):
    if not path.exists():
        return
    with path.open(encoding='utf-8-sig') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                yield item


def evidence_records(root=ROOT):
    """Only source metadata/summary. Never treat discovery as verified evidence."""
    files = [root/'data/raw/evidence_discovery.jsonl',
             root/'data/raw/research_metadata.jsonl']
    seen = set()
    for path in files:
        for item in iter_jsonl(path):
            url = str(item.get('url') or item.get('source_url') or '').strip()
            if not url.startswith('https://') or url in seen:
                continue
            seen.add(url)
            title = str(item.get('title') or '').strip()
            summary = str(item.get('summary') or '')
            if not title and not summary:
                continue
            yield {'evidence_id': str(item.get('doi') or item.get('DOI') or item.get('_key') or url),
                   'url': url, 'title': title, 'summary': summary,
                   'source_type': str(item.get('source_type') or ''),
                   'publisher': str(item.get('publisher') or item.get('publisher_domain') or ''),
                   'published_at': str(item.get('published_at') or item.get('seen_date') or ''),
                   'terms': tokens(title+' '+summary)}


def claims(path):
    if not path.is_file():
        raise FileNotFoundError('Claim candidate file not found: ' + str(path))
    with path.open(encoding='utf-8-sig', newline='') as f:
        yield from csv.DictReader(f)


def score_evidence(claim_text, doc):
    query = tokens(claim_text)
    if not query:
        return 0.0
    title_words = tokens(doc['title'])
    # Simple lexical candidate retrieval, not evidence entailment or fact checking.
    shared = query & doc['terms']
    return round((len(shared) + 0.5 * len(query & title_words)) / len(query), 4)


def write_csv(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def prepare(root=ROOT, input_path=None, max_candidates=3):
    src = input_path or root/'data/auto_baseline/apify_threads/claim_candidates_review.csv'
    claim_rows = list(claims(src))
    docs = list(evidence_records(root))
    results = []
    for claim in claim_rows:
        best = sorted(((score_evidence(claim.get('claim_text',''), d), d) for d in docs),
                      key=lambda x: (-x[0], x[1]['url']))
        hits = [(score, d) for score, d in best[:max_candidates] if score > 0]
        if not hits:
            hits = [(0, {'evidence_id':'', 'url':'', 'title':'', 'publisher':'',
                         'source_type':''})]
        for score, d in hits:
            results.append({
                'claim_id': claim.get('claim_id',''),
                'post_id': claim.get('post_id',''),
                'claim_text': claim.get('claim_text',''),
                'post_url': claim.get('source_url',''),
                'candidate_evidence_id': d['evidence_id'],
                'candidate_evidence_url': d['url'],
                'candidate_evidence_title': d['title'],
                'candidate_evidence_publisher': d['publisher'],
                'lexical_score_not_truth': score,
                'evidence_relevance_verified': '',
                'verified_product_id': '',
                'evidence_valid_at_claim_time': '',
                'verdict': '',
                'reviewer': '',
                'reviewed_at': '',
                'review_notes': 'Candidate only: independently review source, product, dose, audience and time',
            })
    dest = root/'data/auto_baseline/apify_threads/evidence_review_queue.csv'
    fields = list(results[0]) if results else [
        'claim_id','post_id','claim_text','post_url','candidate_evidence_id',
        'candidate_evidence_url','candidate_evidence_title','candidate_evidence_publisher',
        'lexical_score_not_truth','evidence_relevance_verified','verified_product_id',
        'evidence_valid_at_claim_time','verdict','reviewer','reviewed_at','review_notes']
    write_csv(dest, fields, results)
    summary = {'claims':len(claim_rows),'candidate_sources':len(docs),
               'review_queue_rows':len(results),'automatic_truth_labels':0,
               'note':'Lexical retrieval only; a reviewer must verify evidence and verdict.'}
    report = root/'data/reports/apify_evidence_candidates_report.json'
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    return summary


LABELS = ('SUPPORTED','REFUTED','NOT_ENOUGH_INFORMATION')


def evaluate(gold_path, predictions_path):
    """Evaluate *given* adjudicated gold and predictions, never construct gold."""
    def load(path):
        with Path(path).open(encoding='utf-8-sig', newline='') as f:
            rows = list(csv.DictReader(f))
        out = {}
        for row in rows:
            key = str(row.get('claim_id') or '').strip()
            lab = str(row.get('verdict') or row.get('verification_label') or '').strip().upper()
            if key and lab in LABELS:
                out[key] = lab
        return out
    gold, pred = load(gold_path), load(predictions_path)
    keys = sorted(gold.keys() & pred.keys())
    matrix = {truth: {guess: 0 for guess in LABELS} for truth in LABELS}
    for key in keys:
        matrix[gold[key]][pred[key]] += 1
    scores = {}
    for name in LABELS:
        tp = matrix[name][name]
        fp = sum(matrix[other][name] for other in LABELS if other != name)
        fn = sum(matrix[name][other] for other in LABELS if other != name)
        precision = tp/(tp+fp) if tp+fp else 0.0
        recall = tp/(tp+fn) if tp+fn else 0.0
        f1 = 2*precision*recall/(precision+recall) if precision+recall else 0.0
        scores[name] = {'precision':round(precision,4),'recall':round(recall,4),'f1':round(f1,4)}
    return {'compared':len(keys),'gold_count':len(gold),'prediction_count':len(pred),
            'macro_f1':round(sum(v['f1'] for v in scores.values())/len(LABELS),4) if keys else None,
            'accuracy':round(sum(matrix[l][l] for l in LABELS)/len(keys),4) if keys else None,
            'per_class':scores,'confusion_matrix':matrix,
            'warning':'Metrics cover only matched claim IDs; inspect unmatched and class balance.'}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    cmd = p.add_subparsers(dest='command',required=True)
    prepare_cmd = cmd.add_parser('prepare')
    prepare_cmd.add_argument('--input',type=Path)
    prepare_cmd.add_argument('--max-candidates',type=int,default=3)
    eval_cmd=cmd.add_parser('evaluate')
    eval_cmd.add_argument('--gold',type=Path,required=True)
    eval_cmd.add_argument('--predictions',type=Path,required=True)
    a=p.parse_args(argv)
    if a.command=='prepare':
        if not 1<=a.max_candidates<=10:
            p.error('--max-candidates must be 1..10')
        result=prepare(input_path=a.input,max_candidates=a.max_candidates)
    else:
        result=evaluate(a.gold,a.predictions)
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=='__main__':
    main()
