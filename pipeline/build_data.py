"""OneQuest data pipeline.

สร้างไฟล์ web/data.json ที่หน้าเว็บอ่าน

    python pipeline/build_data.py            # ข้อมูลตัวอย่าง (สมมติ) ไม่ต้องใช้อินเทอร์เน็ต
    python pipeline/build_data.py --live     # ดึงข้อมูลจริงจาก World Bank, OpenAlex, GDELT

ใช้แค่ไลบรารีมาตรฐานของ Python 3.9+ ไม่ต้อง pip install อะไร
"""

import argparse
import datetime as dt
import json
import random
import sys
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROBLEMS = ROOT / "pipeline" / "problems.json"
OUT = ROOT / "web" / "data.json"

DAYS = 30

# น้ำหนักของคะแนนโอกาส (รวมกันได้ 1) ปรับได้ตามที่ทีมตกลง
WEIGHTS = {"scale": 0.30, "neglect": 0.25, "tractability": 0.25, "timing": 0.20}

# ชั้นของแหล่งข้อมูล ใช้ตอนตรวจสอบหลักฐาน
TIERS = {
    "research": "งานวิจัยที่ผ่าน peer review",
    "official": "สถิติทางการ",
    "news": "ข่าว",
    "social": "เสียงจากโซเชียล",
}


def opportunity_score(s):
    return round(sum(s[k] * w for k, w in WEIGHTS.items()))


# ---------------------------------------------------------------------------
# การตรวจสอบหลักฐาน
#
# ระบบอัตโนมัติยืนยันไม่ได้ว่า "ข้อเท็จจริงถูก" ทำได้แค่ให้คะแนนความน่าเชื่อถือ
# ของแหล่งและดูว่ามีหลายแหล่งที่เป็นอิสระต่อกันพูดตรงกันหรือไม่
# สถานะ verified จึงต้องผ่านคนตรวจเสมอ (reviewed_by)
# ---------------------------------------------------------------------------

def triage(item, all_items):
    """คืนสถานะเบื้องต้นของหลักฐานหนึ่งชิ้น ก่อนส่งให้คนตรวจ"""
    if item.get("reviewed_by"):
        return item["status"]
    if item["tier"] in ("research", "official") and item.get("doi_or_url"):
        return "corroborating"
    same = {
        i["domain"]
        for i in all_items
        if i is not item and i["problem"] == item["problem"] and i["tier"] in ("news", "official", "research")
    }
    if item["tier"] == "news" and len(same - {item["domain"]}) >= 2:
        return "corroborating"
    return "unverified"


# ---------------------------------------------------------------------------
# แหล่งข้อมูลจริง
# ---------------------------------------------------------------------------

def get_json(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "OneQuest/0.1 (mockup)"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def worldbank_latest(indicator, country):
    """ค่าล่าสุดของตัวชี้วัด World Bank (อัปเดตปีละครั้ง)"""
    url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json&per_page=60"
    data = get_json(url)
    if len(data) < 2 or not data[1]:
        return None
    for row in data[1]:
        if row["value"] is not None:
            return {
                "indicator": indicator,
                "name": row["indicator"]["value"],
                "year": int(row["date"]),
                "value": row["value"],
                "source": "World Bank WDI",
                "url": f"https://data.worldbank.org/indicator/{indicator}?locations={country[:2]}",
            }
    return None


def openalex_recent(query, since):
    """งานวิจัยใหม่จาก OpenAlex (ฐานข้อมูลเปิดที่ครอบคลุมใกล้เคียง Google Scholar และมี API)"""
    params = {
        "search": query,
        "filter": f"from_publication_date:{since.isoformat()},type:article",
        "sort": "cited_by_count:desc",
        "per_page": "5",
    }
    data = get_json("https://api.openalex.org/works?" + urllib.parse.urlencode(params))
    works = []
    for w in data.get("results", []):
        works.append({
            "tier": "research",
            "title": w.get("display_name") or "",
            "date": w.get("publication_date"),
            "source": ((w.get("primary_location") or {}).get("source") or {}).get("display_name") or "OpenAlex",
            "domain": "openalex.org",
            "doi_or_url": w.get("doi") or w.get("id"),
            "cited_by": w.get("cited_by_count", 0),
        })
    return data.get("meta", {}).get("count", 0), works


def gdelt_news(query, days=1):
    """ข่าวล่าสุดจาก GDELT (อัปเดตทุก 15 นาที)"""
    params = {"query": query, "mode": "artlist", "maxrecords": "10", "format": "json", "timespan": f"{days}d"}
    try:
        data = get_json("https://api.gdeltproject.org/api/v2/doc/doc?" + urllib.parse.urlencode(params))
    except Exception as e:  # GDELT ตอบเป็นข้อความธรรมดาเมื่อคำค้นสั้นเกินไป
        print(f"  GDELT: {e}", file=sys.stderr)
        return []
    items = []
    for a in data.get("articles", []):
        items.append({
            "tier": "news",
            "title": a.get("title", ""),
            "date": (a.get("seendate") or "")[:8],
            "source": a.get("domain", ""),
            "domain": a.get("domain", ""),
            "doi_or_url": a.get("url"),
        })
    return items


# ---------------------------------------------------------------------------
# ข้อมูลตัวอย่าง
# ---------------------------------------------------------------------------

SAMPLE_EVIDENCE = {
    "research": ["งานวิจัยประเมินขนาดปัญหาถึงปี 2035", "งานวิจัยเปรียบเทียบนโยบายใน 5 ประเทศ", "บทความทบทวนวรรณกรรม (systematic review)"],
    "official": ["ตัวเลขทางการรอบล่าสุด", "รายงานประจำปีของหน่วยงานรัฐ"],
    "news": ["ข่าว: เหตุการณ์ล่าสุดที่เกี่ยวข้อง", "ข่าว: นโยบายหรืองบประมาณใหม่", "ข่าว: บริษัทเปิดตัวบริการใหม่"],
    "social": ["กระทู้บ่นปัญหาที่คนแชร์มาก"],
}
SAMPLE_SOURCES = {
    "research": ["OpenAlex", "Crossref", "PubMed"],
    "official": ["World Bank WDI", "สำนักงานสถิติแห่งชาติ", "UN SDG Database"],
    "news": ["GDELT", "สำนักข่าว A", "สำนักข่าว B", "สำนักข่าว C"],
    "social": ["Pantip", "X"],
}


def sample_series(rng, base, drift):
    v, out = base, []
    for _ in range(DAYS):
        v = max(0, v + drift + rng.gauss(0, base * 0.18))
        out.append(round(v))
    return out


def build(live=False):
    problems = json.loads(PROBLEMS.read_text(encoding="utf-8"))
    rng = random.Random(519)
    today = dt.date.today()
    dates = [(today - dt.timedelta(days=DAYS - 1 - i)).isoformat() for i in range(DAYS)]
    evidence = []
    out = []

    for p in problems:
        s = p["sample"]
        scores = {k: s[k] for k in WEIGHTS}
        signals = sample_series(rng, 4 + s["timing"] / 10, rng.uniform(-0.1, 0.25))
        item = {
            "id": p["id"], "title": p["title"], "category": p["category"], "gap_type": p["gap_type"],
            "scope": p["scope"], "region": p["region"], "sdg": p["sdg"], "industries": p["industries"],
            "affected_m": s["affected_m"], "scores": scores, "score": opportunity_score(scores),
            "demand": {**p["demand"], "value": s["demand_v"]},
            "supply": {**p["supply"], "value": s["supply_v"]},
            "signals": signals, "indicator": None, "new_papers_12m": None,
        }

        if live:
            print(f"- {p['id']}", file=sys.stderr)
            try:
                item["indicator"] = worldbank_latest(p["wb_indicator"], p["wb_country"])
            except Exception as e:
                print(f"  World Bank: {e}", file=sys.stderr)
            try:
                count, works = openalex_recent(p["query"], today - dt.timedelta(days=365))
                item["new_papers_12m"] = count
                for w in works:
                    evidence.append({**w, "problem": p["id"]})
            except Exception as e:
                print(f"  OpenAlex: {e}", file=sys.stderr)
            for n in gdelt_news(p["query"]):
                evidence.append({**n, "problem": p["id"]})
        else:
            for _ in range(rng.randint(3, 6)):
                tier = rng.choices(list(SAMPLE_EVIDENCE), weights=[3, 2, 4, 1])[0]
                src = rng.choice(SAMPLE_SOURCES[tier])
                ev = {
                    "problem": p["id"], "tier": tier, "title": rng.choice(SAMPLE_EVIDENCE[tier]),
                    "date": dates[rng.randint(DAYS - 10, DAYS - 1)], "source": src, "domain": src,
                    "doi_or_url": None if tier in ("news", "social") else "https://doi.org/10.xxxx/sample",
                }
                if tier in ("research", "official") and rng.random() < 0.6:
                    ev.update(status="verified", reviewed_by="ทีมตรวจสอบ (ตัวอย่าง)")
                elif tier == "news" and rng.random() < 0.12:
                    ev.update(status="disputed", reviewed_by="ทีมตรวจสอบ (ตัวอย่าง)", note="ตัวเลขในข่าวไม่ตรงกับแหล่งทางการ")
                evidence.append(ev)
        out.append(item)

    for ev in evidence:
        ev["status"] = triage(ev, evidence)
    evidence.sort(key=lambda e: e.get("date") or "", reverse=True)
    for item in out:
        item["evidence"] = [e for e in evidence if e["problem"] == item["id"]]

    cats = ["supply", "access", "systemic"]
    daily = {c: [sum(i["signals"][d] for i in out if i["category"] == c) for d in range(DAYS)] for c in cats}
    status_counts = {}
    for e in evidence:
        status_counts[e["status"]] = status_counts.get(e["status"], 0) + 1

    return {
        "is_sample": not live,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="minutes"),
        "weights": WEIGHTS,
        "tiers": TIERS,
        "dates": dates,
        "daily_signals": daily,
        "evidence_status": status_counts,
        "new_evidence_today": sum(1 for e in evidence if e.get("date") == dates[-1]),
        "problems": sorted(out, key=lambda i: -i["score"]),
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="ดึงข้อมูลจริงแทนข้อมูลตัวอย่าง")
    args = ap.parse_args()
    data = build(live=args.live)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"wrote {OUT} ({len(data['problems'])} problems, sample={data['is_sample']})")
