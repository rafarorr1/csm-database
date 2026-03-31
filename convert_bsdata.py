#!/usr/bin/env python3
"""
convert_bsdata.py
Fetches BSData wh40k-10e .cat files from GitHub and converts them
into the wh40k_database.json format used by CSM Army Advisor.

Run locally:   python3 convert_bsdata.py
Run in CI:     python3 convert_bsdata.py --output wh40k_database.json
"""

import json
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

# ── BSData raw URL base ────────────────────────────────────────────────────
BSDATA_BASE = "https://raw.githubusercontent.com/BSData/wh40k-10e/main/"

# ── Map faction display names → .cat filenames in BSData ──────────────────
FACTION_FILES = {
    "chaos_space_marines":  "Chaos - Chaos Space Marines.cat",
    "world_eaters":         "Chaos - World Eaters.cat",
    "death_guard":          "Chaos - Death Guard.cat",
    "thousand_sons":        "Chaos - Thousand Sons.cat",
    "emperors_children":    "Chaos - Emperor's Children.cat",
    "chaos_daemons":        "Chaos - Chaos Daemons.cat",
    "chaos_knights":        "Chaos - Chaos Knights.cat",
    "space_marines":        "Imperium - Space Marines.cat",
    "blood_angels":         "Imperium - Blood Angels.cat",
    "dark_angels":          "Imperium - Dark Angels.cat",
    "space_wolves":         "Imperium - Space Wolves.cat",
    "black_templars":       "Imperium - Black Templars.cat",
    "grey_knights":         "Imperium - Grey Knights.cat",
    "adepta_sororitas":     "Imperium - Adepta Sororitas.cat",
    "adeptus_custodes":     "Imperium - Adeptus Custodes.cat",
    "adeptus_mechanicus":   "Imperium - Adeptus Mechanicus.cat",
    "astra_militarum":      "Imperium - Astra Militarum.cat",
    "imperial_knights":     "Imperium - Imperial Knights.cat",
    "necrons":              "Necrons.cat",
    "orks":                 "Orks.cat",
    "tyranids":             "Tyranids.cat",
    "genestealer_cults":    "Genestealer Cults.cat",
    "tau_empire":           "T'au Empire.cat",
    "craftworlds":          "Aeldari - Craftworlds.cat",
    "drukhari":             "Aeldari - Drukhari.cat",
    "leagues_of_votann":    "Leagues of Votann.cat",
}

NS = "http://www.battlescribe.net/schema/catalogueSchema"

# ── Helpers ────────────────────────────────────────────────────────────────

def tag(name):
    return f"{{{NS}}}{name}"

def fetch_cat(filename):
    url = BSDATA_BASE + urllib.request.quote(filename)
    print(f"  Fetching: {filename}")
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            return r.read().decode("utf-8")
    except Exception as e:
        print(f"  ERROR fetching {filename}: {e}")
        return None

def get_chars(profile_el):
    """Return dict of characteristic name → value from a profile element."""
    chars = {}
    for c in profile_el.iter(tag("characteristic")):
        name = c.get("name", "").strip()
        val  = (c.text or "").strip()
        if name:
            chars[name] = val
    return chars

def parse_stat_block(chars):
    """
    BSData Unit profiles use characteristic names like:
    M, T, SV, W, LD, OC and sometimes INV/INV SV
    """
    stat_keys = {
        "M": "M", "Move": "M",
        "T": "T", "Toughness": "T",
        "SV": "SV", "Save": "SV",
        "W": "W", "Wounds": "W",
        "LD": "LD", "Leadership": "LD",
        "OC": "OC", "Objective Control": "OC",
        "INV": "INV", "INV SV": "INV", "Invulnerable Save": "INV",
    }
    stats = {}
    for raw_key, norm_key in stat_keys.items():
        if raw_key in chars and norm_key not in stats:
            val = chars[raw_key]
            if val and val not in ("-", "N/A", ""):
                stats[norm_key] = val
    return stats

def parse_weapon_profile(profile_el, type_name):
    """Parse a Ranged Weapons or Melee Weapons profile into our weapon dict."""
    chars = get_chars(profile_el)
    weapon = {
        "name":  profile_el.get("name", "Unknown"),
        "type":  "Ranged" if "Ranged" in type_name else "Melee",
    }
    # Map BSData characteristic names to our schema
    field_map = {
        "Range": "range",
        "A": "A", "Attacks": "A",
        "BS": "BS", "WS": "WS", "Skill": "BS",
        "S": "S", "Strength": "S",
        "AP": "AP",
        "D": "D", "Damage": "D",
        "Keywords": "keywords",
    }
    for bsd_key, our_key in field_map.items():
        if bsd_key in chars:
            weapon[our_key] = chars[bsd_key]
    return weapon

def slugify(name):
    """Convert a unit name to a safe JSON key."""
    s = name.lower()
    s = re.sub(r"['\u2019]", "", s)
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")

# ── Core parser ────────────────────────────────────────────────────────────

def parse_catalogue(xml_text, faction_key):
    """
    Parse a BSData .cat XML string and return a faction dict matching
    the wh40k_database.json schema.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        print(f"  XML parse error: {e}")
        return None

    faction_name = root.get("name", faction_key)
    units_out = {}
    army_rule = None
    faction_mechanic = None

    # Walk all selectionEntries at any depth
    for entry in root.iter(tag("selectionEntry")):
        entry_type = entry.get("type", "")
        entry_name = entry.get("name", "").strip()

        # Skip hidden, non-unit entries
        if entry.get("hidden", "false") == "true":
            continue
        if entry_type not in ("unit", "model"):
            continue
        if not entry_name:
            continue

        unit = {
            "name": entry_name,
            "role": "Other",
            "points": {},
            "stats": {},
            "unit_size": "",
            "keywords": "",
            "faction_keywords": "",
            "leader": "",
            "weapons": [],
            "abilities": [],
        }

        # ── Profiles ──────────────────────────────────────────────────────
        for profile in entry.iter(tag("profile")):
            type_name = profile.get("typeName", "")
            prof_name = profile.get("name", "")
            chars = get_chars(profile)

            if type_name == "Unit":
                # Stat block
                stats = parse_stat_block(chars)
                if stats:
                    unit["stats"] = stats
                # Unit size often in a characteristic called "Composition" or similar
                if "Composition" in chars:
                    unit["unit_size"] = chars["Composition"]
                elif "Unit Size" in chars:
                    unit["unit_size"] = chars["Unit Size"]

            elif type_name in ("Abilities", "Ability"):
                desc = chars.get("Description", chars.get("Effect", ""))
                if desc:
                    unit["abilities"].append({
                        "name": prof_name,
                        "text": desc,
                    })
                # Detect Leader ability text for attaching info
                if prof_name == "Leader" and "attached to" in desc.lower():
                    unit["leader"] = desc

            elif "Weapon" in type_name or type_name in ("Ranged Weapons", "Melee Weapons"):
                weapon = parse_weapon_profile(profile, type_name)
                unit["weapons"].append(weapon)

        # ── Keywords ──────────────────────────────────────────────────────
        kw_tags = []
        fkw_tags = []
        for cat_link in entry.iter(tag("categoryLink")):
            name = cat_link.get("name", "")
            if name in ("Faction: Heretic Astartes", "Faction: Space Marines") or name.startswith("Faction"):
                fkw_tags.append(name.replace("Faction: ", ""))
            elif name not in ("Configuration", "Uncategorised"):
                kw_tags.append(name)

        unit["keywords"] = ", ".join(kw_tags) if kw_tags else ""
        unit["faction_keywords"] = ", ".join(fkw_tags) if fkw_tags else ""

        # ── Role from category ─────────────────────────────────────────────
        kw_lower = [k.lower() for k in kw_tags]
        if "battleline" in kw_lower:
            unit["role"] = "Battleline"
        elif "character" in kw_lower:
            unit["role"] = "Character"
        elif "dedicated transport" in kw_lower:
            unit["role"] = "Transport"
        elif "fortification" in kw_lower:
            unit["role"] = "Fortification"
        elif "epic hero" in kw_lower:
            unit["role"] = "Epic Hero"

        # ── Points ────────────────────────────────────────────────────────
        # Points live in <costs> → <cost name="pts" value="X"/>
        pts_vals = []
        for cost in entry.iter(tag("cost")):
            if "pts" in cost.get("name", "").lower():
                try:
                    v = float(cost.get("value", 0))
                    if v > 0:
                        pts_vals.append(int(v))
                except ValueError:
                    pass
        if pts_vals:
            unit["points"] = pts_vals[0] if len(pts_vals) == 1 else pts_vals[0]

        key = slugify(entry_name)
        # Avoid duplicates — keep the first (usually the main entry)
        if key not in units_out:
            units_out[key] = unit

    # ── Army rule and faction mechanic (from sharedRules or rules) ────────
    for rule in root.iter(tag("rule")):
        rule_name = rule.get("name", "")
        desc_el = rule.find(f".//{tag('description')}")
        desc = (desc_el.text or "").strip() if desc_el is not None else ""
        if not army_rule and desc:
            army_rule = {"name": rule_name, "text": desc}
        break  # just grab the first top-level rule as the army rule

    return {
        "_meta": {
            "name": faction_name,
            "source": "BSData/wh40k-10e",
            "wahapedia_url": f"https://wahapedia.ru/wh40k10ed/factions/",
        },
        "army_rule": army_rule or {"name": "", "text": ""},
        "faction_mechanic": faction_mechanic or {"name": "", "text": "", "keyword": ""},
        "units": units_out,
    }

# ── Main ───────────────────────────────────────────────────────────────────

def main():
    output_path = "wh40k_database.json"
    for arg in sys.argv[1:]:
        if arg.startswith("--output="):
            output_path = arg.split("=", 1)[1]
        elif arg == "--output" and len(sys.argv) > sys.argv.index(arg) + 1:
            output_path = sys.argv[sys.argv.index(arg) + 1]

    print("=== BSData → wh40k_database.json converter ===\n")

    db = {
        "_meta": {
            "name": "Warhammer 40K Tactical Database",
            "version": "auto",
            "game": "Warhammer 40,000 10th Edition",
            "source": "BSData/wh40k-10e (auto-generated)",
            "factions": list(FACTION_FILES.keys()),
        },
        "factions": {},
    }

    success = 0
    for faction_key, filename in FACTION_FILES.items():
        print(f"\n[{faction_key}]")
        xml = fetch_cat(filename)
        if xml is None:
            print(f"  Skipped (fetch failed)")
            continue
        faction_data = parse_catalogue(xml, faction_key)
        if faction_data is None:
            print(f"  Skipped (parse failed)")
            continue
        unit_count = len(faction_data.get("units", {}))
        print(f"  Parsed {unit_count} units")
        db["factions"][faction_key] = faction_data
        success += 1

    db["_meta"]["factions"] = list(db["factions"].keys())

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

    total_units = sum(len(v.get("units", {})) for v in db["factions"].values())
    print(f"\n=== Done: {success}/{len(FACTION_FILES)} factions, {total_units} total units ===")
    print(f"Output: {output_path}")

if __name__ == "__main__":
    main()
