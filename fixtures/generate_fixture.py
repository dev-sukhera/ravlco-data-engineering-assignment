#!/usr/bin/env python3
"""Deterministic synthetic party fixture for the Crash-to-Contact exercise.

Every value here is fabricated. Phone line numbers use the NANP fictional
range (555-0100 to 555-0199), which is permanently unassignable. Names and
street addresses do not correspond to real people or deliverable addresses.

Real area codes ARE used, because the calling-window logic must be exercised
against genuine geography. That is the point of records 07 and 08.

Run:  python fixtures/generate_fixture.py > fixtures/synthetic_parties.csv
The output is committed, so you do not need to run this. It is here so you can
see there is no hidden magic in the fixture.
"""
import csv, sys
from datetime import date, timedelta

TODAY = date(2026, 9, 1)          # frozen so the fixture is reproducible
def ago(d): return (TODAY - timedelta(days=d)).isoformat()

# party_id, name, street, city, st, zip, phone, lat, lon,
# incident_days_ago, report_filed_days_ago, line_type, rnd, dnc_listed,
# dnc_scrub_days_old, consent, consent_revoked, note_for_candidate
ROWS = [
 # --- Maryland: the jurisdiction with no accident-specific waiting period ---
 ("P001","Dara Ellingsworth","118 Quillfeather Ln","Rockville","MD","20850","3015550101",39.0840,-77.1528, 12, 11,"wireless","NO",  False, 3,  True,  False,""),
 ("P002","Marcus Vandeleur", "4402 Tinder Row",    "Silver Spring","MD","20901",'3015550102',39.0023,-77.0208, 40, 38,"landline","NO", False, 5,  False, False,""),
 ("P003","Junia Okpara",     "77 Bellwether Ct",   "Bethesda","MD","20814",  "2405550103",38.9847,-77.0947, 61, 60,"wireless","NO",  False, 2,  True,  True, ""),
 ("P004","Theo Marchetti",   "9 Ashgrove Terr",    "Gaithersburg","MD","20877","3015550104",39.1434,-77.2014, 8,  7,"wireless","NO",  True,  4,  True,  False,""),

 # --- Line-type edge cases: voip and unknown must route to most restrictive ---
 ("P005","Wren Calloway",    "2210 Foxbridge Way", "Wheaton","MD","20902",   "2405550105",39.0398,-77.0553, 25, 24,"voip",    "NO",  False, 6,  True,  False,""),
 ("P006","Ilse Brantwood",   "615 Marlowe Cross",  "Olney","MD","20832",     "3015550106",39.1532,-77.0669, 33, 31,"unknown", "NO",  False, 1,  True,  False,""),

 # --- Timezone traps. These two are why the window must come from geography ---
 ("P007","Ramona Quesnel",   "3140 Sandhurst Dr",  "El Paso","TX","79912",   "9155550107",31.8479,-106.5348, 44, 42,"wireless","NO", False, 3,  True,  False,""),
 ("P008","Odell Fairbrother","88 Cordgrass Ave",   "Pensacola","FL","32502", "8505550108",30.4213,-87.2169, 95, 92,"wireless","NO", False, 3,  True,  False,""),

 # --- Florida 60-day data gate: one inside the window, one outside ---
 ("P009","Sabine Trelawny",  "506 Heronwood Pl",   "Orlando","FL","32801",   "4075550109",28.5421,-81.3790, 21, 20,"wireless","NO", False, 2,  True,  False,""),
 ("P010","Casper Nunnelly",  "1712 Palmetto Reach","Tampa","FL","33602",     "8135550110",27.9518,-82.4573, 88, 86,"landline","NO",  False, 4,  True,  False,""),

 # --- Texas 31-day solicitation window: inside and outside ---
 ("P011","Verity Ashcombe",  "233 Longspur Trace", "Austin","TX","78701",    "5125550111",30.2688,-97.7431, 15, 14,"wireless","NO", False, 2,  True,  False,""),
 ("P012","Bram Steadwell",   "4019 Kestrel Bend",  "Houston","TX","77002",   "7135550112",29.7601,-95.3698, 47, 45,"wireless","NO", False, 3,  True,  False,""),

 # --- RND: only "NO" carries the safe harbour ---
 ("P013","Noor Halvorsen",   "820 Winnowing St",   "Rockville","MD","20852", "3015550113",39.0512,-77.1201, 29, 28,"wireless","NO_DATA",False,2,True,False,""),
 ("P014","Piers Oduya",      "17 Cattersley Mews", "Silver Spring","MD","20910","2405550114",38.9944,-77.0289,36,35,"wireless","YES",  False, 2,  True,  False,""),

 # --- DNC: listed, and a stale scrub ---
 ("P015","Elke Vantrease",   "3300 Rushmere Ln",   "Bethesda","MD","20817",  "3015550115",38.9912,-77.1414, 52, 50,"landline","NO",  True,  3,  True,  False,""),
 ("P016","Ozias Kettleburn", "141 Draypool Rd",    "Germantown","MD","20874", "2405550116",39.1732,-77.2717, 19, 18,"wireless","NO",  False, 45, True,  False,""),

 # --- Consent absent / revoked ---
 ("P017","Linnea Fitzwarren","2604 Sablewood Ct",  "Rockville","MD","20853", "3015550117",39.0918,-77.0898, 41, 39,"wireless","NO",  False, 2,  False, False,""),
 ("P018","Hollis Barrowman", "58 Grindlewood Ave", "Takoma Park","MD","20912","2405550118",38.9779,-77.0075, 27, 26,"wireless","NO",  False, 2,  True,  True, ""),

 # --- Geometry defects: out of envelope, and a bad snap ---
 ("P019","Cressida Naylon",  "9902 Farhill Bypass","Cumberland","MD","21502", "3015550119",39.6529,-78.7625, 30, 29,"wireless","NO", False, 2,  True,  False,"coordinate far outside Montgomery County"),
 ("P020","Ferris Aldencourt","74 Stonelap Hollow", "Poolesville","MD","20837","3015550120",39.1462,-77.4166, 22, 21,"wireless","NO", False, 2,  True,  False,"nearest road is ~400m away"),

 # --- Bulk: unremarkable records so the output has volume ---
 ("P021","Marisol Tenbrook", "410 Wickersham Row", "Rockville","MD","20851", "3015550121",39.0765,-77.1360, 55, 53,"wireless","NO", False, 2, True, False,""),
 ("P022","Anselm Pryde",     "1229 Halloway Reach","Silver Spring","MD","20904","2405550122",39.0653,-76.9822,63,61,"landline","NO",  False, 3, True, False,""),
 ("P023","Delphine Ravensby","36 Crowther Gate",   "Bethesda","MD","20816",  "3015550123",38.9603,-77.1215, 70, 68,"wireless","NO", False, 2, True, False,""),
 ("P024","Idris Halloran",   "5507 Marchbank Dr",  "Gaithersburg","MD","20878","3015550124",39.1157,-77.2405,17,16,"wireless","NO",  False, 4, True, False,""),
 ("P025","Solveig Marrable", "902 Thistledown Way","Chevy Chase","MD","20815","2405550125",38.9807,-77.0800, 34, 33,"wireless","NO", False, 2, False,False,""),
 ("P026","Gideon Ashfordly", "215 Pennywhistle Ct","Wheaton","MD","20906",   "3015550126",39.0605,-77.0508, 46, 44,"landline","NO",  False, 6, True, False,""),
 ("P027","Xiomara Belfount", "1140 Ketteridge Ln", "Rockville","MD","20855", "2405550127",39.1290,-77.1660, 11, 10,"wireless","NO", False, 2, True, False,""),
 ("P028","Rune Ollivander",  "63 Sparrowgate Rd",  "Damascus","MD","20872",  "3015550128",39.2887,-77.2036, 58, 57,"wireless","NO", False, 3, True, False,""),
 ("P029","Petra Hollingwood","4820 Ambersley Pl",  "Kensington","MD","20895","3015550129",39.0257,-77.0764, 24, 23,"voip",    "NO", False, 2, True, False,""),
 ("P030","Caspian Verduyn",  "7 Rillbank Terrace", "Potomac","MD","20854",   "2405550130",39.0182,-77.2086, 39, 37,"wireless","NO", False, 2, True, False,""),
 ("P031","Anneke Sturbridge","2933 Larkfield Dr",  "Dallas","TX","75201",    "2145550131",32.7831,-96.8067, 66, 64,"wireless","NO", False, 2, True, False,""),
 ("P032","Emrys Callowhill", "158 Verdigris Way",  "San Antonio","TX","78205","2105550132",29.4260,-98.4861, 51, 49,"landline","NO", False, 3, True, False,""),
 ("P033","Tamsin Ordway",    "6011 Cindermill Rd", "Fort Worth","TX","76102", "8175550133",32.7551,-97.3308, 28, 27,"wireless","NO", False, 2, True, False,""),
 ("P034","Lucian Ferrabee",  "44 Quarrystone Ave", "Miami","FL","33130",     "3055550134",25.7659,-80.2044, 74, 72,"wireless","NO", False, 2, True, False,""),
 ("P035","Rosalind Kepwick", "1806 Turnstile Ct",  "Jacksonville","FL","32202","9045550135",30.3268,-81.6580,102,99,"landline","NO", False, 4, True, False,""),
 ("P036","Barnaby Wroxeter", "329 Mallowdale Ln",  "St Petersburg","FL","33701","7275550136",27.7714,-82.6390, 33, 31,"wireless","NO",False, 2, True, False,""),
 ("P037","Freya Danesmoor",  "9520 Hartsmere Blvd","Rockville","MD","20850", "3015550137",39.0900,-77.1450, 43, 41,"wireless","NO", False, 2, True, False,""),
 ("P038","Osgood Trentham",  "77 Beacontree Row",  "Silver Spring","MD","20903","2405550138",39.0186,-76.9958, 20, 19,"wireless","NO",False, 2, True, False,""),
 ("P039","Ingrid Vasseltine","1315 Coppergate Dr", "Bethesda","MD","20852",  "3015550139",39.0290,-77.1180, 68, 66,"landline","NO",  False, 3, True, False,""),
 ("P040","Alaric Penhallow", "204 Stiverton Cross","Rockville","MD","20850", "2405550140",39.0836,-77.1512, 14, 13,"wireless","NO", False, 2, True, False,""),
]

HEADER = ["party_id","full_name","street_address","city","state","zip5","phone_e164",
          "party_latitude","party_longitude","incident_date","report_filing_date",
          "line_type_reported","rnd_response","on_national_dnc","dnc_scrub_age_days",
          "consent_on_file","consent_revoked","fixture_note"]

def main():
    w = csv.writer(sys.stdout, lineterminator="\n")
    w.writerow(HEADER)
    for (pid,name,street,city,st,z,phone,lat,lon,inc,filed,lt,rnd,dnc,scrub,cons,rev,note) in ROWS:
        w.writerow([pid,name,street,city,st,z,f"+1{phone}",lat,lon,
                    ago(inc),ago(filed),lt,rnd,str(dnc).lower(),scrub,
                    str(cons).lower(),str(rev).lower(),note])

if __name__ == "__main__":
    main()
