#!/usr/bin/env bash
# Build the virus panel and minimap2 index for virus_scan_srr.sh.
#
# Supersedes build_virus_ref.sh. The panel combines curated human/mouse viruses,
# controls, and Papillomavirus Episteme accessions. PAVE uses original GenBank
# accessions such as HPV16=K02718, not RefSeq accessions such as NC_001526.
#
# Entries are fetched from NCBI by accession. Names are `class|label|accession`:
#   virus     infection candidate
#   animalPV  animal papillomavirus; a human-tissue hit is a false positive
#   vector    vector backbone; a hit is usually reagent, not infection
#   spike     PhiX etc.
#
# Core viruses are included when polyA 10x should capture their transcripts. HCV
# is a weak control because its 3' end is poly(U/UC).
#
# Outputs: panel.fa, panel.mmi, panel.tsv
# Usage: ./build_virus_panel.sh [OUT_PREFIX]     # default ./panel
set -euo pipefail
OUT="${1:-panel}"
E="https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

# ---- CORE: accession<TAB>class<TAB>label -----------------------------------
cat > "${OUT}.core.tsv" <<'EOF'
NC_045512.2	virus	SARS-CoV-2
NC_006273.2	virus	HCMV_HHV5
NC_007605.1	virus	EBV_HHV4
NC_009333.1	virus	KSHV_HHV8
NC_001664.4	virus	HHV6A
NC_000898.1	virus	HHV6B
NC_001716.2	virus	HHV7
NC_001806.2	virus	HSV1
NC_001798.2	virus	HSV2
NC_001348.1	virus	VZV_HHV3
NC_003977.2	virus	HBV
NC_004102.1	virus	HCV_nonpolyA_weak
NC_001802.1	virus	HIV1
NC_001722.1	virus	HIV2
NC_001436.1	virus	HTLV1
NC_001538.1	virus	BK_polyomavirus
NC_001699.1	virus	JC_polyomavirus
NC_010277.2	virus	Merkel_cell_polyomavirus
NC_000883.2	virus	Parvovirus_B19
NC_002076.2	virus	TTV_anellovirus
NC_001405.1	virus	Adenovirus_C
NC_011203.1	virus	Adenovirus_B
NC_010956.1	virus	Adenovirus_D
NC_038235.1	virus	RSV
NC_039199.1	virus	HMPV
NC_001796.2	virus	Parainfluenza_3
NC_003461.1	virus	Parainfluenza_1
NC_001498.1	virus	Measles
NC_002200.1	virus	Mumps
NC_001545.2	virus	Rubella
NC_002058.3	virus	Poliovirus
NC_001472.1	virus	Coxsackievirus_B3
NC_038306.1	virus	Enterovirus_A71
NC_038311.1	virus	Rhinovirus_A
NC_001489.1	virus	Hepatitis_A
NC_001434.1	virus	Hepatitis_E
NC_001477.1	virus	Dengue_1
NC_001474.2	virus	Dengue_2
NC_001475.2	virus	Dengue_3
NC_002640.1	virus	Dengue_4
NC_035889.1	virus	Zika
NC_009942.1	virus	West_Nile
NC_044932.1	virus	Norovirus_GII
NC_002023.1	virus	InfluenzaA_PB2
NC_002021.1	virus	InfluenzaA_PB1
NC_002022.1	virus	InfluenzaA_PA
NC_002017.1	virus	InfluenzaA_HA
NC_002019.1	virus	InfluenzaA_NP
NC_002018.1	virus	InfluenzaA_NA
NC_002016.1	virus	InfluenzaA_M
NC_002020.1	virus	InfluenzaA_NS
NC_002204.1	virus	InfluenzaB_PB1
NC_002207.1	virus	InfluenzaB_HA
NC_002211.1	virus	InfluenzaB_NP
NC_001352.1	virus	HPV2a_legacy
NC_004291.1	virus	LCMV_S
NC_004294.1	virus	LCMV_L
NC_048217.1	virus	Murine_hepatitis_virus
NC_001503.1	virus	MMTV
NC_001501.1	virus	Murine_leukemia_virus
NC_008311.1	virus	Murine_norovirus
NC_001366.1	virus	Theilers_virus
NC_001510.1	virus	Mouse_parvovirus_MVM
NC_001552.1	vector	Sendai_virus_reprogramming
NC_001669.1	vector	SV40_largeT
NC_001401.2	vector	AAV2
NC_001422.1	spike	PhiX174
EOF

# ---- PAVE: scrape both reference_genomes tables ----------------------------
echo "scraping PAVE..."
for p in human animal; do
  curl -s --max-time 90 -L "https://pave.niaid.nih.gov/explore/reference_genomes/${p}_genomes" \
       -o "${OUT}.pave_${p}.html"
done
python3 - "$OUT" <<'PY'
import re, sys
out = sys.argv[1]
rows, seen = [], set()
for kind, cls in (("human", "virus"), ("animal", "animalPV")):
    h = open(f"{out}.pave_{kind}.html").read()
    # records carry: "genbank": {... "text": "ACC"} ... "pave_id": "..." ... "virus_id": "..."
    for acc, host, pid, vid in re.findall(
            r'"genbank":\s*\{[^}]*"text":\s*"([^"]+)"\}[^{]*?"host_common_name":\s*"?([^",]*)"?'
            r'.*?"pave_id":\s*"([^"]*)".*?"virus_id":\s*"([^"]*)"', h, re.S):
        acc = acc.strip()
        if not re.fullmatch(r'[A-Za-z0-9_]{5,12}(\.\d+)?', acc) or acc in seen:
            continue
        seen.add(acc)
        lab = vid if vid and vid != 'null' else (pid or acc)
        rows.append((acc, cls, re.sub(r'[^A-Za-z0-9]+', '_', lab)[:28]))
with open(f"{out}.pave.tsv", "w") as f:
    f.writelines(f"{a}\t{c}\t{l}\n" for a, c, l in rows)
print(f"PAVE: {len(rows)} genomes "
      f"({sum(1 for r in rows if r[1]=='virus')} human / "
      f"{sum(1 for r in rows if r[1]=='animalPV')} animal)")
PY
rm -f "${OUT}".pave_*.html

# CORE first so a duplicated accession keeps its curated label
awk -F'\t' '!seen[$1]++' "${OUT}.core.tsv" "${OUT}.pave.tsv" > "${OUT}.tsv"
echo "panel: $(wc -l < "${OUT}.tsv") genomes"

# ---- fetch + rename --------------------------------------------------------
: > "${OUT}.raw.fa"
tmpd=$(mktemp -d); trap 'rm -rf "$tmpd"' EXIT
split -l 150 <(cut -f1 "${OUT}.tsv") "$tmpd/chunk_"
for c in "$tmpd"/chunk_*; do
    curl -s --max-time 300 "${E}/efetch.fcgi?db=nuccore&id=$(paste -sd, "$c")&rettype=fasta&retmode=text" \
        >> "${OUT}.raw.fa"
    sleep 1
done
awk -v map="${OUT}.tsv" '
BEGIN { while ((getline l < map) > 0) { split(l,f,"\t"); CLS[f[1]]=f[2]; LAB[f[1]]=f[3]
          b=f[1]; sub(/\.[0-9]+$/,"",b); CLS[b]=f[2]; LAB[b]=f[3] } }
/^>/ { a=substr($1,2); b=a; sub(/\.[0-9]+$/,"",b)
       k = (a in CLS) ? a : ((b in CLS) ? b : "")
       print ">" (k=="" ? "unmapped|" a : CLS[k] "|" LAB[k] "|" a); next }
{ print }' "${OUT}.raw.fa" > "${OUT}.fa"
rm -f "${OUT}.raw.fa"

got=$(grep -c '^>' "${OUT}.fa"); want=$(wc -l < "${OUT}.tsv")
echo "fetched ${got}/${want}: virus=$(grep -c '^>virus|' "${OUT}.fa") animalPV=$(grep -c '^>animalPV|' "${OUT}.fa") vector=$(grep -c '^>vector|' "${OUT}.fa") spike=$(grep -c '^>spike|' "${OUT}.fa") unmapped=$(grep -c '^>unmapped|' "${OUT}.fa" || true)"
echo "bp: $(awk '!/^>/{s+=length($0)}END{print s}' "${OUT}.fa")"
if [ "$got" -lt "$want" ]; then
  echo "MISSING (bad/retired accessions -- fix or drop them, do not ignore):"
  comm -23 <(cut -f1 "${OUT}.tsv" | sed 's/\.[0-9]*$//' | sort -u) \
           <(grep '^>' "${OUT}.fa" | sed 's/.*|//; s/\.[0-9]*$//' | sort -u)
fi
minimap2 -x sr -d "${OUT}.mmi" "${OUT}.fa" 2>/dev/null
echo "wrote ${OUT}.fa ${OUT}.mmi ${OUT}.tsv"
