#!/usr/bin/env bash
# Build a bacterial panel and minimap2 index for virus_scan_srr.sh.
#
# Bacteria do not polyadenylate mRNA, so 10x mostly captures rRNA through
# internal priming. A negative call is weaker than a viral negative.
#
# Genome breadth is not calibrated for bacteria: a real infection can cover a
# tiny fraction of a large genome. Treat bacterial PASS calls as provisional.
#
# Bacteria have no impossible-in-human negative control. `kitome` entries are
# reagent contaminants; if they score like pathogens, the run is background.
#
# 16S is too conserved for 38 bp unitigs to resolve species. Whole genomes cost
# more but keep species-level specificity.
#
# Classes: pathogen (preterm-relevant) | flora (normal vaginal, expected) |
#          kitome (reagent contaminant control) | culture (cell-culture contaminant)
#
# Outputs: microbe.fa, microbe.mmi, microbe.tsv
# Usage: ./build_microbe_panel.sh [OUT_PREFIX]     # default ./microbe
set -euo pipefail
OUT="${1:-microbe}"
E="https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

cat > "${OUT}.tsv" <<'EOF'
NC_002162.1	pathogen	Ureaplasma_parvum
NC_011374.1	pathogen	Ureaplasma_urealyticum
NC_013511.1	pathogen	Mycoplasma_hominis
NC_000908.2	pathogen	Mycoplasma_genitalium
NC_004116.1	pathogen	Strep_agalactiae_GBS
NC_003454.1	pathogen	Fusobacterium_nucleatum
NC_013721.1	pathogen	Gardnerella_vaginalis
NC_000913.3	pathogen	Escherichia_coli
NC_003210.1	pathogen	Listeria_monocytogenes
NC_002737.2	pathogen	Strep_pyogenes
NC_007795.1	pathogen	Staph_aureus
NC_016845.1	pathogen	Klebsiella_pneumoniae
NC_000907.1	pathogen	Haemophilus_influenzae
NC_000117.1	pathogen	Chlamydia_trachomatis
NC_002946.2	pathogen	Neisseria_gonorrhoeae
NC_021490.2	pathogen	Treponema_pallidum
NC_000962.3	pathogen	Mycobacterium_tuberculosis
NC_009614.1	pathogen	Bacteroides_vulgatus
NC_004350.2	pathogen	Strep_mutans
NC_014106.1	flora	Lactobacillus_crispatus
NC_008497.1	flora	Lactobacillus_brevis
NC_004567.2	flora	Lactobacillus_plantarum
NC_006085.1	kitome	Cutibacterium_acnes
NC_010682.1	kitome	Ralstonia_pickettii
NC_004463.1	kitome	Bradyrhizobium_diazoefficiens
NC_010002.1	kitome	Delftia_acidovorans
NC_009085.1	kitome	Acinetobacter_baumannii
NC_014448.1	culture	Mycoplasma_hyorhinis
NC_006360.1	culture	Mycoplasma_hyopneumoniae
EOF

echo "panel: $(wc -l < "${OUT}.tsv") genomes"
: > "${OUT}.raw.fa"
tmpd=$(mktemp -d); trap 'rm -rf "$tmpd"' EXIT
split -l 10 <(cut -f1 "${OUT}.tsv") "$tmpd/chunk_"      # bacterial genomes are big
for c in "$tmpd"/chunk_*; do
    curl -s --max-time 600 "${E}/efetch.fcgi?db=nuccore&id=$(paste -sd, "$c")&rettype=fasta&retmode=text" \
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
echo "fetched ${got}/${want}: pathogen=$(grep -c '^>pathogen|' "${OUT}.fa" || true) flora=$(grep -c '^>flora|' "${OUT}.fa" || true) kitome=$(grep -c '^>kitome|' "${OUT}.fa" || true) culture=$(grep -c '^>culture|' "${OUT}.fa" || true)"
echo "bp: $(awk '!/^>/{s+=length($0)}END{print s}' "${OUT}.fa")"
if [ "$got" -lt "$want" ]; then
  echo "MISSING (bad/retired accessions -- fix or drop, do not ignore):"
  comm -23 <(cut -f1 "${OUT}.tsv" | sed 's/\.[0-9]*$//' | sort -u) \
           <(grep '^>' "${OUT}.fa" | sed 's/.*|//; s/\.[0-9]*$//' | sort -u)
fi
minimap2 -x sr -d "${OUT}.mmi" "${OUT}.fa" 2>/dev/null
echo "wrote ${OUT}.fa ${OUT}.mmi ${OUT}.tsv"
