#!/usr/bin/env bash
# Build the liver pathogen panel: the general virus/bacteria panel plus the
# organisms that specifically infect or transform the liver and biliary tree.
#
# Requires the general panel first (build_virus_panel.sh + build_microbe_panel.sh);
# this script fetches only the liver-specific additions and concatenates.
#
# Add hepatotropic viruses, liver/biliary bacteria, and parasites missing from
# the general panel. HDV is a weak polyA marker because its circular RNA is
# ribozyme-processed; a negative call means little.
#
# Parasite genomes are megabase eukaryotic genomes scored on the uncalibrated
# density branch. Treat parasite calls as provisional until a known-positive
# dataset is scanned.
#
# Outputs: liver_panel.fa, liver_panel.mmi, liver_panel.tsv
# Usage: ./build_liver_panel.sh [OUT_PREFIX] [GENERAL_FA]
set -euo pipefail
OUT="${1:-liver_panel}"
GEN="${2:-panel.fa}"          # from build_virus_panel.sh
MIC="${3:-microbe.fa}"        # from build_microbe_panel.sh
E="https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

cat > "${OUT}.add.tsv" <<'EOF'
NC_001653.2	virus	HDV_weak_polyA
NC_002031.1	virus	Yellow_fever
NC_001454.1	virus	Adenovirus_F41
NC_004102.1	virus	HCV_nonpolyA_weak
NC_003977.2	virus	HBV
NC_001489.1	virus	Hepatitis_A
NC_001434.1	virus	Hepatitis_E
NC_006273.2	virus	HCMV_HHV5
NC_007605.1	virus	EBV_HHV4
NC_004297.1	pathogen	Leptospira_interrogans_c2
NC_005823.1	pathogen	Leptospira_interrogans_c1
NC_002971.4	pathogen	Coxiella_burnetii
NC_003317.1	pathogen	Brucella_melitensis_c1
NC_005956.1	pathogen	Bartonella_henselae
NC_003198.1	pathogen	Salmonella_Typhi
NC_004668.1	pathogen	Enterococcus_faecalis
NC_015500.1	pathogen	Veillonella_parvula
NC_021790.1	pathogen	Akkermansia_muciniphila
NC_004663.1	pathogen	Bacteroides_fragilis_rel
NC_003454.1	pathogen	Fusobacterium_nucleatum
NC_021224.1	parasite	Entamoeba_histolytica_partial
NC_044548.1	parasite	Echinococcus_granulosus_mt
NC_002546.1	parasite	Fasciola_hepatica_mt
NC_012147.1	parasite	Clonorchis_sinensis_mt
NC_021126.1	parasite	Opisthorchis_viverrini_mt
NC_002545.1	parasite	Schistosoma_mansoni_mt
NC_002544.1	parasite	Schistosoma_japonicum_mt
NC_002375.1	parasite	Plasmodium_falciparum_mt
NC_004916.1	parasite	Leishmania_donovani_kdna
EOF

echo "fetching $(wc -l < "${OUT}.add.tsv") liver-specific references..."
: > "${OUT}.add.raw.fa"
tmpd=$(mktemp -d); trap 'rm -rf "$tmpd"' EXIT
split -l 10 <(cut -f1 "${OUT}.add.tsv") "$tmpd/c_"
for c in "$tmpd"/c_*; do
    curl -s --max-time 400 "${E}/efetch.fcgi?db=nuccore&id=$(paste -sd, "$c")&rettype=fasta&retmode=text" \
        >> "${OUT}.add.raw.fa"
    sleep 1
done
awk -v map="${OUT}.add.tsv" '
BEGIN { while ((getline l < map) > 0) { split(l,f,"\t"); CLS[f[1]]=f[2]; LAB[f[1]]=f[3]
          b=f[1]; sub(/\.[0-9]+$/,"",b); CLS[b]=f[2]; LAB[b]=f[3] } }
/^>/ { a=substr($1,2); b=a; sub(/\.[0-9]+$/,"",b)
       k=(a in CLS)?a:((b in CLS)?b:"")
       print ">" (k==""?"unmapped|" a : CLS[k] "|" LAB[k] "|" a); next }
{ print }' "${OUT}.add.raw.fa" > "${OUT}.add.fa"
rm -f "${OUT}.add.raw.fa"

got=$(grep -c '^>' "${OUT}.add.fa"); want=$(wc -l < "${OUT}.add.tsv")
echo "fetched ${got}/${want} liver-specific"
if [ "$got" -lt "$want" ]; then
  echo "MISSING (bad/retired accessions -- fix or drop, do not ignore):"
  comm -23 <(cut -f1 "${OUT}.add.tsv" | sed 's/\.[0-9]*$//' | sort -u) \
           <(grep '^>' "${OUT}.add.fa" | sed 's/.*|//; s/\.[0-9]*$//' | sort -u)
fi

# concatenate with the general panels, dropping duplicate accessions
cat ${GEN:+"$GEN"} ${MIC:+"$MIC"} "${OUT}.add.fa" 2>/dev/null \
  | awk '/^>/{a=$0; sub(/.*\|/,"",a); keep=!(a in seen); seen[a]=1}
         keep{print}' > "${OUT}.fa"
echo "combined: $(grep -c '^>' "${OUT}.fa") refs, $(awk '!/^>/{s+=length($0)}END{print s}' "${OUT}.fa") bp"
grep '^>' "${OUT}.fa" | cut -d'|' -f1 | sort | uniq -c | sort -rn
minimap2 -x sr -d "${OUT}.mmi" "${OUT}.fa" 2>/dev/null
echo "wrote ${OUT}.fa ${OUT}.mmi ${OUT}.add.tsv"
