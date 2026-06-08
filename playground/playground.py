from pysradb.sraweb import SRAweb

sra = SRAweb()
df = sra.gse_to_pmid(
    [
        "E-MTAB-15098",
    ]
)
print(df)
