import csv

with open("./dump/test.txt", "r") as f:
    sample = f.read(16 * 1024)

try:
    dialect = csv.Sniffer().sniff(sample)
    reader = csv.reader(sample.splitlines(), dialect)
    row = next(reader)
except csv.Error:
    row = sample.splitlines()[0].split("\t")


row = [r for r in row if r.strip()]

if len(row) == 1:
    print("newline separated")
elif dialect.delimiter == "\t":
    print("tsv")
elif dialect.delimiter == " ":
    print("space delimited")
elif dialect.delimiter == ",":
    print("csv")
elif dialect.delimiter == ";":
    print("semi-colon separated")
