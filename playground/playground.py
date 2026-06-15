import re

# s = "1-Education and Extension Organization (AREEO),2- Lorestan University"
s = "AARHUS UNIVERSITY, DENMARK"
result = [x.strip() for x in re.split(r"[,;]?\s*\d+[.\-]\s*", s) if x]
print(result)
