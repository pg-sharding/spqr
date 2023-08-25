N = 16
import sys
import os

l = [[int(line.split()[0]), line.split()[1]] for line in sys.stdin.readlines()]
buckets = [[0, []] for _ in range(N)]
for w, p in l:
    b = min(buckets, key=lambda b: b[0])
    b[0] += w
    b[1].append(p)
matrix = []
for _, ps in buckets:
    if len(ps) == 0:
        continue
    tests = ';'.join([os.path.basename(p) for p in ps])
    matrix.append('GODOG_FEATURE_DIR=generatedFeatures GODOG_FEATURE="{}" make feature_test_ci'.format(tests))
print(matrix)
