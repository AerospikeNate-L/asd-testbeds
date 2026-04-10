cd /home/nlarsen/aeroWork/repos/server_repos/aerospike-server-enterprise

for bm in BM_Find_Short BM_Find_Long BM_Find_Unicode_NFC BM_Replace_Short BM_Replace_Long BM_ReplaceAll; do
    tests/scripts/bench.sh --bench="$bm" -n 3 -t 3 --build
done

tests/scripts/bench_compare.sh tests/results/baseline.5.March.26/