# find_double_fork.py

    python3 -m venv venv
    source venv/bin/activate

    pip install -r scripts/requirements.txt

    python find_double_fork.py  # will use localhost:9200, telos-mainnet-fork-* and telos-mainnet-delta-* as defaults

    python find_double_fork.py \
        --es-host 'http://your-es-host:port' \
        --fork-index 'your-fork-index-pattern' \
        --block-index 'your-block-index-pattern'
