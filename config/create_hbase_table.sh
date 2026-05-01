#!/bin/bash
# Creates the HBase table (idempotent via exists check)

hbase shell <<'EOF'
exists 'web_traffic'
create 'web_traffic', 'cf'
exit
EOF
