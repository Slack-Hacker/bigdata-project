<<<<<<< HEAD
#!/bin/bash
# Creates the HBase table (idempotent via exists check)

hbase shell <<'EOF'
exists 'web_traffic'
create 'web_traffic', 'cf'
exit
EOF
=======
echo "create 'web_traffic','cf'" | hbase shell

>>>>>>> 3e8e9d96940598f46741697fb7af1a50fd08e4b3
