#!/bin/bash

echo "============================"
echo "CREATE TSL/SSL KEYS"
echo "============================"

mysql_ssl_rsa_setup
cd /var/lib/mysql
openssl rsa -in client-key.pem -out client-key.pem
openssl rsa -in server-key.pem -out server-key.pem

ls -l /var/lib/mysql/*.pem

test.sh
docker-entrypoint.sh
mysql -uroot -proot -s -N -e "SHOW VARIABLES LIKE '%ssl%';"

COUNT=12
MIN_COUNT=10000
echo "======== Generate random post data ========"
time mysql -uroot -proot -s -N -e "USE test; CALL generate_random_post($COUNT, $MIN_COUNT)"

mysql -uroot -proot -s -N -e 'USE test; SELECT CONCAT("POST COUNT: ", COUNT(*)) from post';

mysql -uroot -proot -e "\
SELECT \
    TABLE_NAME, CONCAT(ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024), 2), \"MB\") as \"Size in MB\" \
    FROM information_schema.TABLES \
    WHERE TABLE_SCHEMA=\"test\" AND TABLE_NAME=\"post\" \
    ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC;"