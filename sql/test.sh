#!/bin/bash

COUNT=12
MIN_COUNT=10000
echo "======== Generate random user data ========"
time mysql -uroot -proot -s -N -e "USE test; CALL generate_random_user($COUNT, $MIN_COUNT)"
echo "======== Generate random post data ========"
time mysql -uroot -proot -s -N -e "USE test; CALL generate_random_post($COUNT, $MIN_COUNT)"

mysql -uroot -proot -s -N -e 'USE test; SELECT CONCAT("USER COUNT: ", COUNT(*)) from user';
mysql -uroot -proot -s -N -e 'USE test; SELECT CONCAT("POST COUNT: ", COUNT(*)) from post';

mysql -uroot -proot -e "\
SELECT \
    TABLE_NAME, CONCAT(ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024), 2), \"MB\") as \"Size in MB\" \
    FROM information_schema.TABLES \
    WHERE TABLE_SCHEMA=\"test\" AND (TABLE_NAME=\"user\" OR TABLE_NAME=\"post\") \
    ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC;"