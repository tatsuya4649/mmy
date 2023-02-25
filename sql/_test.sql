CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE IF NOT EXISTS post (
    id VARCHAR(100),
    md5 VARCHAR(100) UNIQUE,
    title TEXT,
    duration FLOAT,
    do_on DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS _auto_increment(
    id INT AUTO_INCREMENT,
    PRIMARY KEY (id)
);

DELIMITER $$
CREATE PROCEDURE generate_random_post(IN x INT, IN min_count INT)
BEGIN
  DECLARE counter INT DEFAULT 0;
  loop2:WHILE (counter < x) DO
    SELECT COUNT(*) FROM post INTO @pcount;
    IF @pcount = 0 THEN
      INSERT INTO post(id, md5) VALUES (MD5(UUID()), MD5(UUID()));
    ELSEIF @pcount > min_count THEN
      LEAVE loop2;
    END IF;
    INSERT INTO post(id, md5) SELECT MD5(UUID()), MD5(UUID()) FROM post;
    SET counter = counter + 1;
  END WHILE loop2;
END
$$
DELIMITER ;

SET PERSIST local_infile= 1;