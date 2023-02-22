CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE IF NOT EXISTS user (
    id INT AUTO_INCREMENT,
    md5 VARCHAR(100) UNIQUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    name TEXT,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS post (
    id VARCHAR(100),
    md5 VARCHAR(100) UNIQUE,
    title TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

DELIMITER $$
CREATE PROCEDURE generate_random_user(IN x INT, IN min_count INT)
BEGIN
  DECLARE counter INT DEFAULT 0;
  loop1:WHILE (counter < x) DO
    SELECT COUNT(*) FROM user INTO @ucount;
    IF @ucount = 0 THEN
      INSERT INTO user(md5) VALUES (MD5(UUID()));
    ELSEIF @ucount > min_count THEN
		  LEAVE loop1;
    END IF;
    INSERT INTO user(md5) SELECT MD5(UUID()) FROM user;
    SET counter = counter + 1;
  END WHILE loop1;
END;
$$

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