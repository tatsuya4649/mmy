CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE IF NOT EXISTS user (
    id INT AUTO_INCREMENT,
    name TEXT,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS post (
    id INT AUTO_INCREMENT,
    title TEXT,
    PRIMARY KEY (id)
);

DELIMITER $$
CREATE PROCEDURE generate_random_user(IN x INT, IN min_count INT)
BEGIN
  DECLARE counter INT DEFAULT 0;
  loop1:WHILE (counter < x) DO
    SELECT COUNT(*) FROM user INTO @ucount;
    IF @ucount = 0 THEN
      INSERT INTO user(name) VALUES (MD5(NOW()));
    ELSEIF @ucount > min_count THEN
		  LEAVE loop1;
    END IF;
    INSERT INTO user(name) SELECT MD5(name) FROM user;
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
      INSERT INTO post(title) VALUES (MD5(NOW()));
    ELSEIF @pcount > min_count THEN
      LEAVE loop2;
    END IF;
    INSERT INTO post(title) SELECT MD5(title) FROM post;
    SET counter = counter + 1;
  END WHILE loop2;
END
$$
DELIMITER ;

SET PERSIST local_infile= 1;