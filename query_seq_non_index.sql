DROP PROCEDURE IF EXISTS seq_non_index;
delimiter #
CREATE PROCEDURE seq_non_index()
BEGIN
    DECLARE max_limit int DEFAULT 1000;
    DECLARE index_i int DEFAULT 0;

    WHILE index_i < max_limit DO
      SELECT * FROM TABLE_A WHERE A_VAL_SEQ = index_i;
      SET index_i = index_i + 1;
    END WHILE;
END #

delimiter ;

call seq_non_index();
