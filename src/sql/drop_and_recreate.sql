CREATE TABLE user (
	id INT(9) NOT NULL AUTO_INCREMENT PRIMARY KEY,	
	user_name VARCHAR(20) NOT NULL,
	first_name VARCHAR(20) DEFAULT NULL ,
	last_name VARCHAR(20) DEFAULT NULL ,
	email VARCHAR(50) NOT NULL,		
	password VARCHAR(75) NOT NULL,
	title VARCHAR(40) DEFAULT NULL ,
	company VARCHAR(40) DEFAULT NULL ,
    country VARCHAR(250) DEFAULT NULL,
    phone VARCHAR(20) DEFAULT NULL,
    
	created DATE, -- DEFAULT SYSDATE
	-- activated DATE,
	-- welcomed DATE DEFAULT NULL,
	-- password_last_set DATE,
	-- account_expiration_date DATE,
	account_disabled INT(4) DEFAULT 0  NOT NULL,
	account_lockout INT(4) DEFAULT 0  NOT NULL,
	last_logon DATE		 
);

CREATE UNIQUE INDEX unique_user_name ON user (user_name); 
CREATE UNIQUE INDEX unique_user_email ON user (email); 
    
CREATE TABLE group_table (
    id INT(9) NOT NULL AUTO_INCREMENT PRIMARY KEY,
    group_name VARCHAR(30) NOT NULL,
    description VARCHAR(50),
    disabled INT(4) NOT NULL DEFAULT 0,
    created DATE
);

CREATE TABLE user_groups (
    user_id INT(9) NOT NULL,
    group_id INT(9) NOT NULL,
    PRIMARY KEY (user_id, group_id),
    CONSTRAINT fk_user_groups_user FOREIGN KEY (user_id) REFERENCES user(id),
    CONSTRAINT fk_user_groups_group FOREIGN KEY (group_id) REFERENCES group_table(id)
);
