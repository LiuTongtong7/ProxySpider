DROP TABLE IF EXISTS proxies;
CREATE TABLE IF NOT EXISTS proxies (
    ip CHAR(15),
    port INT,
    protocol CHAR(5),
    ds DATETIME DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ip, port, protocol)
);