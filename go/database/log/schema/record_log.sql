CREATE TABLE log (
                        id   BIGSERIAL PRIMARY KEY,
                        resource text NOT NULL,
                        quota int NOT NULL,
                        subject text,
                        UNIQUE NULLS NOT DISTINCT (resource, subject)
);