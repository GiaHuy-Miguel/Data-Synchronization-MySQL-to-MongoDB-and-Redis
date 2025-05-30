CREATE TABLE Users (
    users_id BIGINT PRIMARY KEY,
    login VARCHAR(255),
    gravatar_id VARCHAR(255),
    url VARCHAR(255),
    avatar_url VARCHAR(255)
);

CREATE TABLE Repo (
    repo_id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    url VARCHAR(255)
);