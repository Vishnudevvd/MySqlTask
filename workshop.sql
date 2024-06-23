-- Create Groups table
CREATE TABLE `Groups` (
    group_id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    group_name VARCHAR(50),
    description TEXT,
    creation_date DATE
);

-- Create Locations table
CREATE TABLE `Locations` (
    location_id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    location_name VARCHAR(50),
    address VARCHAR(100),
    city VARCHAR(50),
    country VARCHAR(50),
    group_id INT,
    FOREIGN KEY (group_id) REFERENCES `Groups`(group_id)
);

-- Create Users table
CREATE TABLE `Users` (
    user_id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    user_name VARCHAR(50),
    email VARCHAR(100),
    phone_number VARCHAR(10),
    location_id INT,
    FOREIGN KEY (location_id) REFERENCES Locations(location_id)
);

