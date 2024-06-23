ALTER TABLE employee.groups
ADD CONSTRAINT uc_group_name_description UNIQUE (group_name, description(255));

ALTER TABLE employee.locations
ADD CONSTRAINT uc_location_city UNIQUE (location_name, address, city, country);

ALTER TABLE employee.users
ADD CONSTRAINT uc_users_email UNIQUE (user_name, email, phone_number);
