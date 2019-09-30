CREATE DATABASE substrate_archive;
CREATE USER archive WITH ENCRYPTED PASSWORD 'default';
grant all privileges on database substrate_archive to archive;
