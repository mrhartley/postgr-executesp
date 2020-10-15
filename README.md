# Execute Stored Procedure for PostGreSQL
This Smart Service is a branch of the Execute Smart Service and allows a stored procedure to be executed while mapping data in and out. Result sets are returned as CDTs.  

Note that this version uses a 3rd party credential linked to the plugin which specifies:

url (jdbc connection)
username (username of db user)
password (password of db user)
driver (postgresql driver:  "org.postgresql.Driver")

This is a tacticle fix for a particular client and should only be used as such - please use the latest Execute Stored Procedure Plug-in where possible.
