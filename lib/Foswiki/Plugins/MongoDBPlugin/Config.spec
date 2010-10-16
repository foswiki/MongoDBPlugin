# ---+ Extensions
# ---++ MongoDBPlugin
# host information for mongodb server
# **STRING 30**
# hostname
$Foswiki::cfg{MongoDBPlugin}{host} = 'localhost';
# **STRING 30**
# port
$Foswiki::cfg{MongoDBPlugin}{port} = '27017';
# **STRING 30**
# username
$Foswiki::cfg{MongoDBPlugin}{username} = '';
# **PASSWORD**
# password
$Foswiki::cfg{MongoDBPlugin}{password} = '';
# **STRING 30**
# database
$Foswiki::cfg{MongoDBPlugin}{database} = 'foswiki';
# **BOOLEAN**
# update the mongoDB database on Save (when using Mongo as an accellerator, and not as a store)
$Foswiki::cfg{Plugins}{MongoDBPlugin}{EnableOnSaveUpdates} = 1; 

