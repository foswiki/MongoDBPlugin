# ---+ Extensions
# ---++ MongoDBPlugin
# host information for mongodb server
# see http://search.cpan.org/~kristina/MongoDB/lib/MongoDB/Connection.pm for more detailed info

# **STRING 30**
# hostname - can be a list of hosts
# eg: mongodb://host1[:port1][,host2[:port2],...[,hostN[:portN]]]
$Foswiki::cfg{MongoDBPlugin}{host} = 'mongodb://localhost:27017';

# **BOOLEAN EXPERT**
#Boolean indicating whether or not to reconnect if the connection is interrupted. Defaults to 1.
$Foswiki::cfg{MongoDBPlugin}{auto_reconnect} = 1;

# **BOOLEAN EXPERT**
#Boolean indication whether or not to connect automatically on object construction. Defaults to 1.
$Foswiki::cfg{MongoDBPlugin}{auto_connect} = 1;

# **NUMBER EXPERT**
#Connection timeout in milliseconds. Defaults to 20000.
$Foswiki::cfg{MongoDBPlugin}{timeout} = 20000;

# **NUMBER EXPERT**
#Connection timeout in milliseconds. Defaults to 20000.
#$Foswiki::cfg{MongoDBPlugin}{query_timeout} = 20000;

# **BOOLEAN EXPERT**
# If this is true, the driver will attempt to find a master given the list of hosts. 
$Foswiki::cfg{MongoDBPlugin}{find_master} = 0;

# **NUMBER EXPERT**
# The default number of mongod slaves to replicate a change to before reporting success for all operations on this collection.
$Foswiki::cfg{MongoDBPlugin}{w} = 1;
# **NUMBER EXPERT**
# The number of milliseconds an operation should wait for w slaves to replicate it.
$Foswiki::cfg{MongoDBPlugin}{wtimeout} = 1000;

# **SELECT 0,1,2 EXPERT**
# Mongo includes a profiling tool to analyze the performance of database operations.
#    0 - off,
#    1 - log slow operations (by default, >100ms is considered slow), 
#    2 - log all operations
$Foswiki::cfg{Plugins}{MongoDBPlugin}{ProfilingLevel} = 0;

# **STRING 30**
# username
#$Foswiki::cfg{MongoDBPlugin}{username} = undef;

# **PASSWORD**
# password
#$Foswiki::cfg{MongoDBPlugin}{password} = undef;


# **BOOLEAN**
# enable debug logging
$Foswiki::cfg{MONITOR}{'Foswiki::Plugins::MongoDBPlugin'} = 0;
# **BOOLEAN**
# enable hoisting
$Foswiki::cfg{Plugins}{MongoDBPlugin}{ExperimentalCode} = 1;
