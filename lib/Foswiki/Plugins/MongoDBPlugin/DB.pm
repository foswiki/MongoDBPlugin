# Plugin for Foswiki - The Free and Open Source Wiki, http://foswiki.org/
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details, published at
# http://www.gnu.org/copyleft/gpl.html

=pod

---+ package Foswiki::Plugins::MongoDBPlugin::DB


=cut

package Foswiki::Plugins::MongoDBPlugin::DB;

# Always use strict to enforce variable scoping
use strict;
use MongoDB;
use MongoDB::Cursor;
use Assert;
use Data::Dumper;
use Time::HiRes ();
use Tie::IxHash          ();
use Foswiki::Func;

#lets declare it ok to run queries on slaves.
#http://search.cpan.org/~kristina/MongoDB-0.42/lib/MongoDB/Cursor.pm#slave_okay
$MongoDB::Cursor::slave_okay = 1;

#I wish
#use constant MONITOR => $Foswiki::cfg{MONITOR}{'Foswiki::Plugins::MongoDBPlugin'} || 0;
use constant MONITOR => 0;
use constant MONITOR_INDEX => 0;

sub new {
    my $class  = shift;
    my $params = shift;

    my $self =
      bless( { %$params, session => $Foswiki::Func::SESSION }, $class );

    $Foswiki::Func::SESSION->{MongoDB} = $self;
    return $self;
}

sub query {
    my $self           = shift;
    my $database       = shift;
    my $collectionName = shift;
    my $ixhQuery       = shift;
    my $queryAttrs     = shift || {};

    #remove all the history versions from the result.
    if (ref($ixhQuery) eq 'Tie::IxHash') {
        $ixhQuery->Unshift('_history' => {'$exists' => 0});
    } else {
        $ixhQuery->{_history} = {'$exists' => 0};
    }    

    my $startTime = [Time::HiRes::gettimeofday];
    
    my $collection = $self->_getCollection($database, 'current');
    print STDERR "searching mongo : "
      . Dumper($ixhQuery) . " , "
      . Dumper($queryAttrs) . "\n"
      if MONITOR;


#debugging for upstream
#print STDERR "----------------------------------------------------------------------------------\n" if DEBUG;
my $db   = $self->_getDatabase( $database );
#$db->run_command({"profile" => 2});

#use Devel::Peek;
#Dump($db->run_command({"count" => 'current', "query" => $ixhQuery}));
#print STDERR "----------------------------------------------------------------------------------\n";
    my $long_count = $db->run_command({"count" => 'current', "query" => $ixhQuery});
    my $cursor = $collection->query( $ixhQuery, $queryAttrs );
    #TODO: this is to make sure we're getting the cursor->count before anyone uses the cursor.
    my $count = $long_count;
    if ($count > 100) {
        $cursor->{noCache} = 1;
        $cursor = $cursor->fields({_web=>1, _topic=>1});
    }

##    my $real_count = $cursor->count;

##if (($cursor->count == 0) and $cursor->has_next()) {
	#fake it
##	$real_count = $long_count->{n};
	$cursor->{real_count} = $long_count->{n};
##}

##use Data::Dumper;
##    print STDERR "found "
##      . $cursor->count
##      . " (long_count = ".Dumper($long_count).") "
##      . " _BUT_ has_next is "
##      . ( $cursor->has_next() ? 'true' : 'false' ) . "\n" if MONITOR;

#more debugging
#print STDERR "get_collection(system.profile)".Dumper($db->get_collection("system.profile")->find->all)."\n";
#$db->run_command({"profile" => 0});
#print STDERR "----------------------------------------------------------------------------------\n" if DEBUG;

    #end timer
    my $endTime = [Time::HiRes::gettimeofday];
    my $timeDiff = Time::HiRes::tv_interval( $startTime, $endTime );
    print STDERR "query took $timeDiff\n" if MONITOR;
    push(@{$Foswiki::Func::SESSION->{MongoDB}->{lastQueryTime}}, $timeDiff);

    return $cursor;
}

sub update {
    my $self           = shift;
    my $database       = shift;
    my $collectionName = shift;
    my $address        = shift;
    my $hash           = shift;
    my $history_only   = shift; #set to true when importing so that we don't make a 'current rev' entry

    #    use Data::Dumper;
    print STDERR "+++++ mongo update $database, $collectionName, $address \n" if MONITOR;
    #print STDERR " == ".Dumper($hash)."\n" if MONITOR;

    my $collection = $self->_getCollection($database, $collectionName);

#TODO: not the most efficient place to create and index, but I want to be sure, to be sure.
    $self->ensureIndex( $collection, { _topic => 1 }, { name => '_topic' } );
#    $self->ensureIndex(
#        $collection,
#        { _topic => 1,             _web   => 1 },
#        { name   => '_topic:_web', unique => 1 }
#    );
    $self->ensureIndex(
        $collection,
        { _topic => 1,             'TOPICINFO.rev'   => -1 },
        { name   => '_topic:_rev' }
    );

    $self->ensureIndex(
        $collection,
        { 'TOPICINFO.author' => 1 },
        { name               => 'TOPICINFO.author' }
    );
    $self->ensureIndex(
        $collection,
        { 'TOPICINFO.date' => 1 },
        { name             => 'TOPICINFO.date' }
    );
    $self->ensureIndex(
        $collection,
        { 'TOPICPARENT.name' => 1 },
        { name               => 'TOPICPARENT.name' }
    );
    $self->ensureIndex(
        $collection,
        { 'CREATEINFO.author' => 1 },
        { name               => 'CREATEINFO.author' }
    );
    $self->ensureIndex(
        $collection,
        { 'CREATEINFO.date' => 1 },
        { name             => 'CREATEINFO.date' }
    );


#TODO: maybe should use the auto indexed '_id' (or maybe we can use this as a tuid - unique foreach rev of each topic..)
#then again, atm, its totally random, so may be good for sharding.
    $hash->{address} = $address.'@'.$hash->{'TOPICINFO'}->{rev};

    #indicate that all existing topics are _not_ the HEAD anymore
    if ($history_only == 0) {
        my $object = $collection->find_one({ _topic=>$hash->{_topic}, '_history' => {'$exists' => 0}});
        if (defined($object->{_id})) {
            $object->{_history} = 1;
            $collection->save($object);
        }
        #add a new entry into the versions collection too (this is the duplicate version thing)
#        print STDERR 'update '.$address."\n";
#        $collection->update(
#            { address  => $address },
#            { address  => $address, %$hash },
#            { 'upsert' => 1 }
#        );
    }
    
    
    $hash->{_history} = 1 if ($history_only ==1);
    
    #print STDERR "making new entry ".$hash->{address}."\n";
    $collection->update(
        { address  => $hash->{address} },
        { address  => $hash->{address}, %$hash },
        { 'upsert' => 1 }
    );

}

#BUGGER. compound indexes won't help with large queries
#> db.current.dropIndexes();
#{
#	"nIndexesWas" : 2,
#	"msg" : "non-_id indexes dropped for collection",
#	"ok" : 1
#}
#> db.current.find().sort({_topic:1})
#error: {
#	"$err" : "too much data for sort() with no index.  add an index or specify a smaller limit",
#	"code" : 10128
#}
#> db.current.ensureIndex({_web:1, _topic:1, 'TOPICINFO.date':1, 'TOPICINFO.author': 1});
#> db.current.find().sort({_topic:1})
#error: {
#	"$err" : "too much data for sort() with no index.  add an index or specify a smaller limit",
#	"code" : 10128
#}
#>
#    $collection->ensure_index( { 'TOPICINFO.author' => 1, 'TOPICINFO.date' => 1, 'TOPICPARENT.name' => 1  } );
#    $collection->ensure_index( { 'TOPICINFO.author' => -1, 'TOPICINFO.date' => -1, 'TOPICPARENT.name' => -1  } );
#MongoDB's ensure_index causes the server to re0index, even if that index already exists, so we need to wrap it.
sub ensureIndex {
    my $self       = shift;
    my $collection = shift;#must be a collection obj
    my $indexRef   = shift;    #can be a hashref or an ixHash
    my $options    = shift;

    ASSERT( defined( $options->{name} ) ) if DEBUG;
    
    #force the index to be made in the background - so we don't timeout the cursor
    $options->{background} = 1;

    if ( ref($collection) eq '' ) {
die 'must convert $collection param to be a collection obj';
        #convert name of collection to collection obj
        #$collection = $self->_getCollection($database, $collection);
    }

    #cache the indexes we know about
    if ( not defined( $self->{mongoDBIndexes} ) ) {
        my @indexes = $collection->get_indexes();
        $self->{mongoDBIndexes} = \@indexes;
    }
    foreach my $index ( @{ $self->{mongoDBIndexes} } ) {

        #print STDERR "we already have:  ".$index->{name}." index\n";
        if ( $options->{name} eq $index->{name} ) {

            #print STDERR "already exists " . $options->{name} . " index\n" if MONITOR_INDEX;

            #already exists, do nothing.
            return;
        }
    }
    if ( scalar( @{ $self->{mongoDBIndexes} } ) >= 40 ) {
        print STDERR
"*******************ouch. MongoDB can only have 40 indexes per collection : " . $options->{name} . "\n"
          if MONITOR_INDEX;
        return;
    }
    print STDERR "creating " . $options->{name} . " index\n" if MONITOR_INDEX;

    #TODO: consider doing these in a batch at the end of a request, or?
    $collection->ensure_index( $indexRef, $options );
    undef $self->{mongoDBIndexes};    #clear the cache :/
}


sub remove {
    my $self           = shift;
    my $database       = shift;
    my $collectionName = shift;
    my $mongoDbQuery           = shift;

    if (scalar(keys(%$mongoDbQuery)) == 0) {
        #remove web - so drop database.
        print STDERR "...........Dropping $database\n" if MONITOR;
        my $db   = $self->_getDatabase($database);
        $db->drop();
    } else {
        my $collection = $self->_getCollection($database, $collectionName);
        print STDERR "...........remove ".join(',', keys(%$mongoDbQuery))."\n" if MONITOR;
        $collection->remove($mongoDbQuery);
    }
}

sub updateSystemJS {
    my $self = shift;
    my $database       = shift;
    my $functionname = shift;
    my $sourcecode = shift;
    
    my $collection = $self->_getCollection($database, 'system.js');

use MongoDB::Code;
    my $code = MongoDB::Code->new('code' => $sourcecode);
   
    $collection->save(
        {
            _id => $functionname,
            value => $code
        }
    );
}


#######################################################
sub getDatabaseName {
    my $self           = shift;
    my $web       = shift;
    
    #using webname as database name, so we need to sanitise
    #replace / with __ and pre-pend foswiki__ ?
    $web =~ s/\//__/g;
    #remove the 'dots' too.
    $web =~ s/\./__/g;
    return 'foswiki__'.$web;
}
sub databaseExists {
    my $self = shift;
    my $web = shift;

    my $name = $self->getDatabaseName($web);
    
    my $connection = $self->_connect();
    my @dbs = $connection->database_names;
    foreach my $db_name (@dbs) {
        return 1 if ($name eq $db_name);
    }
    return;
}

sub _getDatabase {
    my $self           = shift;
    my $web       = shift;
    
    my $name = $self->getDatabaseName($web);
   
    my $connection = $self->_connect();
    return $connection->get_database( $name );
}
sub _getCollection {
    my $self           = shift;
    my $web       = shift;
    my $collectionName = shift;
    
    my $db   = $self->_getDatabase($web);

    return $db->get_collection($collectionName);
}

sub _connect {
    my $self = shift;

    if ( not defined( $self->{connection} ) ) {
        
        $self->{connection} = MongoDB::Connection->new(
            $self->{cfg},
        );
        ASSERT( $self->{connection} ) if DEBUG;
    }
    return $self->{connection};
}

#I'm using this to test where i'm up to
sub _MONGODB {
    my $self   = shift;
    my $params = shift;

    my $web   = $params->{web};
    my $topic = $params->{topic};

    my $database = $self->_getDatabase($web);
    
    #this will return the list of commands and how to use them
    #my $result = $database->run_command( ['listCommands'=>1] );
    
    my $result = $database->run_command( ['collStats'=>'current'] );


    return "\n<verbatim>\n".Dumper($result)."\n</verbatim>\n";

    #return join(', ', map { "$_: ".($data->{$_}||'UNDEF')."\n" } keys(%$data));
}

1;
__END__
This copyright information applies to the MongoDBPlugin:

# Plugin for Foswiki - The Free and Open Source Wiki, http://foswiki.org/
#
# Copyright 2010 - SvenDowideit@fosiki.com
#
# MongoDBPlugin is # This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# For licensing info read LICENSE file in the root of this distribution.
