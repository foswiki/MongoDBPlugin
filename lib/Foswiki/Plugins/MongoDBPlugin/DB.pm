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
use Assert;

use constant MONITOR => $Foswiki::cfg{MONITOR}{'Foswiki::Plugins::MongoDBPlugin'} || 0;

sub new {
    my $class  = shift;
    my $params = shift;

    my $self =
      bless( { %$params, session => $Foswiki::Func::SESSION }, $class );

    $Foswiki::Func::SESSION->{MongoDB} = $self;
    return $self;
}

sub query {
    my $self = shift;
    my $collectionName = shift;
    my $ixhQuery        = shift;
    my $queryAttrs = shift || {};
    
    my $collection = $self->_getCollection('current');
    print STDERR "searching mongo : ".Dumper($ixhQuery)." , ".Dumper($queryAttrs)."\n" if MONITOR;
    my $cursor = $collection->query($ixhQuery, $queryAttrs);
    print STDERR "found " . $cursor->count . " _BUT_ has_next is ".($cursor->has_next()?'true':'false')."\n" if MONITOR;
    
    return $cursor;
}


sub update {
    my $self           = shift;
    my $collectionName = shift;
    my $address        = shift;
    my $hash           = shift;

#    use Data::Dumper;
#print STDERR "+++++ mongo update $address == ".Dumper($hash)."\n";

    my $collection = $self->_getCollection($collectionName);

#TODO: not the most efficient place to create and index, but I want to be sure, to be sure.
    $self->ensureIndex( $collection, { _topic => 1 }, {name=>'_topic'});
    $self->ensureIndex( $collection, { _topic => 1, _web => 1 }, { name=>'_topic:_web', unique => 1 } );
    $self->ensureIndex( $collection, { 'TOPICINFO.author' => 1 }, {name=>'TOPICINFO.author'} );
    $self->ensureIndex( $collection, { 'TOPICINFO.date' => 1  }, {name=>'TOPICINFO.date'} );
    $self->ensureIndex( $collection, { 'TOPICPARENT.name' => 1  }, {name=>'TOPICPARENT.name'} );

#TODO: maybe should use the auto indexed '_id' (or maybe we can use this as a tuid - unique foreach rev of each topic..)
#then again, atm, its totally random, so may be good for sharding.

    $collection->update(
        { address  => $address },
        { address  => $address, %$hash },
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
    my $self = shift;
    my $collection = shift; #either a collection object of a name
    my $indexRef = shift;   #can be a hashref or an ixHash
    my $options = shift;
    
    ASSERT(defined($options->{name})) if DEBUG;
    
    if (ref($collection) eq '') {
        #convert name of collection to collection obj
        $collection = $self->_getCollection($collection);
    }
    
    #cache the indexes we know about
    if (not defined($self->{mongoDBIndexes})) {
        my @indexes = $collection->get_indexes();
        $self->{mongoDBIndexes} = \@indexes;
    }
    foreach my $index (@{$self->{mongoDBIndexes}})  {
        #print STDERR "we already have:  ".$index->{name}." index\n";
        if ($options->{name} eq $index->{name}) {
            #already exists, do nothing.
            return;
        }
    }
    if (scalar(@{$self->{mongoDBIndexes}}) >= 40) {
        print STDERR "*******************ouch. MongoDB can only have 40 indexes per collection\n" if MONITOR;
        return;
    }
print STDERR "creating ".$options->{name}." index\n" if MONITOR;
    #TODO: consider doing these in a batch at the end of a request, or?
    $collection->ensure_index($indexRef, $options);
    undef $self->{mongoDBIndexes}; #clear the cache :/
}


sub remove {
    my $self           = shift;
    my $collectionName = shift;
    my $hash           = shift;

#    use Data::Dumper;
#print STDERR "+++++ mongo remove $address == ".Dumper($hash)."\n";

    my $collection = $self->_getCollection($collectionName);

    $collection->remove( $hash );
}

#######################################################
#Webname?
sub _getCollection {
    my $self           = shift;
    my $collectionName = shift;

    my $connection = $self->_connect();
    my $database   = $connection->get_database( $self->{database} );
  
    return $database->get_collection($collectionName);
}

sub _connect {
    my $self = shift;

    if ( not defined( $self->{connection} ) ) {
        $self->{connection} = MongoDB::Connection->new(
            host => $self->{host},
            port => $self->{port}
        );
	ASSERT($self->{connection}) if DEBUG;
    }
    return $self->{connection};
}

#I'm using this to test where i'm up to
sub _MONGODB {
    my $self   = shift;
    my $params = shift;

    my $web   = $params->{web};
    my $topic = $params->{topic};

    my $collection = $self->_getCollection('current');
    my $data = $collection->find_one( { _web => $web, _topic => $topic } );

    use Foswiki::Plugins::MongoDBPlugin::Meta;
    my $meta =
      new Foswiki::Plugins::MongoDBPlugin::Meta( $self, $data->{_web},
        $data->{_topic}, $data );

    return $meta->stringify();

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
