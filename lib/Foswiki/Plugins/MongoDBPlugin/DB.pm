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

sub new {
    my $class = shift;
    my $params = shift;

    my $self = bless({%$params, session => $Foswiki::Func::SESSION}, $class);

    $Foswiki::Func::SESSION->{MongoDB} = $self;
    return $self;
}

sub update {
    my $self = shift;
    my $collectionName = shift;
    my $address = shift;
    my $hash = shift;
    
    my $collection = $self->_getCollection($collectionName);
    $collection->update({address=>$address},
                        {address=>$address,%$hash},
                        {'upsert'=>1});
}


#######################################################
#Webname?
sub _getCollection {
    my $self = shift;
    my $collectionName = shift;

    my $connection = $self->_connect();
    my $database = $connection->get_database($self->{database});
    return $database->get_collection($collectionName);
}

sub _connect {
    my $self = shift;
    
    if (not defined($self->{connection})) {
        $self->{connection} = MongoDB::Connection->new(host => $self->{host}, port=>$self->{port});
    }
    return $self->{connection};
}

#I'm using this to test where i'm up to
sub _MONGODB {
    my $self = shift;
    my $params = shift;
    
    my $web = $params->{web};
    my $topic = $params->{topic};
    
    my $collection = $self->_getCollection('current');
    my $data       = $collection->find_one({_web=>$web,_topic=>$topic});
    
    use Foswiki::Plugins::MongoDBPlugin::Meta;
    my $meta = new Foswiki::Plugins::MongoDBPlugin::Meta($self, $data->{_web}, $data->{_topic}, $data);

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
