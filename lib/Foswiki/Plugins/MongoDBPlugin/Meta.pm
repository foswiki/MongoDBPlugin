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

---+ package Foswiki::Plugins::MongoDBPlugin::Meta


=cut

package Foswiki::Plugins::MongoDBPlugin::Meta;
use Foswiki::Plugins::MongoDBPlugin::DB;

use Foswiki::Meta;

our @ISA = ('Foswiki::Meta');

# Always use strict to enforce variable scoping
use strict;

sub new {
    my $class   = shift;
    my $session = shift;
    my $web     = shift;
    my $topic   = shift;
    my $data    = shift;

    #my $meta = new Foswiki::Meta($session, $web, $topic );
    my $meta = $class->SUPER::new( $session, $web, $topic );

#TODO: if $data is undef - see if its in mongoDB already, and if so, load it... ((OR... this should happen in the load/reload mess))

    my @validKeys = keys(%Foswiki::Meta::VALIDATE);
    push( @validKeys, '_text' );
    @$meta{@validKeys} = @$data{@validKeys};
#print STDERR ": make me a new MongoDB::Meta with $web.$topic \n";

    return $meta;
}

=begin TML

---++ ObjectMethod reload($rev)

Reload the object from the store; perhaps because we haven't loaded it yet,
or we are looking at a different rev. See =getLoadedRev= to determine what
revision is currently being viewed.

#SMELL: its quite worrying that to over-ride this method, I have to reproduce most of it.

TODO: obviously all I really need to do is push this into the Store imp's readTopic, and make a matching saveTopic and I should be golden enough

=cut

sub reload {
    my ( $this, $rev ) = @_;

    return unless $this->{_topic};
    if ( defined $rev ) {
        $rev = Foswiki::Store::cleanUpRevID($rev);
    }
    else {
        $rev = $this->{_loadedRev};    # if any
    }
    foreach my $field ( keys %$this ) {
        next if $field =~ /^_(web|topic|session)/;
        $this->{$field} = undef;
    }
    $this->{FILEATTACHMENT} = [];

    my $collection =
      $Foswiki::Func::SESSION->{MongoDB}->_getCollection('current');
    my $data = $collection->find_one(
        { _web => $this->{_web}, _topic => $this->{_topic} } );
    my @validKeys = keys(%Foswiki::Meta::VALIDATE);
    push( @validKeys, '_text' );
    @$this{@validKeys} = @$data{@validKeys};

    $this->{_loadedRev} =
      Foswiki::Store::cleanUpRevID( $this->{TOPICINFO}[0]->{version} );

    # SMELL: removed see getLoadedRev - should remove any
    # non-numeric rev's (like the $rev stuff from svn)
    $this->{_preferences}->finish() if defined $this->{_preferences};
    $this->{_preferences} = undef;

    $this->addDependency();
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
