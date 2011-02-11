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
use Foswiki::Plugins::MongoDBPlugin;
#use Foswiki::Plugins::MongoDBPlugin::DB;

use Foswiki::Meta;
#use Foswiki::Form;
#our @ISA = ('Foswiki::Form');

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

    #print STDERR ": make me a new MongoDB::Meta with $web.$topic \n";

#TODO: if $data is undef - see if its in mongoDB already, and if so, load it... ((OR... this should happen in the load/reload mess))

#doooooood, shouln't this be in the reload?
#TODO: as of Oct 2010, mongodb can't sort on an element in an array, so we re-array the meta (see Foswiki::Plugins::MongoDBPlugin::_updateTopic).
    $meta->{_text} = $data->{_text};
    $meta->{_indices} = $data->{_indices};
    foreach my $key ( keys(%Foswiki::Meta::VALIDATE) ) {
        if ( $Foswiki::Meta::isArrayType{$key} )
        {
            if ( defined( $data->{$key} ) ) {
                my %FIELD = $data->{$key};
                $meta->{$key} = [];
                foreach my $elem ( keys(%FIELD) ) {
                    push( @{ $meta->{$key} }, $FIELD{$elem} );
                }
            }
        }
        else {
            if (ref(\$data->{$key}) eq 'SCALAR') {
                $meta->{$key} = $data->{$key};
            } else {
                my $temp = $data->{$key};
                $meta->{$key} = [];
                if ( defined($temp) ) {
                    push( @{ $meta->{$key} }, %$temp );
                    undef $meta->{$key}{_authorWikiName} if ( $key eq 'TOPICINFO' );
                }
            }
        }
        
    }

    return $meta;
}

=begin TML

---++ ObjectMethod reload($rev)

Reload the object from the store; perhaps because we haven't loaded it yet,
or we are looking at a different rev. See =getLoadedRev= to determine what
revision is currently being viewed.

#SMELL: its quite worrying that to over-ride this method, I have to reproduce most of it.

TODO: obviously all I really need to do is push this into the Store imp's readTopic, 
and make a matching saveTopic and I should be golden enough

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
      Foswiki::Plugins::MongoDBPlugin::getMongoDB->_getCollection('current');
    my $data = $collection->find_one(
        { _web => $this->{_web}, _topic => $this->{_topic} } );
    my @validKeys = keys(%Foswiki::Meta::VALIDATE);
    #push( @validKeys, '_text' );
    #need to do more than this now
    #@$this{@validKeys} = @$data{@validKeys};
    foreach my $key (@validKeys) {
        if ( $Foswiki::Meta::isArrayType{$key} ) {
            #print STDERR "---- $key == many\n";
            $this->{$key} = $data->{$key}->{'__RAW_ARRAY'};
        } else {
            #$meta->{$key} = $savedMeta->{$key}[0];
            $this->{$key} = [];
            push(@{$this->{$key}}, $data->{$key});
        }
    }
#use Data::Dumper;
#print STDERR "--- FIELD: ".Dumper($this->{FIELD}[0])."\n";
    $this->{_text} = $data->{_text};
    $this->{_indices} = $data->{_indices};


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
