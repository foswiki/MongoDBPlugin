# See bottom of file for license and copyright information
package Foswiki::Plugins::MongoDBPlugin::Listener;

use Foswiki::Plugins::MongoDBPlugin       ();
use Foswiki::Plugins::MongoDBPlugin::Meta ();

use Assert;

=begin TML

---+ package Foswiki::Plugins::MongoDBPlugin::Listener;
push(@{$Foswiki::cfg{Store}{Listeners}}, 'Foswiki::Plugins::MongoDBPlugin::Listener');


=cut

sub new {
    my $class = shift;

    my $self = bless {@_}, $class;

    #disable the plugin handler's attempts to keep the mongoDB in sync
    $Foswiki::cfg{Plugins}{MongoDBPlugin}{EnableOnSaveUpdates} = 0;
    $Foswiki::Plugins::MongoDBPlugin::enableOnSaveUpdates = 0;

#print STDERR "***************************************MongoDB Listening****************************\n";

    return $self;
}

=begin TML

---++ ObjectMethod insert($metaObject)
Event triggered when a new Meta object is inserted into the store

=cut

sub insert {
    my $self = shift;
    my %args = @_;

    return if ( defined( $args{newattachment} ) );

    Foswiki::Plugins::MongoDBPlugin::_updateTopic( $args{newmeta}->web,
        $args{newmeta}->topic, $args{newmeta} );
}

=begin TML

---++ ObjectMethod update($oldMetaObject[, $newMetaObject])

We are updating the object. This is triggered when a meta-object
is saved. It should be logically equivalent to:
<verbatim>
remove($oldMetaObject)
insert($newMetaObject || $oldMetaObject)
</verbatim>
but listeners may optimise on this. The two parameter form is called when
a topic is moved.

=cut

sub update {
    my $self = shift;
    my %args = @_;

    #TODO: I'm not doing attachments yet
    return if ( defined( $args{newattachment} ) );
    return if ( defined( $args{oldattachment} ) );

    #TODO: not doing web create/move etc yet
    return if ( not defined( $args{newmeta}->topic ) );

    #TODO: do this differently when we support previous revs
    if ( defined( $args{oldmeta} ) ) {

        #move topic is (currently) a delete&insert
        $self->remove( oldmeta => $args{oldmeta} );
    }
    Foswiki::Plugins::MongoDBPlugin::_updateTopic( $args{newmeta}->web,
        $args{newmeta}->topic, $args{newmeta} );
}

=begin TML

---++ ObjectMethod remove(oldmeta=>obj [,  oldattachment=>$string])
We are removing the given object.

=cut

sub remove {
    my $self = shift;
    my %args = @_;

    ASSERT( $args{oldmeta} ) if DEBUG;

#print STDERR "removing ".join(',',keys(%args))."\n";
#print STDERR "     (".$args{oldmeta}->web.", ".($args{oldmeta}->topic||'UNDEF').")\n";

    #TODO: find the old topic object, and remove it.
    Foswiki::Plugins::MongoDBPlugin::_removeTopic( $args{oldmeta}->web,
        $args{oldmeta}->topic );
}

=begin TML

---++ ObjectMethod loadTopic($meta, $version) -> ($gotRev, $isLatest)

NOTE: atm, this will only get called if the Store says yes, this meta item exists on disk
    so we can't inject new topics, only corrupt existing ones

=cut

sub loadTopic {
    my $self    = shift;
    my $meta    = shift;
    my $version = shift;
    
    #allow the MongoDBPlugin to disable the listener when running a web update resthandler
    return if (not $Foswiki::cfg{Store}{Listeners}{'Foswiki::Plugins::MongoDBPlugin::Listener'});

    if ( defined( $Foswiki::cfg{Plugins}{MongoDBPlugin}{ExperimentalCode} )
        and $Foswiki::cfg{Plugins}{MongoDBPlugin}{ExperimentalCode} )
    {

        return
          if ( defined($version) );   #not doing topic versioning in mongodb yet
        return $session->{ $meta->web . '.' . $meta->topic }
          if defined( $self->{ $meta->web . '.' . $meta->topic } );

        #this code can do nothing - ignore it
        if (    ( $meta->web eq 'Sandbox' )
            and ( $meta->topic eq 'SvenDoesNotExist' ) )
        {
            $meta->text('This is not really a topic');
            $meta->{_loadedRev}      = 1;
            $meta->{_latestIsLoaded} = 1;
            return ( 1, 1 );
        }

  #print STDERR "===== loadTopic(".$meta->web." , ".$meta->topic.", version)\n";

        #rebless into a mighter version of Meta
        bless( $meta, 'Foswiki::Plugins::MongoDBPlugin::Meta' );
        $meta->reload();    #get the latest version
        if ( $meta->topic =~ /Form$/ ) {
            use Foswiki::Form;
            bless( $meta, 'Foswiki::Form' );

            $session->{forms}->{ $meta->web . '.' . $meta->topic } = $meta;
            print STDERR "------ init Form obj\n";
        }

        $self->{ $meta->web . '.' . $meta->topic } = $meta;

        return ( 1, 1 );
    }
}

1;
__DATA__

Author: Sven Dowideit http://fosiki.com

Module of Foswiki - The Free and Open Source Wiki, http://foswiki.org/, http://Foswiki.org/

Copyright (C) 2010 Foswiki Contributors. All Rights Reserved.
Foswiki Contributors are listed in the AUTHORS file in the root
of this distribution. NOTE: Please extend that file, not this notice.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 3
of the License, or (at your option) any later version. For
more details read LICENSE in the root of this distribution.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

As per the GPL, removal of this notice is prohibited.
